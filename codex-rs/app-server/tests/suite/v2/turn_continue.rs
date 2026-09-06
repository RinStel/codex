use anyhow::Result;
use app_test_support::MockResponsesConfig;
use app_test_support::TestAppServer;
use app_test_support::create_final_assistant_message_sse_response;
use codex_app_server_protocol::ClientRequest;
use codex_app_server_protocol::ItemCompletedNotification;
use codex_app_server_protocol::RequestId;
use codex_app_server_protocol::ThreadHistoryMode;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadReadParams;
use codex_app_server_protocol::ThreadReadResponse;
use codex_app_server_protocol::ThreadResumeParams;
use codex_app_server_protocol::ThreadResumeResponse;
use codex_app_server_protocol::ThreadStartParams;
use codex_app_server_protocol::TurnCompletedNotification;
use codex_app_server_protocol::TurnContinueParams;
use codex_app_server_protocol::TurnContinueResponse;
use codex_app_server_protocol::TurnInterruptParams;
use codex_app_server_protocol::TurnInterruptResponse;
use codex_app_server_protocol::TurnStartParams;
use codex_app_server_protocol::TurnStartResponse;
use codex_app_server_protocol::TurnStatus;
use codex_app_server_protocol::UserInput;
use codex_protocol::protocol::EventMsg;
use codex_rollout::CompactedItem;
use codex_rollout::RolloutItem;
use codex_rollout::RolloutRecorder;
use codex_rollout::append_rollout_item_to_path;
use core_test_support::responses;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use test_case::test_case;
use tokio::sync::oneshot;

#[derive(Clone, Copy)]
enum RecoveryLocation {
    Live,
    Reopened,
    CompactedReopened,
}

#[derive(Clone, Copy)]
enum Interruption {
    User,
    Disconnected,
}

async fn assert_continue_rejected(mcp: &mut TestAppServer, thread_id: &str) -> Result<String> {
    let id = mcp
        .send_request("turn/continue", Some(json!({ "threadId": thread_id })))
        .await?;
    let error = mcp
        .read_stream_until_error_message(RequestId::Integer(id))
        .await?;
    assert_eq!(error.error.code, -32600);
    Ok(error.error.message)
}

#[test_case(RecoveryLocation::Live, Interruption::User, ThreadHistoryMode::Paginated; "live")]
#[test_case(RecoveryLocation::Reopened, Interruption::User, ThreadHistoryMode::Paginated; "reopened")]
#[test_case(RecoveryLocation::CompactedReopened, Interruption::User, ThreadHistoryMode::Paginated; "compacted_reopened")]
#[test_case(RecoveryLocation::Live, Interruption::Disconnected, ThreadHistoryMode::Paginated; "disconnected")]
#[test_case(RecoveryLocation::Reopened, Interruption::Disconnected, ThreadHistoryMode::Paginated; "disconnected_reopened")]
#[test_case(RecoveryLocation::Live, Interruption::User, ThreadHistoryMode::Legacy; "legacy_live")]
#[test_case(RecoveryLocation::Reopened, Interruption::User, ThreadHistoryMode::Legacy; "legacy_reopened")]
#[test_case(RecoveryLocation::Live, Interruption::Disconnected, ThreadHistoryMode::Legacy; "legacy_disconnected")]
#[test_case(RecoveryLocation::Reopened, Interruption::Disconnected, ThreadHistoryMode::Legacy; "legacy_disconnected_reopened")]
#[tokio::test]
async fn continue_interrupted_turn_without_user_input(
    location: RecoveryLocation,
    interruption: Interruption,
    history_mode: ThreadHistoryMode,
) -> Result<()> {
    let (first_gate, first_wait) = oneshot::channel();
    let (second_gate, second_wait) = oneshot::channel();
    let (server, _) = start_streaming_sse_server(vec![
        vec![
            StreamingSseChunk {
                gate: None,
                body: responses::sse(vec![
                    responses::ev_response_created("response-before-interruption"),
                    responses::ev_assistant_message(
                        "saved-progress",
                        "Already inspected the source.",
                    ),
                ]),
            },
            StreamingSseChunk {
                gate: Some(first_wait),
                body: String::new(),
            },
        ],
        vec![StreamingSseChunk {
            gate: Some(second_wait),
            body: create_final_assistant_message_sse_response("finished")?,
        }],
    ])
    .await;
    let home = TempDir::new()?;
    MockResponsesConfig::new(server.uri()).write(home.path())?;
    let mut mcp = TestAppServer::builder()
        .with_codex_home(home.path())
        .build_initialized()
        .await?;
    let thread = mcp
        .start_thread(ThreadStartParams {
            history_mode: Some(history_mode),
            ..Default::default()
        })
        .await?
        .thread;
    assert_continue_rejected(&mut mcp, &thread.id).await?;
    let started: TurnStartResponse = mcp
        .request(|request_id| ClientRequest::TurnStart {
            request_id,
            params: TurnStartParams {
                thread_id: thread.id.clone(),
                input: vec![UserInput::Text {
                    text: "finish the original task".to_string(),
                    text_elements: Vec::new(),
                }],
                ..Default::default()
            },
        })
        .await?;
    server.wait_for_request_count(/*count*/ 1).await;
    loop {
        let completed: ItemCompletedNotification = mcp.read_notification("item/completed").await?;
        if matches!(completed.item, ThreadItem::AgentMessage { .. }) {
            break;
        }
    }
    assert_eq!(
        assert_continue_rejected(&mut mcp, &thread.id).await?,
        "Cannot continue a running turn."
    );
    let expected_status = match interruption {
        Interruption::User => {
            let _: TurnInterruptResponse = mcp
                .request(|request_id| ClientRequest::TurnInterrupt {
                    request_id,
                    params: TurnInterruptParams {
                        thread_id: thread.id.clone(),
                        turn_id: started.turn.id.clone(),
                    },
                })
                .await?;
            drop(first_gate);
            TurnStatus::Interrupted
        }
        Interruption::Disconnected => {
            first_gate
                .send(())
                .expect("close unfinished response stream");
            TurnStatus::Failed
        }
    };
    let completed: TurnCompletedNotification = mcp.read_notification("turn/completed").await?;
    assert_eq!(completed.turn.status, expected_status);
    let original: ThreadReadResponse = mcp
        .request(|request_id| ClientRequest::ThreadRead {
            request_id,
            params: ThreadReadParams {
                thread_id: thread.id.clone(),
                include_turns: true,
            },
        })
        .await?;
    if !matches!(location, RecoveryLocation::Live) {
        mcp.shutdown_gracefully().await?;
        if matches!(location, RecoveryLocation::CompactedReopened) {
            let path = original.thread.path.as_ref().expect("durable rollout");
            let (items, _, _) = RolloutRecorder::load_rollout_items(path).await?;
            let start = items
                .iter()
                .find(|item| matches!(item, RolloutItem::EventMsg(EventMsg::TurnStarted(_))))
                .expect("saved turn start")
                .clone();
            let context = items
                .iter()
                .find(|item| matches!(item, RolloutItem::TurnContext(_)))
                .expect("saved turn context")
                .clone();
            let world = items
                .iter()
                .find(|item| matches!(item, RolloutItem::WorldState(state) if state.full))
                .expect("full world state")
                .clone();
            let abort = items
                .iter()
                .find(|item| matches!(item, RolloutItem::EventMsg(EventMsg::TurnAborted(_))))
                .expect("saved interruption")
                .clone();
            let checkpoint = RolloutItem::Compacted(CompactedItem {
                message: "Saved recovery checkpoint".to_string(),
                replacement_history: Some(
                    items
                        .iter()
                        .filter_map(|item| match item {
                            RolloutItem::ResponseItem(item) => Some(item.clone()),
                            _ => None,
                        })
                        .collect(),
                ),
                retained_context: None,
                guardian_history: None,
                mcp_resource_origins: None,
                window_number: Some(1),
                first_window_id: None,
                previous_window_id: None,
                window_id: None,
                compaction_response_id: None,
                latest_token_usage_record: None,
            });
            // A later attempt compacted and stopped. Its model-context suffix
            // starts here, while visible items still belong to the first attempt.
            for item in [start, checkpoint, world, context, abort] {
                append_rollout_item_to_path(path, &item).await?;
            }
        }
        mcp = TestAppServer::builder()
            .with_codex_home(home.path())
            .build_initialized()
            .await?;
        let _: ThreadResumeResponse = mcp
            .request(|request_id| ClientRequest::ThreadResume {
                request_id,
                params: ThreadResumeParams {
                    thread_id: thread.id.clone(),
                    ..Default::default()
                },
            })
            .await?;
    }
    let recovered: TurnContinueResponse = mcp
        .request(|request_id| ClientRequest::TurnContinue {
            request_id,
            params: TurnContinueParams {
                thread_id: thread.id.clone(),
            },
        })
        .await?;
    assert_eq!(
        recovered,
        TurnContinueResponse {
            turn_id: started.turn.id
        }
    );
    server.wait_for_request_count(/*count*/ 2).await;
    assert_continue_rejected(&mut mcp, &thread.id).await?;
    let resumed: ThreadResumeResponse = mcp
        .request(|request_id| ClientRequest::ThreadResume {
            request_id,
            params: ThreadResumeParams {
                thread_id: thread.id.clone(),
                ..Default::default()
            },
        })
        .await?;
    assert_eq!(resumed.thread.turns.len(), 1);
    let active = &resumed.thread.turns[0];
    let mut expected = original.thread.turns[0].clone();
    expected.status = TurnStatus::InProgress;
    expected.error = None;
    expected.started_at = active.started_at;
    expected.completed_at = None;
    expected.duration_ms = None;
    assert_eq!(active, &expected);
    second_gate.send(()).expect("release recovery response");
    let completed: TurnCompletedNotification = mcp.read_notification("turn/completed").await?;
    assert_eq!(completed.turn.status, TurnStatus::Completed);
    assert_continue_rejected(&mut mcp, &thread.id).await?;

    let history: ThreadReadResponse = mcp
        .request(|request_id| ClientRequest::ThreadRead {
            request_id,
            params: ThreadReadParams {
                thread_id: thread.id.clone(),
                include_turns: true,
            },
        })
        .await?;
    assert_eq!(
        history
            .thread
            .turns
            .iter()
            .map(|turn| turn.id.as_str())
            .collect::<Vec<_>>(),
        vec![recovered.turn_id.as_str()]
    );
    assert_eq!(history.thread.turns[0].status, TurnStatus::Completed);

    let requests = server.requests().await;
    assert_eq!(requests.len(), 2);
    let recovered_request: Value = serde_json::from_slice(&requests[1])?;
    let assistant_messages = recovered_request["input"]
        .as_array()
        .expect("input items")
        .iter()
        .filter(|item| item["role"] == "assistant")
        .map(|item| item["content"].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        assistant_messages,
        vec![json!([
            { "type": "output_text", "text": "Already inspected the source." }
        ])]
    );
    // Interruption and changed resume settings can add context fragments, but
    // recovery must not manufacture a user-authored message.
    let users = requests
        .iter()
        .map(|body| {
            let request: Value = serde_json::from_slice(body).expect("model request JSON");
            request["input"]
                .as_array()
                .expect("input items")
                .iter()
                .filter(|item| item["role"] == "user")
                .filter(|item| {
                    !item["content"].as_array().is_some_and(|content| {
                        content.iter().any(|part| {
                            part["text"].as_str().is_some_and(|text| {
                                text.starts_with("<turn_aborted>")
                                    || text.starts_with("<environment_context>")
                            })
                        })
                    })
                })
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(users[1], users[0]);
    server.shutdown().await;
    Ok(())
}
