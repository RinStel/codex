use super::*;
use pretty_assertions::assert_eq;

#[test]
fn recovery_reuses_turn_items_and_clears_terminal_state() {
    let started = TurnStartedEvent {
        turn_id: "original-turn".to_string(),
        trace_id: None,
        started_at: Some(10),
        model_context_window: None,
        collaboration_mode_kind: Default::default(),
    };
    let progress = AgentMessageEvent {
        message: "Saved progress".to_string(),
        phase: None,
        memory_citation: None,
        delivery: None,
        questions: None,
    };
    let history = vec![
        RolloutItem::EventMsg(EventMsg::AgentMessage(AgentMessageEvent {
            message: "An earlier turn".to_string(),
            ..progress.clone()
        })),
        RolloutItem::EventMsg(EventMsg::TurnStarted(started.clone())),
        RolloutItem::EventMsg(EventMsg::AgentMessage(progress.clone())),
        RolloutItem::EventMsg(EventMsg::TurnAborted(TurnAbortedEvent {
            turn_id: Some(started.turn_id.clone()),
            started_at: Some(10),
            reason: codex_protocol::protocol::TurnAbortReason::Interrupted,
            completed_at: Some(12),
            duration_ms: Some(2000),
        })),
    ];
    let mut expected = build_turns_from_rollout_items(&history)
        .pop()
        .expect("interrupted turn");
    let mut builder = ThreadHistoryBuilder::from_turn(expected.clone());
    expected.status = TurnStatus::InProgress;
    expected.error = None;
    expected.started_at = Some(20);
    expected.completed_at = None;
    expected.duration_ms = None;

    builder.handle_event(&EventMsg::TurnStarted(TurnStartedEvent {
        started_at: Some(20),
        ..started
    }));

    assert_eq!(builder.active_turn_snapshot(), Some(expected.clone()));
    builder.handle_event(&EventMsg::AgentMessage(AgentMessageEvent {
        message: "Resumed progress".to_string(),
        ..progress
    }));
    expected.items.push(ThreadItem::AgentMessage {
        id: "item-3".to_string(),
        text: "Resumed progress".to_string(),
        phase: None,
        memory_citation: None,
        delivery: None,
        questions: None,
    });
    assert_eq!(builder.finish(), vec![expected]);
}
