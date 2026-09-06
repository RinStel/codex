//! Resume durable turn history without manufacturing a user message.

use super::*;
use crate::request_processors::paginated_turn_items::paginated_turn_full_items;
use codex_app_server_protocol::ThreadHistoryBuilder;
use codex_app_server_protocol::TurnContinueParams;
use codex_app_server_protocol::TurnContinueResponse;
use codex_app_server_protocol::build_turns_from_rollout_items;
use codex_core::RecoverTurnRequest;
use codex_protocol::turn_input::StartIfIdleSubmission;
use codex_thread_store::LoadThreadHistoryParams;

impl TurnRequestProcessor {
    pub(crate) async fn turn_continue(
        &self,
        request_id: &ConnectionRequestId,
        params: TurnContinueParams,
    ) -> Result<Option<ClientResponsePayload>, JSONRPCErrorError> {
        let (thread_id, thread) = self.load_thread(&params.thread_id).await?;
        self.ensure_direct_input_allowed(request_id, thread.as_ref())
            .await?;
        // Check before reading history: an active turn could finish while that
        // read is in flight and otherwise be restarted from a stale snapshot.
        if matches!(thread.agent_status().await, AgentStatus::Running) {
            return Err(invalid_request("Cannot continue a running turn."));
        }
        // A new thread has no materialized rollout yet. Reject it before asking
        // the store to open a history file that does not exist.
        if thread
            .conversation_history_snapshot()
            .await
            .items()
            .next()
            .is_none()
        {
            return Err(invalid_request("There is no unfinished turn to continue."));
        }
        thread
            .flush_rollout()
            .await
            .map_err(|err| internal_error(format!("failed to flush turn history: {err}")))?;
        let history = match self
            .thread_store
            .load_latest_model_context(LoadThreadHistoryParams {
                thread_id,
                include_archived: false,
            })
            .await
        {
            Ok(context) => context.items,
            Err(ThreadStoreError::Unsupported {
                operation: "load_latest_model_context",
            }) => {
                thread
                    .load_history(/*include_archived*/ false)
                    .await
                    .map_err(|err| internal_error(format!("failed to load turn history: {err}")))?
                    .items
            }
            Err(err) => {
                return Err(internal_error(format!(
                    "failed to load turn context: {err}"
                )));
            }
        };
        let mut turn = build_turns_from_rollout_items(&history)
            .pop()
            .ok_or_else(|| invalid_request("There is no unfinished turn to continue."))?;
        match turn.status {
            TurnStatus::Completed => {
                return Err(invalid_request("The last turn has already completed."));
            }
            TurnStatus::Interrupted | TurnStatus::Failed | TurnStatus::InProgress => {}
        }
        if turn.error.as_ref().is_some_and(|error| {
            error.codex_error_info == Some(CodexErrorInfo::MisalignmentPolicyViolation)
        }) {
            return Err(invalid_request(
                "This turn was stopped as a precaution and cannot be continued.",
            ));
        }
        let cyber_access_program = history
            .iter()
            .rev()
            .find_map(|item| match item {
                RolloutItem::TurnContext(context)
                    if context.turn_id.as_deref() == Some(turn.id.as_str()) =>
                {
                    Some(context.cyber_access_program)
                }
                _ => None,
            })
            .flatten();
        let thread_state = self.thread_state_manager.thread_state(thread_id).await;
        if history.iter().any(|item| {
            matches!(item, RolloutItem::SessionMeta(meta)
                if meta.meta.history_mode == codex_protocol::protocol::ThreadHistoryMode::Paginated)
        }) {
            // A model checkpoint can omit earlier attempts of this same turn.
            turn.items =
                paginated_turn_full_items(self.thread_store.as_ref(), thread_id, &turn.id).await?;
        }
        let turn_id = turn.id.clone();
        // Install at TurnStarted so a queued terminal event from the previous
        // attempt cannot discard the recovered items before the new attempt.
        thread_state.lock().await.pending_recovery_history =
            Some(ThreadHistoryBuilder::from_turn(turn));
        let submission = thread
            .recover_turn_if_idle(RecoverTurnRequest {
                turn_id,
                thread_settings: Default::default(),
                trace: self.request_trace_context(request_id).await,
                cyber_access_program,
            })
            .await;
        if !matches!(&submission, Ok(StartIfIdleSubmission::Started { .. })) {
            thread_state.lock().await.pending_recovery_history = None;
        }
        let submission =
            submission.map_err(|err| internal_error(format!("failed to continue turn: {err}")))?;
        match submission {
            StartIfIdleSubmission::Started { turn_id } => {
                self.outgoing
                    .record_request_turn_id(request_id, &turn_id)
                    .await;
                Ok(Some(TurnContinueResponse { turn_id }.into()))
            }
            StartIfIdleSubmission::NotSubmitted { reason } => {
                Err(invalid_request(format!("Cannot continue turn: {reason:?}")))
            }
        }
    }
}
