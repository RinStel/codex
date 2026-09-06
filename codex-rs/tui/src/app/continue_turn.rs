//! The slash command uses control-plane recovery, with no composer submission.

use super::*;
use codex_app_server_protocol::TurnContinueParams;
use codex_app_server_protocol::TurnContinueResponse;

impl App {
    pub(super) async fn continue_turn(
        &mut self,
        app_server: &mut AppServerSession,
        thread_id: ThreadId,
    ) {
        let request_id = app_server.next_request_id();
        if let Err(err) = app_server
            .request_handle()
            .request_typed::<TurnContinueResponse>(ClientRequest::TurnContinue {
                request_id,
                params: TurnContinueParams {
                    thread_id: thread_id.to_string(),
                },
            })
            .await
        {
            self.chat_widget
                .add_error_message(format!("Failed to continue turn: {err}"));
        }
    }
}
