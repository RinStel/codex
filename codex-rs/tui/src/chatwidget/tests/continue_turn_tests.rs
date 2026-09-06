use super::*;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn continue_dispatches_control_event_without_user_input() {
    let (mut chat, mut rx, mut op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    let thread_id = ThreadId::new();
    chat.thread_id = Some(thread_id);
    handle_turn_started(&mut chat, "interrupted-turn");
    handle_turn_interrupted(&mut chat, "interrupted-turn");
    while rx.try_recv().is_ok() {}
    chat.dispatch_command(SlashCommand::Continue);
    assert_matches!(rx.try_recv(), Ok(AppEvent::ContinueTurn { thread_id: id }) if id == thread_id);
    assert!(op_rx.try_recv().is_err());
    assert!(drain_insert_history(&mut rx).is_empty());
}

#[tokio::test]
async fn continue_without_session_displays_local_error() {
    let (mut chat, mut rx, mut op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.thread_id = None;
    chat.dispatch_command(SlashCommand::Continue);
    let lines = drain_insert_history(&mut rx)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    insta::assert_snapshot!(lines_to_single_string(&lines), @"■ There is no unfinished turn to continue.");
    assert!(op_rx.try_recv().is_err());
}

#[tokio::test]
async fn continue_while_running_displays_local_error() {
    let (mut chat, mut rx, mut op_rx) = make_chatwidget_manual(/*model_override*/ None).await;
    chat.thread_id = Some(ThreadId::new());
    handle_turn_started(&mut chat, "running-turn");
    drain_insert_history(&mut rx);
    chat.dispatch_command(SlashCommand::Continue);
    let lines = drain_insert_history(&mut rx)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    insta::assert_snapshot!(lines_to_single_string(&lines), @"■ '/continue' is disabled while a task is in progress.");
    assert!(op_rx.try_recv().is_err());
    assert_eq!(
        chat.turn_lifecycle.last_turn_id.as_deref(),
        Some("running-turn")
    );
}
