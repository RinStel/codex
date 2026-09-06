//! Hydrate a complete turn from lineage-aware persisted item pages.

use super::thread_processor::THREAD_ITEMS_MAX_LIMIT;
use super::thread_processor::deserialize_stored_thread_item;
use super::thread_processor::paginated_history_list_error;
use super::*;

pub(super) async fn paginated_turn_full_items(
    thread_store: &dyn ThreadStore,
    thread_id: ThreadId,
    turn_id: &str,
) -> Result<Vec<ThreadItem>, JSONRPCErrorError> {
    let mut cursor = None;
    let mut items = Vec::new();
    loop {
        let page = thread_store
            .list_items(StoreListItemsParams {
                thread_id,
                turn_id: Some(turn_id.to_string()),
                include_archived: true,
                cursor: cursor.clone(),
                page_size: THREAD_ITEMS_MAX_LIMIT,
                sort_direction: StoreSortDirection::Asc,
                sort_key: StoreItemSortKey::CreatedAtOrdinal,
                after_updated_at_ordinal: None,
            })
            .await
            .map_err(paginated_history_list_error)?;
        for item in page.items {
            items.push(deserialize_stored_thread_item(item)?);
        }
        let Some(next_cursor) = page.next_cursor else {
            return Ok(items);
        };
        if cursor.as_ref() == Some(&next_cursor) {
            return Err(internal_error(format!(
                "failed to load full turn items for {turn_id}: thread store returned a repeated cursor"
            )));
        }
        cursor = Some(next_cursor);
    }
}
