use std::collections::HashSet;

use uuid::Uuid;

use super::SongInfo;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct QueueAnchor {
    pub consumed_queue_item_ids: HashSet<Uuid>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueueCursor {
    pub current_queue_item_id: Option<Uuid>,
    pub consumed_queue_item_ids: Vec<Uuid>,
    pub legacy_position: i32,
}

pub(crate) struct QueueState {
    items: Vec<SongInfo>,
    current: Option<SongInfo>,
    consumed: HashSet<Uuid>,
    legacy_position: i32,
}

impl QueueState {
    pub(crate) fn new(items: Vec<SongInfo>, current_index: usize) -> Self {
        let current = items.get(current_index).cloned();
        let consumed = items.iter().take(current_index).map(|song| song.queue_item_id).collect();
        let legacy_position = current.as_ref().map_or(0, |song| song.position);
        Self {
            items,
            current,
            consumed,
            legacy_position,
        }
    }

    pub(crate) fn from_cursor(items: Vec<SongInfo>, cursor: QueueCursor) -> Self {
        let current = cursor
            .current_queue_item_id
            .and_then(|id| items.iter().find(|song| song.queue_item_id == id).cloned());
        Self {
            items,
            current,
            consumed: cursor.consumed_queue_item_ids.into_iter().collect(),
            legacy_position: cursor.legacy_position,
        }
    }

    pub(crate) fn replace(&mut self, items: Vec<SongInfo>, retain_missing_current: bool) {
        let existing = self.current.take();
        self.current = existing.and_then(|current| {
            items
                .iter()
                .find(|song| song.queue_item_id == current.queue_item_id)
                .cloned()
                .or_else(|| retain_missing_current.then_some(current))
        });
        self.items = items;
        if self.current.is_none() {
            self.current = self.first_unconsumed();
        }
        let present: HashSet<_> = self.items.iter().map(|song| song.queue_item_id).collect();
        self.consumed.retain(|id| present.contains(id));
    }

    pub(crate) fn current_song_index(&self) -> usize {
        self.current
            .as_ref()
            .and_then(|current| self.items.iter().position(|song| song.queue_item_id == current.queue_item_id))
            .unwrap_or(0)
    }

    pub(crate) fn song_count(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn current_song_info(&self) -> Option<SongInfo> {
        self.current.clone()
    }
    pub(crate) fn song_by_queue_item_id(&self, queue_item_id: Uuid) -> Option<SongInfo> {
        self.items
            .iter()
            .find(|song| song.queue_item_id == queue_item_id)
            .cloned()
            .or_else(|| self.current.as_ref().filter(|song| song.queue_item_id == queue_item_id).cloned())
    }

    pub(crate) fn peek_next_song(&self) -> Option<SongInfo> {
        self.first_unconsumed_excluding(self.current.as_ref().map(|song| song.queue_item_id))
    }

    pub(crate) fn successor_after(&self, key: &super::pipeline::TrackKey) -> Option<SongInfo> {
        let start = self
            .items
            .iter()
            .position(|song| song.queue_item_id == key.queue_item_id)
            .map_or(0, |index| index + 1);
        let current = self.current.as_ref().map(|song| song.queue_item_id);
        self.items[start..]
            .iter()
            .find(|song| !self.consumed.contains(&song.queue_item_id) && Some(song.queue_item_id) != current)
            .cloned()
    }

    pub(crate) fn anchor_after_current(&self) -> QueueAnchor {
        let mut consumed_queue_item_ids = self.consumed.clone();
        if let Some(current) = &self.current {
            consumed_queue_item_ids.insert(current.queue_item_id);
        }
        QueueAnchor { consumed_queue_item_ids }
    }

    pub(crate) fn commit_current(&mut self, song: SongInfo, anchor: QueueAnchor) -> Option<SongInfo> {
        self.consumed = anchor.consumed_queue_item_ids;
        self.legacy_position = song.position;
        self.current = Some(song);
        self.peek_next_song()
    }

    pub(crate) fn persistence_cursor(&self) -> QueueCursor {
        let mut consumed_queue_item_ids: Vec<_> = self.consumed.iter().copied().collect();
        consumed_queue_item_ids.sort_unstable();
        QueueCursor {
            current_queue_item_id: self.current.as_ref().map(|song| song.queue_item_id),
            consumed_queue_item_ids,
            legacy_position: self.legacy_position,
        }
    }

    pub(crate) fn upcoming(&self) -> i64 {
        self.items
            .iter()
            .filter(|song| {
                !self.consumed.contains(&song.queue_item_id)
                    && self
                        .current
                        .as_ref()
                        .is_none_or(|current| current.queue_item_id != song.queue_item_id)
            })
            .count() as i64
    }

    fn first_unconsumed(&self) -> Option<SongInfo> {
        self.first_unconsumed_excluding(None)
    }

    fn first_unconsumed_excluding(&self, excluded: Option<Uuid>) -> Option<SongInfo> {
        self.items
            .iter()
            .find(|song| !self.consumed.contains(&song.queue_item_id) && Some(song.queue_item_id) != excluded)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn song(queue_item_id: Uuid, position: i32) -> SongInfo {
        SongInfo {
            queue_item_id,
            song_id: Uuid::new_v4(),
            title: String::new(),
            artist: String::new(),
            duration: 1,
            file_path: String::new(),
            position,
            cue_in: 0.0,
            cue_out: 0.0,
            cross_start_next: 0.0,
            analyzed: false,
        }
    }

    #[test]
    fn prepared_anchor_does_not_consume_items_inserted_before_next() {
        let a = song(Uuid::new_v4(), 0);
        let b = song(Uuid::new_v4(), 1);
        let c = song(Uuid::new_v4(), 2);
        let d = song(Uuid::new_v4(), 3);
        let mut state = QueueState::new(vec![a, b.clone(), c.clone(), d.clone()], 0);
        let anchor = state.anchor_after_current();

        state.replace(vec![d.clone(), b.clone(), c], true);
        state.commit_current(b, anchor);

        assert_eq!(state.peek_next_song().unwrap().queue_item_id, d.queue_item_id);
    }

    #[test]
    fn detached_current_survives_reload_until_handover() {
        let a = song(Uuid::new_v4(), 0);
        let b = song(Uuid::new_v4(), 1);
        let c = song(Uuid::new_v4(), 2);
        let mut state = QueueState::new(vec![a.clone(), b, c.clone()], 0);

        state.replace(vec![c], true);

        assert_eq!(state.current_song_info().unwrap().queue_item_id, a.queue_item_id);
    }

    #[test]
    fn reorder_does_not_change_consumed_identity_set() {
        let a = song(Uuid::new_v4(), 0);
        let b = song(Uuid::new_v4(), 1);
        let c = song(Uuid::new_v4(), 2);
        let mut state = QueueState::new(vec![a.clone(), b.clone(), c.clone()], 0);
        let anchor = state.anchor_after_current();
        state.commit_current(b, anchor);

        state.replace(vec![c.clone(), a, state.current_song_info().unwrap()], true);

        assert_eq!(state.peek_next_song().unwrap().queue_item_id, c.queue_item_id);
    }

    #[test]
    fn cursor_restores_current_and_uses_latest_order_for_successor() {
        let a = song(Uuid::new_v4(), 0);
        let b = song(Uuid::new_v4(), 1);
        let c = song(Uuid::new_v4(), 2);
        let state = QueueState::from_cursor(
            vec![c.clone(), b.clone(), a.clone()],
            QueueCursor {
                current_queue_item_id: Some(b.queue_item_id),
                consumed_queue_item_ids: vec![a.queue_item_id],
                legacy_position: 1,
            },
        );

        assert_eq!(state.current_song_info().unwrap().queue_item_id, b.queue_item_id);
        assert_eq!(state.peek_next_song().unwrap().queue_item_id, c.queue_item_id);
    }
}
