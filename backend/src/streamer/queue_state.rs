use super::pipeline::TrackKey;
use super::SongInfo;

pub(crate) struct QueueState {
    items: Vec<SongInfo>,
    current_index: usize,
}

impl QueueState {
    pub(crate) fn new(items: Vec<SongInfo>, current_index: usize) -> Self {
        Self { items, current_index }
    }

    pub(crate) fn replace(&mut self, items: Vec<SongInfo>) {
        self.items = items;
    }

    pub(crate) fn replace_with_current_index(&mut self, items: Vec<SongInfo>, current_index: usize) {
        self.items = items;
        self.current_index = current_index;
    }

    pub(crate) fn current_song_index(&self) -> usize {
        if self.items.is_empty() {
            0
        } else {
            self.current_index.min(self.items.len() - 1)
        }
    }

    pub(crate) fn song_count(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn song_info(&self, index: usize) -> Option<SongInfo> {
        self.items.get(index).cloned()
    }

    pub(crate) fn current_song_info(&self) -> Option<SongInfo> {
        self.song_info(self.current_index)
    }

    pub(crate) fn peek_next_song(&self) -> Option<SongInfo> {
        self.song_info(self.current_index + 1)
    }

    pub(crate) fn successor_after(&self, key: &TrackKey) -> Option<SongInfo> {
        self.successor_index(key).and_then(|index| self.song_info(index))
    }

    pub(crate) fn commit_current(&mut self, key: &TrackKey) -> Option<TrackKey> {
        if let Some(index) = self.items.iter().position(|song| song.queue_item_id == key.queue_item_id) {
            self.current_index = index;
        }
        self.successor_after(key).map(track_key)
    }

    pub(crate) fn current_position(&self, fallback: i32) -> i32 {
        self.current_song_info().map_or(fallback, |song| song.position)
    }

    pub(crate) fn upcoming_after(&self, key: &TrackKey) -> i64 {
        let start = self.successor_index(key).unwrap_or(self.items.len());
        self.items.len().saturating_sub(start) as i64
    }

    fn successor_index(&self, key: &TrackKey) -> Option<usize> {
        let start = self
            .items
            .iter()
            .position(|song| song.queue_item_id == key.queue_item_id)
            .map(|index| index + 1)
            .unwrap_or_else(|| {
                self.items
                    .iter()
                    .position(|song| song.position > key.position)
                    .unwrap_or(self.items.len())
            });
        (start < self.items.len()).then_some(start)
    }
}

fn track_key(song: SongInfo) -> TrackKey {
    TrackKey {
        queue_item_id: song.queue_item_id,
        song_id: song.song_id,
        position: song.position,
    }
}
