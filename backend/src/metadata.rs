use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct EnrichedMetadata {
    pub title: String,
    pub artist: String,
    pub album: String,
}

#[derive(Deserialize)]
struct TrackSearchResponse {
    results: TrackSearchResults,
}

#[derive(Deserialize)]
struct TrackSearchResults {
    trackmatches: TrackMatches,
}

#[derive(Deserialize)]
struct TrackMatches {
    track: Vec<TrackSearchHit>,
}

#[derive(Deserialize)]
struct TrackSearchHit {
    name: String,
    artist: String,
}

#[derive(Deserialize)]
struct TrackInfoResponse {
    track: TrackInfo,
}

#[derive(Deserialize)]
struct TrackInfo {
    name: String,
    artist: TrackInfoArtist,
    album: Option<TrackInfoAlbum>,
}

#[derive(Deserialize)]
struct TrackInfoArtist {
    name: String,
}

#[derive(Deserialize)]
struct TrackInfoAlbum {
    title: String,
}

/// Try to extract artist and title from a filename (without extension).
/// Supports "Artist - Title", "01 - Artist - Title", "01. Artist - Title".
pub fn parse_filename(name: &str) -> (String, String) {
    let cleaned = name.trim().replace('_', " ").split_whitespace().collect::<Vec<_>>().join(" ");

    let cleaned = if let Some(space) = cleaned.find(' ') {
        let first_word = &cleaned[..space];
        if first_word.bytes().all(|b| b.is_ascii_digit()) {
            cleaned[space..].trim().to_string()
        } else {
            cleaned
        }
    } else {
        cleaned
    };
    let cleaned = if let Some(dot) = cleaned.find('.') {
        let first_word = &cleaned[..dot];
        if first_word.bytes().all(|b| b.is_ascii_digit()) {
            cleaned[dot + 1..].trim().to_string()
        } else {
            cleaned
        }
    } else {
        cleaned
    };

    let cleaned = {
        let mut s = cleaned;
        for (open, close) in [('(', ')'), ('[', ']')] {
            while let Some(start) = s.find(open) {
                if let Some(end) = s[start..].find(close) {
                    s = format!("{}{}", &s[..start], &s[start + end + 1..]);
                } else {
                    break;
                }
            }
        }
        s.split_whitespace().collect::<Vec<_>>().join(" ").trim().to_string()
    };

    if let Some(dash_pos) = cleaned.find(" - ") {
        let artist = cleaned[..dash_pos].trim().to_string();
        let title = cleaned[dash_pos + 3..].trim().to_string();
        if !artist.is_empty() && !title.is_empty() {
            return (artist, title);
        }
    }
    ("".to_string(), cleaned.to_string())
}

pub async fn enrich(api_key: &str, title: &str, artist: &str) -> Option<EnrichedMetadata> {
    if !artist.is_empty() && !title.is_empty() {
        if let Some(info) = fetch_track_info(api_key, artist, title).await {
            return Some(info);
        }
    }
    if !title.is_empty() {
        if let Some(info) = search_and_enrich(api_key, title, artist).await {
            return Some(info);
        }
    }
    None
}

async fn search_and_enrich(api_key: &str, title: &str, _fallback_artist: &str) -> Option<EnrichedMetadata> {
    let url = format!(
        "https://ws.audioscrobbler.com/2.0/?method=track.search&track={}&api_key={}&format=json&limit=1",
        util::url_encode(title),
        api_key,
    );

    let resp: TrackSearchResponse = reqwest::get(&url).await.ok()?.json().await.ok()?;
    let hit = resp.results.trackmatches.track.into_iter().next()?;

    let artist = hit.artist;
    let corrected_title = hit.name;

    fetch_track_info(api_key, &artist, &corrected_title).await
}

async fn fetch_track_info(api_key: &str, artist: &str, track: &str) -> Option<EnrichedMetadata> {
    let url = format!(
        "https://ws.audioscrobbler.com/2.0/?method=track.getInfo&artist={}&track={}&api_key={}&format=json",
        util::url_encode(artist),
        util::url_encode(track),
        api_key,
    );

    let resp: TrackInfoResponse = reqwest::get(&url).await.ok()?.json().await.ok()?;

    let corrected_title = resp.track.name;
    let corrected_artist = resp.track.artist.name;

    let album = resp.track.album.as_ref().map(|album| album.title.clone()).unwrap_or_default();

    Some(EnrichedMetadata {
        title: corrected_title,
        artist: corrected_artist,
        album,
    })
}

use crate::util;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_filename_artist_title() {
        let (artist, title) = parse_filename("Artist - Title");
        assert_eq!(artist, "Artist");
        assert_eq!(title, "Title");
    }

    #[test]
    fn test_parse_filename_feat() {
        let (artist, title) = parse_filename("Artist - Title (feat. X)");
        assert_eq!(artist, "Artist");
        assert_eq!(title, "Title");
    }

    #[test]
    fn test_parse_filename_no_separator() {
        let (artist, title) = parse_filename("NoSeparator");
        assert_eq!(artist, "");
        assert_eq!(title, "NoSeparator");
    }

    #[test]
    fn test_parse_filename_empty() {
        let (artist, title) = parse_filename("");
        assert_eq!(artist, "");
        assert_eq!(title, "");
    }

    #[test]
    fn test_parse_filename_multiple_dashes() {
        let (artist, title) = parse_filename("Artist - Title - Remix");
        assert_eq!(artist, "Artist");
        assert_eq!(title, "Title - Remix");
    }

    #[test]
    fn test_parse_filename_track_number_dot() {
        let (artist, title) = parse_filename("01. Artist - Title");
        assert_eq!(artist, "Artist");
        assert_eq!(title, "Title");
    }
}
