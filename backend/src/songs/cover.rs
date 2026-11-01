//! Reads embedded cover art straight from the audio file's tags.
//!
//! Nothing is persisted: covers live in the music file, so serving one is a
//! cheap tag read (no audio decode). Only properties parsing is skipped, so
//! files without audio frames still work.

use lofty::config::ParseOptions;
use lofty::file::TaggedFileExt;
use lofty::picture::MimeType;
use lofty::probe::Probe;

use crate::songs::handlers::cover_extension_from_mime;

/// Cover art pulled from an audio file's tags.
pub(crate) struct EmbeddedCover {
    pub data: Vec<u8>,
    pub mime: String,
}

/// Reads the first embedded picture from `path`. Returns `None` when the file
/// has no tag, no picture, an unsupported image format, or cannot be read.
pub(crate) async fn read_embedded_cover(path: String) -> Option<EmbeddedCover> {
    tokio::task::spawn_blocking(move || read_embedded_cover_blocking(std::path::Path::new(&path)))
        .await
        .ok()
        .flatten()
}

fn read_embedded_cover_blocking(path: &std::path::Path) -> Option<EmbeddedCover> {
    let tagged = Probe::open(path)
        .ok()?
        .options(ParseOptions::new().read_properties(false))
        .read()
        .ok()?;
    let tag = tagged.primary_tag().or_else(|| tagged.first_tag())?;
    let picture = tag.pictures().iter().find(|p| !p.data().is_empty())?;
    let mime = picture.mime_type().map(MimeType::as_str).unwrap_or("image/jpeg");
    cover_extension_from_mime(mime)?;
    Some(EmbeddedCover {
        data: picture.data().to_vec(),
        mime: mime.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use id3::TagLike;
    use std::io::Write;

    const COVER_BASE64: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9Zl1sAAAAASUVORK5CYII=";

    fn cover_bytes() -> Vec<u8> {
        base64::engine::general_purpose::STANDARD.decode(COVER_BASE64).unwrap()
    }

    fn write_mp3_with_cover(path: &std::path::Path) {
        let mut tag = id3::Tag::new();
        tag.set_title("Title");
        tag.add_frame(id3::frame::Picture {
            mime_type: "image/png".to_string(),
            picture_type: id3::frame::PictureType::CoverFront,
            description: String::new(),
            data: cover_bytes(),
        });
        tag.write_to_path(path, id3::Version::Id3v24).unwrap();
        // A minimal, silent MPEG frame so the file is a valid MP3 container.
        std::fs::OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(&mpeg_silent_frame())
            .unwrap();
    }

    /// One 128 kbps, 44.1 kHz MPEG-1 Layer III frame of silence.
    fn mpeg_silent_frame() -> Vec<u8> {
        let mut frame = vec![0xFF, 0xFB, 0x90, 0x64];
        frame.resize(417, 0);
        frame
    }

    #[test]
    fn reads_embedded_cover() {
        let file = tempfile::Builder::new().suffix(".mp3").tempfile().unwrap();
        write_mp3_with_cover(file.path());

        let cover = read_embedded_cover_blocking(file.path()).expect("cover should be found");
        assert_eq!(cover.data, cover_bytes());
        assert_eq!(cover.mime, "image/png");
    }

    #[test]
    fn missing_cover_returns_none() {
        let file = tempfile::Builder::new().suffix(".mp3").tempfile().unwrap();
        std::fs::write(file.path(), mpeg_silent_frame()).unwrap();

        assert!(read_embedded_cover_blocking(file.path()).is_none());
    }
}
