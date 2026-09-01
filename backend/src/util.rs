use std::path::{Component, Path, PathBuf};

/// Lexically normalizes a path by resolving `.` and `..` components without
/// touching the filesystem (no `canonicalize`). Preserves `RootDir`/`Prefix`.
pub fn normalize_lexically(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            _ => out.push(comp.as_os_str()),
        }
    }
    if out.as_os_str().is_empty() {
        out.push(Component::CurDir.as_os_str());
    }
    out
}

pub fn url_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(b as char);
            }
            b' ' => result.push_str("%20"),
            _ => {
                result.push_str(&format!("%{:02X}", b));
            }
        }
    }
    result
}

pub fn slugify(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == ' ' {
                c.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect();
    let s: Vec<&str> = s.split_whitespace().collect();
    s.join("-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_encode_normal_ascii() {
        assert_eq!(url_encode("hello"), "hello");
    }

    #[test]
    fn test_url_encode_special_chars() {
        assert_eq!(url_encode("a&b%c=d e"), "a%26b%25c%3Dd%20e");
    }

    #[test]
    fn test_url_encode_unicode() {
        assert_eq!(url_encode("zażółć"), "za%C5%BC%C3%B3%C5%82%C4%87");
    }

    #[test]
    fn test_url_encode_empty() {
        assert_eq!(url_encode(""), "");
    }

    #[test]
    fn test_url_encode_long() {
        let input = "a".repeat(1000);
        assert_eq!(url_encode(&input), input);
    }

    #[test]
    fn test_slugify_normal() {
        assert_eq!(slugify("My Station"), "my-station");
    }

    #[test]
    fn test_slugify_special_chars() {
        assert_eq!(slugify("Station #1"), "station-1");
    }

    #[test]
    fn test_slugify_unicode() {
        assert_eq!(slugify("Stacja Muzyczna"), "stacja-muzyczna");
    }

    #[test]
    fn test_slugify_empty() {
        assert_eq!(slugify(""), "");
    }

    #[test]
    fn test_normalize_lexically_dot_dot() {
        use std::path::Path;
        assert_eq!(
            normalize_lexically(Path::new("/home/a/backend/./../uploads/audio/x.mp3")),
            Path::new("/home/a/uploads/audio/x.mp3")
        );
        assert_eq!(
            normalize_lexically(Path::new("/home/pengwius/dev/surcast/backend/./../uploads/audio/x.mp3")),
            Path::new("/home/pengwius/dev/surcast/uploads/audio/x.mp3")
        );
    }

    #[test]
    fn test_normalize_lexically_preserves_absolute() {
        use std::path::Path;
        assert_eq!(
            normalize_lexically(Path::new("/a/b/c")),
            Path::new("/a/b/c")
        );
    }

    #[test]
    fn test_normalize_lexically_relative() {
        use std::path::Path;
        assert_eq!(
            normalize_lexically(Path::new("./../uploads")),
            Path::new("uploads")
        );
    }
}
