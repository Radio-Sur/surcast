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
}
