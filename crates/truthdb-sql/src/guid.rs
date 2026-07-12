//! UNIQUEIDENTIFIER text <-> byte conversions. The stored 16-byte order keeps
//! the first three groups little-endian (SQL Server's on-wire order), so the
//! canonical `8-4-4-4-12` text reverses those groups.

/// Renders 16 bytes as canonical uppercase `8-4-4-4-12`.
pub fn render(b: &[u8; 16]) -> String {
    format!(
        "{:02X}{:02X}{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}",
        b[3],
        b[2],
        b[1],
        b[0],
        b[5],
        b[4],
        b[7],
        b[6],
        b[8],
        b[9],
        b[10],
        b[11],
        b[12],
        b[13],
        b[14],
        b[15]
    )
}

/// Byte order mapping text position -> stored byte (first three groups LE).
const ORDER: [usize; 16] = [3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15];

/// Parses a GUID string (optional braces, hyphens) into the stored byte order.
pub fn parse(s: &str) -> Option<[u8; 16]> {
    let hex: String = s
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}')
        .chars()
        .filter(|c| *c != '-')
        .collect();
    if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 16];
    for (text_pos, stored) in ORDER.iter().enumerate() {
        out[*stored] = u8::from_str_radix(&hex[text_pos * 2..text_pos * 2 + 2], 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let text = "6F9619FF-8B86-D011-B42D-00C04FC964FF";
        let bytes = parse(text).expect("parse");
        assert_eq!(render(&bytes), text);
        // Hyphen-less and braced forms parse identically.
        assert_eq!(parse("6F9619FF8B86D011B42D00C04FC964FF"), Some(bytes));
        assert_eq!(parse("{6F9619FF-8B86-D011-B42D-00C04FC964FF}"), Some(bytes));
        assert_eq!(parse("not-a-guid"), None);
    }
}
