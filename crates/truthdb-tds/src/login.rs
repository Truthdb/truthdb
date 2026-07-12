//! PRELOGIN response and LOGIN7 parsing (MS-TDS 2.2.6.5, 2.2.6.4).

/// Builds a PRELOGIN response: version, ENCRYPTION = NOT_SUP (plaintext
/// only), MARS off. The option table is a list of
/// `token u8 | offset u16 (BE) | length u16 (BE)` entries ended by `0xFF`,
/// followed by the option data.
pub fn prelogin_response() -> Vec<u8> {
    const TOKEN_VERSION: u8 = 0x00;
    const TOKEN_ENCRYPTION: u8 = 0x01;
    const TOKEN_MARS: u8 = 0x04;
    const TERMINATOR: u8 = 0xff;
    /// ENCRYPT_NOT_SUP: this server does not support TLS (Stage 4 plaintext).
    const ENCRYPT_NOT_SUP: u8 = 0x02;

    // Three options + terminator: table is 3*5 + 1 = 16 bytes; data follows.
    let table_len = 3 * 5 + 1;
    let version = [16u8, 0, 0, 0, 0, 0]; // major 16, build/subbuild 0
    let mut out = Vec::new();

    let version_off = table_len;
    let encryption_off = version_off + version.len();
    let mars_off = encryption_off + 1;

    let push_option = |out: &mut Vec<u8>, token: u8, offset: usize, len: usize| {
        out.push(token);
        out.extend_from_slice(&(offset as u16).to_be_bytes());
        out.extend_from_slice(&(len as u16).to_be_bytes());
    };
    push_option(&mut out, TOKEN_VERSION, version_off, version.len());
    push_option(&mut out, TOKEN_ENCRYPTION, encryption_off, 1);
    push_option(&mut out, TOKEN_MARS, mars_off, 1);
    out.push(TERMINATOR);

    out.extend_from_slice(&version);
    out.push(ENCRYPT_NOT_SUP);
    out.push(0x00); // MARS off
    out
}

/// The fields a LOGIN7 request carries that Stage 4 uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Login7 {
    pub username: String,
    pub password: String,
    pub database: String,
    /// Client-requested packet size (0 = keep the current one).
    pub packet_size: u32,
}

const FIXED_HEADER_LEN: usize = 36;
const OFFSET_TABLE_LEN: usize = 58; // 36..94

/// Parses a LOGIN7 message payload, extracting username, de-obfuscated
/// password, database, and requested packet size.
pub fn parse_login7(payload: &[u8]) -> Result<Login7, LoginError> {
    if payload.len() < FIXED_HEADER_LEN + OFFSET_TABLE_LEN {
        return Err(LoginError("LOGIN7 shorter than its fixed header"));
    }
    let packet_size = u32::from_le_bytes(payload[8..12].try_into().unwrap());

    // Offset/length pairs are relative to the start of the message and count
    // UCS-2 characters (2 bytes each).
    let field = |at: usize| -> (usize, usize) {
        let off = u16::from_le_bytes([payload[at], payload[at + 1]]) as usize;
        let cch = u16::from_le_bytes([payload[at + 2], payload[at + 3]]) as usize;
        (off, cch)
    };
    let (user_off, user_cch) = field(40);
    let (pass_off, pass_cch) = field(44);
    let (db_off, db_cch) = field(68);

    let username = read_ucs2(payload, user_off, user_cch, false)?;
    let password = read_ucs2(payload, pass_off, pass_cch, true)?;
    let database = read_ucs2(payload, db_off, db_cch, false)?;

    Ok(Login7 {
        username,
        password,
        database,
        packet_size,
    })
}

/// Reads `cch` UCS-2LE characters at `offset`. When `deobfuscate` is set the
/// bytes are un-scrambled first (LOGIN7 password obfuscation: swap nibbles,
/// XOR 0xA5).
fn read_ucs2(
    payload: &[u8],
    offset: usize,
    cch: usize,
    deobfuscate: bool,
) -> Result<String, LoginError> {
    let byte_len = cch * 2;
    if offset + byte_len > payload.len() {
        return Err(LoginError("LOGIN7 field extends past the message"));
    }
    let mut units = Vec::with_capacity(cch);
    for i in 0..cch {
        let mut lo = payload[offset + i * 2];
        let mut hi = payload[offset + i * 2 + 1];
        if deobfuscate {
            lo = deobfuscate_byte(lo);
            hi = deobfuscate_byte(hi);
        }
        units.push(u16::from_le_bytes([lo, hi]));
    }
    String::from_utf16(&units).map_err(|_| LoginError("invalid UTF-16 in LOGIN7 field"))
}

/// Inverts the LOGIN7 password obfuscation. Per MS-TDS 2.2.6.4 the client
/// obfuscates each byte by swapping its nibbles and then XOR-ing with 0xA5;
/// de-obfuscation therefore XORs with 0xA5 first, then swaps nibbles back.
fn deobfuscate_byte(b: u8) -> u8 {
    (b ^ 0xa5).rotate_left(4)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginError(pub &'static str);

impl std::fmt::Display for LoginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Encodes a &str to UCS-2LE (test/response helper).
pub fn ucs2le(s: &str) -> Vec<u8> {
    s.encode_utf16().flat_map(|u| u.to_le_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Obfuscates a password the way a client would, so the parser can be
    /// tested against its own inverse.
    fn obfuscate(s: &str) -> Vec<u8> {
        ucs2le(s)
            .into_iter()
            .map(|b| b.rotate_left(4) ^ 0xa5)
            .collect()
    }

    fn build_login7(user: &str, password: &str, database: &str) -> Vec<u8> {
        let mut payload = vec![0u8; 94];
        // Packet size field.
        payload[8..12].copy_from_slice(&4096u32.to_le_bytes());
        let mut data = Vec::new();
        let add = |payload: &mut Vec<u8>, data: &mut Vec<u8>, at: usize, bytes: &[u8]| {
            let offset = 94 + data.len();
            let cch = bytes.len() / 2;
            payload[at..at + 2].copy_from_slice(&(offset as u16).to_le_bytes());
            payload[at + 2..at + 4].copy_from_slice(&(cch as u16).to_le_bytes());
            data.extend_from_slice(bytes);
        };
        add(&mut payload, &mut data, 40, &ucs2le(user));
        add(&mut payload, &mut data, 44, &obfuscate(password));
        add(&mut payload, &mut data, 68, &ucs2le(database));
        payload.extend(data);
        payload
    }

    #[test]
    fn parses_login7_fields_and_deobfuscates_password() {
        let payload = build_login7("sa", "S3cr3t!", "truthdb");
        let login = parse_login7(&payload).expect("parse");
        assert_eq!(login.username, "sa");
        assert_eq!(login.password, "S3cr3t!");
        assert_eq!(login.database, "truthdb");
        assert_eq!(login.packet_size, 4096);
    }

    #[test]
    fn deobfuscation_is_the_inverse_of_obfuscation() {
        for b in 0u8..=255 {
            let obf = b.rotate_left(4) ^ 0xa5;
            assert_eq!(deobfuscate_byte(obf), b);
        }
    }

    #[test]
    fn prelogin_response_is_well_formed() {
        let resp = prelogin_response();
        // First option token is VERSION with a sane offset.
        assert_eq!(resp[0], 0x00);
        let version_off = u16::from_be_bytes([resp[1], resp[2]]) as usize;
        assert_eq!(version_off, 16);
        // Encryption byte is NOT_SUP.
        assert_eq!(resp[16 + 6], 0x02);
    }

    #[test]
    fn short_login7_errors() {
        assert!(parse_login7(&[0u8; 10]).is_err());
    }
}
