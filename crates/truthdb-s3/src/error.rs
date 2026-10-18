//! S3 error responses: an HTTP status and an XML `<Error>` body with the code
//! clients branch on.

/// One S3 error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Error {
    pub status: u16,
    pub code: &'static str,
    pub message: String,
}

impl S3Error {
    pub fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        S3Error {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn access_denied(message: impl Into<String>) -> Self {
        S3Error::new(403, "AccessDenied", message)
    }

    pub fn invalid_access_key() -> Self {
        S3Error::new(
            403,
            "InvalidAccessKeyId",
            "The AWS access key Id you provided does not exist in our records.",
        )
    }

    pub fn signature_mismatch() -> Self {
        S3Error::new(
            403,
            "SignatureDoesNotMatch",
            "The request signature we calculated does not match the signature you provided.",
        )
    }

    pub fn bad_digest(message: impl Into<String>) -> Self {
        S3Error::new(400, "BadDigest", message)
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        S3Error::new(400, "InvalidRequest", message)
    }

    pub fn not_implemented(message: impl Into<String>) -> Self {
        S3Error::new(501, "NotImplemented", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        S3Error::new(500, "InternalError", message)
    }

    /// The XML body; `resource` is the request path.
    pub fn to_xml(&self, resource: &str) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message><Resource>{}</Resource></Error>",
            self.code,
            escape(&self.message),
            escape(resource)
        )
    }
}

/// XML text escaping.
pub fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_render_as_s3_xml() {
        let xml = S3Error::access_denied("no <entry> & such").to_xml("/b/k");
        assert!(xml.contains("<Code>AccessDenied</Code>"));
        assert!(xml.contains("no &lt;entry&gt; &amp; such"));
        assert!(xml.contains("<Resource>/b/k</Resource>"));
    }
}
