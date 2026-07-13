//! TDS transport-layer TLS (MS-TDS 2.2.6.5 / "TLS tunneled in PRELOGIN").
//!
//! SQL Server's TLS handshake is not a normal TLS-over-TCP handshake: while the
//! handshake is in progress, every TLS record is wrapped in a TDS packet of type
//! `PRELOGIN` (0x12); once the handshake completes, records flow raw and the
//! encrypted payload is ordinary TDS traffic (LOGIN7, SQL batches, ...).
//!
//! [`Detunnel`] is a byte stream that performs exactly this de-framing: while
//! `raw` is false it reads/writes TLS records inside 0x12 packets; once flipped
//! to true (right after the handshake finishes) it passes bytes through. We hand
//! it to `tokio-rustls`, which drives the actual handshake, and flip `raw` the
//! instant `accept()` resolves — at which point every handshake record has been
//! exchanged and only application data remains.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

const HEADER_LEN: usize = 8;
const PKT_PRELOGIN: u8 = 0x12;
const STATUS_EOM: u8 = 0x01;
/// Bounds a single handshake-phase TDS packet's payload (a TLS record is at most
/// ~16 KiB; this is generous while capping a pre-auth allocation).
const MAX_HANDSHAKE_PACKET: usize = 32 * 1024;
/// Max payload per handshake-phase 0x12 packet the server emits: the default
/// packet size (4096) minus the 8-byte header. A larger handshake flight is
/// split across packets (the packet size is not yet negotiated during PRELOGIN).
const HANDSHAKE_PACKET_DATA: usize = 4096 - HEADER_LEN;

/// Server-side TLS configuration built from a PEM certificate chain and key.
#[derive(Clone)]
pub struct TlsConfig {
    acceptor: TlsAcceptor,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TlsConfig(..)")
    }
}

/// A client stream that is either plaintext or (after a tunneled handshake) TLS.
/// Both variants are `AsyncRead + AsyncWrite`, so the LOGIN7/batch loop is
/// oblivious to encryption.
pub enum MaybeTlsStream<S> {
    Plain(S),
    Tls(Box<TlsStream<Detunnel<S>>>),
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for MaybeTlsStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for MaybeTlsStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

impl TlsConfig {
    /// Builds a config from a PEM certificate chain and a PEM private key.
    pub fn from_pem(cert_pem: &[u8], key_pem: &[u8]) -> io::Result<Self> {
        let certs = rustls_pemfile::certs(&mut &cert_pem[..])
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("bad certificate: {e}"))
            })?;
        if certs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "no certificate found in the TLS certificate PEM",
            ));
        }
        let key = rustls_pemfile::private_key(&mut &key_pem[..])
            .map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("bad private key: {e}"))
            })?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "no private key found in the TLS key PEM",
                )
            })?;
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| io::Error::other(format!("tls config: {e}")))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("tls config: {e}")))?;
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }

    /// Completes the tunneled TLS handshake over `io` and returns the encrypted
    /// stream for the rest of the session (LOGIN7 onward).
    pub async fn accept<S>(&self, io: S) -> io::Result<TlsStream<Detunnel<S>>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let raw = Arc::new(AtomicBool::new(false));
        let stream = self.acceptor.accept(Detunnel::new(io, raw.clone())).await?;
        // The handshake is done; every further record is raw application data.
        raw.store(true, Ordering::SeqCst);
        Ok(stream)
    }
}

/// Reads exactly one TDS packet header/body worth of bytes across polls.
#[derive(Default)]
struct PacketReader {
    header: [u8; HEADER_LEN],
    header_filled: usize,
    body: Vec<u8>,
    body_filled: usize,
    body_len: usize,
    have_header: bool,
}

/// A byte stream that tunnels TLS records inside TDS `PRELOGIN` (0x12) packets
/// while `raw` is false, and passes bytes through once it is true.
pub struct Detunnel<S> {
    io: S,
    raw: Arc<AtomicBool>,
    reader: PacketReader,
    /// Decoded handshake-phase bytes waiting to be delivered upward.
    read_out: Vec<u8>,
    read_out_pos: usize,
    /// Framed handshake-phase bytes waiting to be written to `io`.
    write_buf: Vec<u8>,
    write_pos: usize,
}

impl<S> Detunnel<S> {
    fn new(io: S, raw: Arc<AtomicBool>) -> Self {
        Self {
            io,
            raw,
            reader: PacketReader::default(),
            read_out: Vec::new(),
            read_out_pos: 0,
            write_buf: Vec::new(),
            write_pos: 0,
        }
    }
    fn is_raw(&self) -> bool {
        self.raw.load(Ordering::SeqCst)
    }
}

/// Reads into `dst[*filled..]` from `io`, updating `filled`. Ready(true) once
/// `dst` is full, Ready(false) on EOF, Pending if the socket would block.
fn poll_fill<S: AsyncRead + Unpin>(
    io: &mut S,
    dst: &mut [u8],
    filled: &mut usize,
    cx: &mut Context<'_>,
) -> Poll<io::Result<bool>> {
    while *filled < dst.len() {
        let mut rb = ReadBuf::new(&mut dst[*filled..]);
        match Pin::new(&mut *io).poll_read(cx, &mut rb) {
            Poll::Ready(Ok(())) => {
                let n = rb.filled().len();
                if n == 0 {
                    return Poll::Ready(Ok(false)); // EOF
                }
                *filled += n;
            }
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }
    }
    Poll::Ready(Ok(true))
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for Detunnel<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.is_raw() {
            return Pin::new(&mut this.io).poll_read(cx, buf);
        }
        loop {
            // Deliver any buffered decoded bytes first.
            if this.read_out_pos < this.read_out.len() {
                let n = buf.remaining().min(this.read_out.len() - this.read_out_pos);
                buf.put_slice(&this.read_out[this.read_out_pos..this.read_out_pos + n]);
                this.read_out_pos += n;
                return Poll::Ready(Ok(()));
            }
            this.read_out.clear();
            this.read_out_pos = 0;

            let r = &mut this.reader;
            if !r.have_header {
                match poll_fill(&mut this.io, &mut r.header, &mut r.header_filled, cx)? {
                    Poll::Ready(true) => {}
                    Poll::Ready(false) => return Poll::Ready(Ok(())), // EOF -> 0 bytes
                    Poll::Pending => return Poll::Pending,
                }
                let length = u16::from_be_bytes([r.header[2], r.header[3]]) as usize;
                if length < HEADER_LEN || length - HEADER_LEN > MAX_HANDSHAKE_PACKET {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid TDS handshake packet length",
                    )));
                }
                r.body_len = length - HEADER_LEN;
                r.body = vec![0u8; r.body_len];
                r.body_filled = 0;
                r.have_header = true;
            }
            match poll_fill(&mut this.io, &mut r.body, &mut r.body_filled, cx)? {
                Poll::Ready(true) => {}
                Poll::Ready(false) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "EOF within a TDS handshake packet",
                    )));
                }
                Poll::Pending => return Poll::Pending,
            }
            // One packet decoded; its body is the TLS record bytes.
            this.read_out = std::mem::take(&mut r.body);
            this.read_out_pos = 0;
            *r = PacketReader::default();
            // Loop to deliver the freshly decoded bytes.
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Detunnel<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.is_raw() {
            return Pin::new(&mut this.io).poll_write(cx, data);
        }
        // Drain any framed bytes still pending before accepting more.
        while this.write_pos < this.write_buf.len() {
            match Pin::new(&mut this.io).poll_write(cx, &this.write_buf[this.write_pos..])? {
                Poll::Ready(n) => this.write_pos += n,
                Poll::Pending => return Poll::Pending,
            }
        }
        this.write_buf.clear();
        this.write_pos = 0;
        // Frame this write as one or more 0x12 packets no larger than the
        // handshake-phase packet size (a large handshake flight — e.g. a big
        // certificate chain — must be split, since the packet size is not yet
        // negotiated and clients cap it). EOM is set only on the final packet.
        let mut offset = 0;
        let mut packet_id: u8 = 1;
        loop {
            let end = (offset + HANDSHAKE_PACKET_DATA).min(data.len());
            let is_last = end == data.len();
            let chunk = &data[offset..end];
            let length = (HEADER_LEN + chunk.len()) as u16;
            this.write_buf.extend_from_slice(&[
                PKT_PRELOGIN,
                if is_last { STATUS_EOM } else { 0 },
                (length >> 8) as u8,
                (length & 0xff) as u8,
                0,
                0,
                packet_id,
                0,
            ]);
            this.write_buf.extend_from_slice(chunk);
            packet_id = packet_id.wrapping_add(1);
            offset = end;
            if is_last {
                break;
            }
        }
        while this.write_pos < this.write_buf.len() {
            match Pin::new(&mut this.io).poll_write(cx, &this.write_buf[this.write_pos..])? {
                Poll::Ready(n) => this.write_pos += n,
                Poll::Pending => return Poll::Pending,
            }
        }
        this.write_buf.clear();
        this.write_pos = 0;
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while this.write_pos < this.write_buf.len() {
            match Pin::new(&mut this.io).poll_write(cx, &this.write_buf[this.write_pos..])? {
                Poll::Ready(n) => this.write_pos += n,
                Poll::Pending => return Poll::Pending,
            }
        }
        this.write_buf.clear();
        this.write_pos = 0;
        Pin::new(&mut this.io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn detunnel_frames_handshake_and_passes_raw() {
        let (mut peer, server) = tokio::io::duplex(4096);
        let raw = Arc::new(AtomicBool::new(false));
        let mut det = Detunnel::new(server, raw.clone());

        // Handshake phase: a write is wrapped in one EOM'd 0x12 packet.
        det.write_all(b"hello").await.unwrap();
        det.flush().await.unwrap();
        let mut hdr = [0u8; HEADER_LEN];
        peer.read_exact(&mut hdr).await.unwrap();
        assert_eq!(hdr[0], PKT_PRELOGIN);
        assert_eq!(hdr[1], STATUS_EOM);
        let len = u16::from_be_bytes([hdr[2], hdr[3]]) as usize;
        let mut body = vec![0u8; len - HEADER_LEN];
        peer.read_exact(&mut body).await.unwrap();
        assert_eq!(body, b"hello");

        // Handshake phase: a 0x12 packet from the peer is de-framed on read.
        let payload = b"world";
        let l = (HEADER_LEN + payload.len()) as u16;
        peer.write_all(&[
            PKT_PRELOGIN,
            STATUS_EOM,
            (l >> 8) as u8,
            (l & 0xff) as u8,
            0,
            0,
            1,
            0,
        ])
        .await
        .unwrap();
        peer.write_all(payload).await.unwrap();
        peer.flush().await.unwrap();
        let mut got = [0u8; 5];
        det.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"world");

        // After the handshake, bytes pass through unframed in both directions.
        raw.store(true, Ordering::SeqCst);
        det.write_all(b"raw-out").await.unwrap();
        det.flush().await.unwrap();
        let mut r = [0u8; 7];
        peer.read_exact(&mut r).await.unwrap();
        assert_eq!(&r, b"raw-out");
        peer.write_all(b"raw-in").await.unwrap();
        peer.flush().await.unwrap();
        let mut r2 = [0u8; 6];
        det.read_exact(&mut r2).await.unwrap();
        assert_eq!(&r2, b"raw-in");
    }

    #[tokio::test]
    async fn detunnel_splits_a_large_handshake_flight() {
        // A flight larger than one packet is split; EOM only on the last, and
        // the peer reassembles the original bytes.
        let (mut peer, server) = tokio::io::duplex(1 << 16);
        let raw = Arc::new(AtomicBool::new(false));
        let mut det = Detunnel::new(server, raw);
        let flight: Vec<u8> = (0..(HANDSHAKE_PACKET_DATA * 2 + 100))
            .map(|i| (i % 253) as u8)
            .collect();
        det.write_all(&flight).await.unwrap();
        det.flush().await.unwrap();

        let mut reassembled = Vec::new();
        let mut packets = 0;
        loop {
            let mut hdr = [0u8; HEADER_LEN];
            peer.read_exact(&mut hdr).await.unwrap();
            assert_eq!(hdr[0], PKT_PRELOGIN);
            let len = u16::from_be_bytes([hdr[2], hdr[3]]) as usize;
            let mut body = vec![0u8; len - HEADER_LEN];
            peer.read_exact(&mut body).await.unwrap();
            reassembled.extend_from_slice(&body);
            packets += 1;
            if hdr[1] & STATUS_EOM != 0 {
                break;
            }
        }
        assert_eq!(packets, 3);
        assert_eq!(reassembled, flight);
    }
}
