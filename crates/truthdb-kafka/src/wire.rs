//! Kafka's primitive types as the non-flexible message versions encode
//! them: big-endian integers, strings and byte arrays behind a signed
//! length (-1: null), arrays behind an `int32` count, and the zigzag
//! varints of record batches.

/// A request that does not parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Malformed;

/// Reads a request body from the front.
pub struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Reader { bytes }
    }

    /// What is left to read.
    pub fn rest(&self) -> &'a [u8] {
        self.bytes
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], Malformed> {
        if self.bytes.len() < n {
            return Err(Malformed);
        }
        let (head, rest) = self.bytes.split_at(n);
        self.bytes = rest;
        Ok(head)
    }

    pub fn i8(&mut self) -> Result<i8, Malformed> {
        Ok(self.take(1)?[0] as i8)
    }

    pub fn i16(&mut self) -> Result<i16, Malformed> {
        Ok(i16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    pub fn i32(&mut self) -> Result<i32, Malformed> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn i64(&mut self) -> Result<i64, Malformed> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn nullable_string(&mut self) -> Result<Option<String>, Malformed> {
        let len = self.i16()?;
        if len < 0 {
            return Ok(None);
        }
        let bytes = self.take(len as usize)?;
        String::from_utf8(bytes.to_vec())
            .map(Some)
            .map_err(|_| Malformed)
    }

    pub fn string(&mut self) -> Result<String, Malformed> {
        self.nullable_string()?.ok_or(Malformed)
    }

    pub fn bytes(&mut self) -> Result<Vec<u8>, Malformed> {
        let len = self.i32()?;
        if len < 0 {
            return Err(Malformed);
        }
        Ok(self.take(len as usize)?.to_vec())
    }

    /// An array's count, `None` for a null array. Each element takes a byte
    /// at least, so a count past the bytes left is refused before anything
    /// is allocated for it.
    pub fn nullable_count(&mut self) -> Result<Option<usize>, Malformed> {
        let count = self.i32()?;
        if count < 0 {
            return Ok(None);
        }
        let count = count as usize;
        if count > self.bytes.len() {
            return Err(Malformed);
        }
        Ok(Some(count))
    }

    /// An array of elements read by `item` (`None`: a null array).
    pub fn nullable_array<T>(
        &mut self,
        mut item: impl FnMut(&mut Self) -> Result<T, Malformed>,
    ) -> Result<Option<Vec<T>>, Malformed> {
        let Some(count) = self.nullable_count()? else {
            return Ok(None);
        };
        (0..count)
            .map(|_| item(self))
            .collect::<Result<_, _>>()
            .map(Some)
    }

    pub fn array<T>(
        &mut self,
        item: impl FnMut(&mut Self) -> Result<T, Malformed>,
    ) -> Result<Vec<T>, Malformed> {
        self.nullable_array(item)?.ok_or(Malformed)
    }
}

/// Writes a response body.
#[derive(Default)]
pub struct Writer {
    pub bytes: Vec<u8>,
}

impl Writer {
    pub fn i8(&mut self, value: i8) -> &mut Self {
        self.bytes.push(value as u8);
        self
    }

    pub fn bool(&mut self, value: bool) -> &mut Self {
        self.i8(value as i8)
    }

    pub fn i16(&mut self, value: i16) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn i32(&mut self, value: i32) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_be_bytes());
        self
    }

    pub fn i64(&mut self, value: i64) -> &mut Self {
        self.bytes.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// A string (the protocol's names and messages are far below 32 KiB).
    pub fn string(&mut self, value: &str) -> &mut Self {
        self.i16(value.len() as i16);
        self.bytes.extend_from_slice(value.as_bytes());
        self
    }

    pub fn nullable_string(&mut self, value: Option<&str>) -> &mut Self {
        match value {
            Some(value) => self.string(value),
            None => self.i16(-1),
        }
    }

    pub fn bytes(&mut self, value: &[u8]) -> &mut Self {
        self.i32(value.len() as i32);
        self.bytes.extend_from_slice(value);
        self
    }

    /// An array of `items`, each written by `item`.
    pub fn array<T>(&mut self, items: &[T], mut item: impl FnMut(&mut Self, &T)) -> &mut Self {
        self.i32(items.len() as i32);
        for value in items {
            item(self, value);
        }
        self
    }

    /// A zigzag varint (record batches' `varint` and `varlong`).
    pub fn varint(&mut self, value: i64) -> &mut Self {
        let mut zigzag = ((value << 1) ^ (value >> 63)) as u64;
        while zigzag >= 0x80 {
            self.bytes.push((zigzag as u8) | 0x80);
            zigzag >>= 7;
        }
        self.bytes.push(zigzag as u8);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_zigzag() {
        // Kafka's ByteUtils: 0 -> 00, -1 -> 01, 1 -> 02, 63 -> 7e, -65 -> 81 01,
        // 300 -> d8 04.
        let cases: [(i64, &[u8]); 6] = [
            (0, &[0x00]),
            (-1, &[0x01]),
            (1, &[0x02]),
            (63, &[0x7e]),
            (-65, &[0x81, 0x01]),
            (300, &[0xd8, 0x04]),
        ];
        for (value, expected) in cases {
            let mut writer = Writer::default();
            writer.varint(value);
            assert_eq!(writer.bytes, expected, "{value}");
        }
    }

    #[test]
    fn reads_what_is_written() {
        let mut writer = Writer::default();
        writer
            .i8(-2)
            .i16(-300)
            .i32(70_000)
            .i64(-1)
            .string("topic")
            .nullable_string(None)
            .bytes(b"abc")
            .array(&[1, 2], |w, v| {
                w.i32(*v);
            });
        let mut reader = Reader::new(&writer.bytes);
        assert_eq!(reader.i8(), Ok(-2));
        assert_eq!(reader.i16(), Ok(-300));
        assert_eq!(reader.i32(), Ok(70_000));
        assert_eq!(reader.i64(), Ok(-1));
        assert_eq!(reader.string(), Ok("topic".to_string()));
        assert_eq!(reader.nullable_string(), Ok(None));
        assert_eq!(reader.bytes(), Ok(b"abc".to_vec()));
        assert_eq!(reader.array(|r| r.i32()), Ok(vec![1, 2]));
        assert_eq!(reader.i8(), Err(Malformed), "nothing left");
    }

    #[test]
    fn refuses_counts_past_the_bytes_left() {
        // A count of a billion elements in a 4-byte body: refused before any
        // allocation.
        let mut reader = Reader::new(&[0x3b, 0x9a, 0xca, 0x00]);
        assert_eq!(reader.array(|r| r.i8()), Err(Malformed));
        let mut reader = Reader::new(&[0, 0, 0, 9, 1]);
        assert_eq!(reader.bytes(), Err(Malformed), "a byte array past the end");
        let mut reader = Reader::new(&[0xff, 0xff]);
        assert_eq!(
            reader.string(),
            Err(Malformed),
            "a null where a string is due"
        );
        let mut reader = Reader::new(&[0, 2, 0xff, 0xfe]);
        assert_eq!(reader.string(), Err(Malformed), "not UTF-8");
    }
}
