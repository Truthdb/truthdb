//! Combining CRCs: the CRC of two byte strings one after the other, from
//! the CRC of each and the second one's length, as zlib's `crc32_combine`
//! does (by GF(2) matrices). It holds for the reflected CRCs S3 offers a
//! full-object multipart checksum with: CRC32, CRC32C and CRC64NVME.

/// A reflected CRC of `width` bits with (reflected) polynomial `poly`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Crc {
    pub poly: u64,
    pub width: usize,
}

/// CRC-32 (ISO-HDLC), S3's `CRC32`.
pub(crate) const CRC32: Crc = Crc {
    poly: 0xedb8_8320,
    width: 32,
};
/// CRC-32C (Castagnoli), S3's `CRC32C`.
pub(crate) const CRC32C: Crc = Crc {
    poly: 0x82f6_3b78,
    width: 32,
};
/// CRC-64/NVME, S3's `CRC64NVME`.
pub(crate) const CRC64NVME: Crc = Crc {
    poly: 0x9a6c_9329_ac4b_c9b5,
    width: 64,
};

fn times(matrix: &[u64], mut vector: u64) -> u64 {
    let mut sum = 0;
    let mut row = 0;
    while vector != 0 {
        if vector & 1 != 0 {
            sum ^= matrix[row];
        }
        vector >>= 1;
        row += 1;
    }
    sum
}

fn square(matrix: &[u64]) -> Vec<u64> {
    matrix.iter().map(|&row| times(matrix, row)).collect()
}

impl Crc {
    /// The CRC of `a` then `b`, from `a`'s CRC, `b`'s, and `b`'s length.
    pub(crate) fn combine(self, mut first: u64, second: u64, mut second_len: u64) -> u64 {
        if second_len == 0 {
            return first;
        }
        // The operator for one zero bit, then two, then four.
        let mut odd = vec![self.poly];
        odd.extend((0..self.width - 1).map(|bit| 1u64 << bit));
        let even = square(&odd);
        let mut odd = square(&even);
        // Applies `len` zero bytes to `first`, a squaring per bit of it.
        loop {
            let even = square(&odd);
            if second_len & 1 != 0 {
                first = times(&even, first);
            }
            second_len >>= 1;
            if second_len == 0 {
                break;
            }
            odd = square(&even);
            if second_len & 1 != 0 {
                first = times(&odd, first);
            }
            second_len >>= 1;
            if second_len == 0 {
                break;
            }
        }
        first ^ second
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combined_crcs_are_the_crcs_of_the_whole() {
        let crc32 = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);
        let crc32c = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);
        let crc64 = crc::Crc::<u64>::new(&crc::CRC_64_NVME);
        let data: Vec<u8> = (0..20_000u32).map(|n| (n * 7 + n / 13) as u8).collect();
        for split in [0, 1, 7, 4096, 9999, 19_999, 20_000] {
            let (a, b) = data.split_at(split);
            let len = b.len() as u64;
            assert_eq!(
                CRC32.combine(
                    u64::from(crc32.checksum(a)),
                    u64::from(crc32.checksum(b)),
                    len
                ),
                u64::from(crc32.checksum(&data)),
                "crc32 at {split}"
            );
            assert_eq!(
                CRC32C.combine(
                    u64::from(crc32c.checksum(a)),
                    u64::from(crc32c.checksum(b)),
                    len
                ),
                u64::from(crc32c.checksum(&data)),
                "crc32c at {split}"
            );
            assert_eq!(
                CRC64NVME.combine(crc64.checksum(a), crc64.checksum(b), len),
                crc64.checksum(&data),
                "crc64nvme at {split}"
            );
        }
        // Three parts, combined in order.
        let (a, rest) = data.split_at(5000);
        let (b, c) = rest.split_at(8000);
        let ab = CRC64NVME.combine(crc64.checksum(a), crc64.checksum(b), b.len() as u64);
        assert_eq!(
            CRC64NVME.combine(ab, crc64.checksum(c), c.len() as u64),
            crc64.checksum(&data)
        );
    }
}
