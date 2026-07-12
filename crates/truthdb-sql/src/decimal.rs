//! Exact DECIMAL/NUMERIC values and arithmetic with SQL Server's result-type
//! rules (MS "Precision, scale, and length"). A value is an unscaled `i128`
//! plus a declared `precision`/`scale`; arithmetic derives the result type by
//! the documented formulas and rounds to it, erroring on overflow of the
//! integral part.

pub const MAX_PRECISION: u8 = 38;
/// Minimum scale a multiply/divide result is reduced to when the derived
/// precision exceeds 38 (SQL Server keeps at least 6 fractional digits).
const MIN_RESULT_SCALE: u8 = 6;

/// 10^38 - 1, the largest magnitude a DECIMAL(38, s) can hold.
const MAX_MAGNITUDE: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Decimal {
    pub value: i128,
    pub precision: u8,
    pub scale: u8,
}

/// Overflow of a DECIMAL's integral part (SQL Server error 8115).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DecimalOverflow;

fn pow10(n: u8) -> i128 {
    let mut p: i128 = 1;
    for _ in 0..n {
        p = p.saturating_mul(10);
    }
    p
}

// The arithmetic methods (add/sub/mul/div) intentionally mirror the operator
// names but return `Result` with SQL Server's derived result type, so they
// cannot be the std ops traits.
#[allow(clippy::should_implement_trait)]
impl Decimal {
    pub fn new(value: i128, precision: u8, scale: u8) -> Self {
        Decimal {
            value,
            precision,
            scale,
        }
    }

    pub fn from_i64(v: i64) -> Self {
        // Precision = digit count (min 1), scale 0.
        Decimal {
            value: v as i128,
            precision: digit_count(v as i128),
            scale: 0,
        }
    }

    pub fn to_f64(self) -> f64 {
        self.value as f64 / pow10(self.scale) as f64
    }

    pub fn is_zero(self) -> bool {
        self.value == 0
    }

    /// Rescales the value to `target` fractional digits, rounding half-away
    /// from zero when reducing scale. Returns None on i128 overflow.
    pub fn rescaled(self, target: u8) -> Option<i128> {
        if target >= self.scale {
            let factor = pow10(target - self.scale);
            self.value.checked_mul(factor)
        } else {
            let factor = pow10(self.scale - target);
            let half = factor / 2;
            let adjusted = if self.value >= 0 {
                self.value.checked_add(half)?
            } else {
                self.value.checked_sub(half)?
            };
            Some(adjusted / factor)
        }
    }

    /// Parses a decimal literal (`[-]digits[.digits]`), deriving precision and
    /// scale from the text as SQL Server does for numeric literals.
    pub fn parse(text: &str) -> Option<Decimal> {
        let text = text.trim();
        let (neg, body) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text.strip_prefix('+').unwrap_or(text)),
        };
        let (int_part, frac_part) = match body.split_once('.') {
            Some((i, f)) => (i, f),
            None => (body, ""),
        };
        if int_part.is_empty() && frac_part.is_empty() {
            return None;
        }
        if !int_part.chars().all(|c| c.is_ascii_digit())
            || !frac_part.chars().all(|c| c.is_ascii_digit())
        {
            return None;
        }
        let digits: String = format!("{int_part}{frac_part}");
        let value: i128 = if digits.is_empty() {
            0
        } else {
            digits.parse().ok()?
        };
        let value = if neg { -value } else { value };
        let scale = u8::try_from(frac_part.len()).ok()?;
        // Precision is the significant digit count (at least scale, at least 1).
        let precision = digit_count(value).max(scale).max(1);
        if precision > MAX_PRECISION {
            return None;
        }
        Some(Decimal {
            value,
            precision,
            scale,
        })
    }

    /// Renders as SQL Server would (fixed scale, no thousands separators).
    pub fn render(self) -> String {
        if self.scale == 0 {
            return self.value.to_string();
        }
        let neg = self.value < 0;
        let mag = self.value.unsigned_abs();
        let digits = mag.to_string();
        let scale = self.scale as usize;
        let padded = if digits.len() <= scale {
            format!("{:0>width$}", digits, width = scale + 1)
        } else {
            digits
        };
        let dot = padded.len() - scale;
        let sign = if neg { "-" } else { "" };
        format!("{sign}{}.{}", &padded[..dot], &padded[dot..])
    }

    /// Coerces to a target DECIMAL(precision, scale), rounding and checking the
    /// integral part fits (error 8115 otherwise).
    pub fn coerce(self, precision: u8, scale: u8) -> Result<Decimal, DecimalOverflow> {
        let value = self.rescaled(scale).ok_or(DecimalOverflow)?;
        let limit = pow10(precision).min(MAX_MAGNITUDE + 1);
        if value >= limit || value <= -limit {
            return Err(DecimalOverflow);
        }
        Ok(Decimal {
            value,
            precision,
            scale,
        })
    }

    pub fn add(self, other: Decimal) -> Result<Decimal, DecimalOverflow> {
        self.add_sub(other, false)
    }

    pub fn sub(self, other: Decimal) -> Result<Decimal, DecimalOverflow> {
        self.add_sub(other, true)
    }

    fn add_sub(self, other: Decimal, subtract: bool) -> Result<Decimal, DecimalOverflow> {
        // scale = max(s1, s2); precision = max(p1-s1, p2-s2) + scale + 1.
        let scale = self.scale.max(other.scale);
        let int_digits = (self.precision - self.scale).max(other.precision - other.scale);
        let (precision, scale) = cap(int_digits as u16 + scale as u16 + 1, scale);
        let a = self.rescaled(scale).ok_or(DecimalOverflow)?;
        let b = other.rescaled(scale).ok_or(DecimalOverflow)?;
        let raw = if subtract {
            a.checked_sub(b)
        } else {
            a.checked_add(b)
        }
        .ok_or(DecimalOverflow)?;
        Decimal::new(raw, precision, scale).coerce(precision, scale)
    }

    pub fn mul(self, other: Decimal) -> Result<Decimal, DecimalOverflow> {
        // precision = p1 + p2 + 1; scale = s1 + s2.
        let (precision, scale) = cap(
            self.precision as u16 + other.precision as u16 + 1,
            self.scale + other.scale,
        );
        let raw = self.value.checked_mul(other.value).ok_or(DecimalOverflow)?;
        // raw is at scale s1+s2; rescale to the (possibly reduced) result scale.
        let src = Decimal::new(raw, precision, self.scale + other.scale);
        let value = src.rescaled(scale).ok_or(DecimalOverflow)?;
        Decimal::new(value, precision, scale).coerce(precision, scale)
    }

    pub fn div(self, other: Decimal) -> Result<Option<Decimal>, DecimalOverflow> {
        if other.value == 0 {
            return Ok(None); // caller raises divide-by-zero
        }
        // scale = max(6, s1 + p2 + 1); precision = p1 - s1 + s2 + scale.
        let result_scale = MIN_RESULT_SCALE.max(self.scale + other.precision + 1);
        let raw_precision =
            self.precision as u16 - self.scale as u16 + other.scale as u16 + result_scale as u16;
        let (precision, scale) = cap(raw_precision, result_scale);
        // Compute quotient at `scale` fractional digits: numerator scaled up by
        // (scale + s2 - s1), divided by other.value, with round-half-away.
        let shift = scale as i32 + other.scale as i32 - self.scale as i32;
        let mut num = self.value;
        if shift >= 0 {
            num = num.checked_mul(pow10(shift as u8)).ok_or(DecimalOverflow)?;
        } else {
            num /= pow10((-shift) as u8);
        }
        let denom = other.value;
        let mut q = num / denom;
        let rem = num % denom;
        // Round half away from zero.
        if rem.unsigned_abs() * 2 >= denom.unsigned_abs() {
            q += if (num < 0) ^ (denom < 0) { -1 } else { 1 };
        }
        Ok(Some(
            Decimal::new(q, precision, scale).coerce(precision, scale)?,
        ))
    }

    /// Three-valued numeric comparison against another decimal.
    pub fn cmp(self, other: Decimal) -> std::cmp::Ordering {
        let scale = self.scale.max(other.scale);
        match (self.rescaled(scale), other.rescaled(scale)) {
            (Some(a), Some(b)) => a.cmp(&b),
            // Fall back to float on the (astronomically unlikely) i128 overflow.
            _ => self
                .to_f64()
                .partial_cmp(&other.to_f64())
                .unwrap_or(std::cmp::Ordering::Equal),
        }
    }
}

fn digit_count(value: i128) -> u8 {
    let mut n = value.unsigned_abs();
    if n == 0 {
        return 1;
    }
    let mut digits = 0u8;
    while n > 0 {
        n /= 10;
        digits += 1;
    }
    digits
}

/// Caps a derived (precision, scale) at SQL Server's 38-digit limit, reducing
/// scale (never below 6) to preserve integral digits.
fn cap(precision: u16, scale: u8) -> (u8, u8) {
    if precision <= MAX_PRECISION as u16 {
        return (precision as u8, scale);
    }
    let excess = precision - MAX_PRECISION as u16;
    let new_scale = (scale as u16)
        .saturating_sub(excess)
        .max(MIN_RESULT_SCALE as u16);
    (MAX_PRECISION, new_scale.min(scale as u16) as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        Decimal::parse(text).expect("parse")
    }

    #[test]
    fn parse_and_render_round_trip() {
        assert_eq!(dec("12.34").value, 1234);
        assert_eq!(dec("12.34").scale, 2);
        assert_eq!(dec("12.34").precision, 4);
        assert_eq!(dec("12.34").render(), "12.34");
        assert_eq!(dec("-0.05").render(), "-0.05");
        assert_eq!(dec("100").render(), "100");
        assert_eq!(dec("0.500").scale, 3);
    }

    #[test]
    fn addition_scale_and_precision() {
        // DECIMAL(4,2) + DECIMAL(4,2): scale 2, value exact.
        let r = dec("12.34").add(dec("1.11")).unwrap();
        assert_eq!(r.render(), "13.45");
        assert_eq!(r.scale, 2);
        // Differing scales align to the larger.
        let r = dec("1.5").add(dec("2.25")).unwrap();
        assert_eq!(r.render(), "3.75");
    }

    #[test]
    fn multiplication_scale_is_sum() {
        // scale(2)+scale(2) = 4.
        let r = dec("12.34").mul(dec("2.00")).unwrap();
        assert_eq!(r.scale, 4);
        assert_eq!(r.render(), "24.6800");
    }

    #[test]
    fn division_scale_matches_sql_server() {
        // 10.0 / 3.0: MS result scale = max(6, 1 + 2 + 1) = 6.
        let r = dec("10.0").div(dec("3.0")).unwrap().unwrap();
        assert_eq!(r.scale, 6);
        assert_eq!(r.render(), "3.333333");
        // Exact division still carries the derived scale.
        let r = dec("9").div(dec("2")).unwrap().unwrap();
        assert_eq!(r.render(), "4.500000");
    }

    #[test]
    fn division_by_zero_returns_none() {
        assert_eq!(dec("1.0").div(dec("0.0")).unwrap(), None);
    }

    #[test]
    fn coerce_rounds_half_away_from_zero() {
        assert_eq!(dec("2.345").coerce(5, 2).unwrap().render(), "2.35");
        assert_eq!(dec("-2.345").coerce(5, 2).unwrap().render(), "-2.35");
    }

    #[test]
    fn coerce_overflow_errors() {
        assert_eq!(dec("1000").coerce(3, 0), Err(DecimalOverflow));
    }

    #[test]
    fn comparison() {
        use std::cmp::Ordering;
        assert_eq!(dec("1.50").cmp(dec("1.5")), Ordering::Equal);
        assert_eq!(dec("1.5").cmp(dec("1.51")), Ordering::Less);
        assert_eq!(dec("-1").cmp(dec("1")), Ordering::Less);
    }
}
