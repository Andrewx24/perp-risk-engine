//! Signed fixed-point decimal arithmetic.
//!
//! Money never touches `f64` inside the engine. Every price, quantity and
//! balance is an [`Fx`]: an `i64` scaled by 10^6. Products go through `i128`
//! so `price * qty` cannot overflow before it is rescaled, and the narrowing
//! back to `i64` is checked — an out-of-range result panics instead of
//! wrapping (release builds also keep `overflow-checks = true`).
//!
//! Rounding is explicit. [`Fx::mul_trunc`] truncates toward zero; margin
//! *requirements* use [`Fx::mul_ceil`] / [`Fx::bps_ceil`] so that rounding
//! always errs on the side of the house.

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};
use std::str::FromStr;

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fx(i64);

const SCALE_I128: i128 = Fx::SCALE as i128;

impl Fx {
    pub const DECIMALS: u32 = 6;
    pub const SCALE: i64 = 1_000_000;
    pub const ZERO: Fx = Fx(0);
    pub const ONE: Fx = Fx(Self::SCALE);

    #[inline]
    pub const fn from_raw(raw: i64) -> Self {
        Fx(raw)
    }

    #[inline]
    pub const fn raw(self) -> i64 {
        self.0
    }

    #[inline]
    pub const fn from_int(v: i64) -> Self {
        Fx(v * Self::SCALE)
    }

    /// For simulation and display only. Never used on a balance path.
    pub fn from_f64_lossy(v: f64) -> Self {
        Fx((v * Self::SCALE as f64).round() as i64)
    }

    pub fn to_f64(self) -> f64 {
        self.0 as f64 / Self::SCALE as f64
    }

    #[inline]
    pub const fn abs(self) -> Self {
        Fx(self.0.abs())
    }

    #[inline]
    pub const fn signum(self) -> i64 {
        self.0.signum()
    }

    #[inline]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub const fn is_positive(self) -> bool {
        self.0 > 0
    }

    #[inline]
    pub const fn is_negative(self) -> bool {
        self.0 < 0
    }

    #[inline]
    fn narrow(v: i128) -> Fx {
        Fx(i64::try_from(v).expect("Fx overflow"))
    }

    // Fast paths. `i128 / constant` compiles to a `__divti3` libcall, tens of
    // nanoseconds each, and margining a position takes four of them. When the
    // product fits in an `i64` — every realistic price * size does — divide
    // in 64 bits instead, where division by a constant becomes a multiply and
    // a shift. Results are bit-identical to the `i128` path; only the cost
    // differs, and the property tests compare the two.

    /// `self * rhs`, truncated toward zero.
    #[inline]
    pub fn mul_trunc(self, rhs: Fx) -> Fx {
        match self.0.checked_mul(rhs.0) {
            Some(p) => Fx(p / Self::SCALE),
            None => Self::narrow(self.0 as i128 * rhs.0 as i128 / SCALE_I128),
        }
    }

    /// `self * rhs`, rounded away from zero. Use for requirements.
    #[inline]
    pub fn mul_ceil(self, rhs: Fx) -> Fx {
        if let Some(p) = self.0.checked_mul(rhs.0) {
            let q = p / Self::SCALE;
            return Fx(if p % Self::SCALE != 0 {
                q + p.signum()
            } else {
                q
            });
        }
        let p = self.0 as i128 * rhs.0 as i128;
        let q = p / SCALE_I128;
        Self::narrow(if p % SCALE_I128 != 0 {
            q + p.signum()
        } else {
            q
        })
    }

    /// `self / rhs`, truncated toward zero. `None` on division by zero or
    /// if the quotient does not fit.
    #[inline]
    pub fn checked_div(self, rhs: Fx) -> Option<Fx> {
        if rhs.0 == 0 {
            return None;
        }
        if let Some(n) = self.0.checked_mul(Self::SCALE) {
            return n.checked_div(rhs.0).map(Fx);
        }
        i64::try_from(self.0 as i128 * SCALE_I128 / rhs.0 as i128)
            .ok()
            .map(Fx)
    }

    /// `self * bps / 10_000`, rounded away from zero.
    #[inline]
    pub fn bps_ceil(self, bps: i64) -> Fx {
        if let Some(p) = self.0.checked_mul(bps) {
            let q = p / 10_000;
            return Fx(if p % 10_000 != 0 { q + p.signum() } else { q });
        }
        let p = self.0 as i128 * bps as i128;
        let q = p / 10_000;
        Self::narrow(if p % 10_000 != 0 { q + p.signum() } else { q })
    }

    /// `self * bps / 10_000`, truncated toward zero.
    #[inline]
    pub fn bps(self, bps: i64) -> Fx {
        match self.0.checked_mul(bps) {
            Some(p) => Fx(p / 10_000),
            None => Self::narrow(self.0 as i128 * bps as i128 / 10_000),
        }
    }

    /// Reference implementations: always through `i128`. Used by tests to
    /// pin the fast paths to them.
    #[doc(hidden)]
    pub fn mul_trunc_wide(self, rhs: Fx) -> Fx {
        Self::narrow(self.0 as i128 * rhs.0 as i128 / SCALE_I128)
    }

    #[doc(hidden)]
    pub fn mul_ceil_wide(self, rhs: Fx) -> Fx {
        let p = self.0 as i128 * rhs.0 as i128;
        let q = p / SCALE_I128;
        Self::narrow(if p % SCALE_I128 != 0 {
            q + p.signum()
        } else {
            q
        })
    }

    #[inline]
    pub fn checked_add(self, rhs: Fx) -> Option<Fx> {
        self.0.checked_add(rhs.0).map(Fx)
    }

    #[inline]
    pub fn checked_sub(self, rhs: Fx) -> Option<Fx> {
        self.0.checked_sub(rhs.0).map(Fx)
    }

    /// Rounds toward zero to a multiple of `tick`.
    pub fn round_to(self, tick: Fx) -> Fx {
        if tick.0 <= 0 {
            return self;
        }
        Fx(self.0 / tick.0 * tick.0)
    }

    /// Signed distance from `reference` in basis points, truncated.
    pub fn bps_from(self, reference: Fx) -> Option<i64> {
        if reference.0 == 0 {
            return None;
        }
        Some(((self.0 as i128 - reference.0 as i128) * 10_000 / reference.0 as i128) as i64)
    }
}

impl Add for Fx {
    type Output = Fx;
    #[inline]
    fn add(self, rhs: Fx) -> Fx {
        Fx(self.0 + rhs.0)
    }
}

impl Sub for Fx {
    type Output = Fx;
    #[inline]
    fn sub(self, rhs: Fx) -> Fx {
        Fx(self.0 - rhs.0)
    }
}

impl Neg for Fx {
    type Output = Fx;
    #[inline]
    fn neg(self) -> Fx {
        Fx(-self.0)
    }
}

impl AddAssign for Fx {
    #[inline]
    fn add_assign(&mut self, rhs: Fx) {
        self.0 += rhs.0;
    }
}

impl SubAssign for Fx {
    #[inline]
    fn sub_assign(&mut self, rhs: Fx) {
        self.0 -= rhs.0;
    }
}

impl std::iter::Sum for Fx {
    fn sum<I: Iterator<Item = Fx>>(iter: I) -> Fx {
        iter.fold(Fx::ZERO, |a, b| a + b)
    }
}

impl fmt::Display for Fx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.0 < 0 { "-" } else { "" };
        let abs = self.0.unsigned_abs();
        let int = abs / Self::SCALE as u64;
        let frac = abs % Self::SCALE as u64;
        if frac == 0 {
            return write!(f, "{sign}{int}");
        }
        let digits = format!("{frac:06}");
        write!(f, "{sign}{int}.{}", digits.trim_end_matches('0'))
    }
}

impl fmt::Debug for Fx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseFxError {
    #[error("empty decimal")]
    Empty,
    #[error("invalid character in decimal")]
    InvalidChar,
    #[error("more than {} significant fractional digits", Fx::DECIMALS)]
    TooPrecise,
    #[error("decimal out of range")]
    Overflow,
}

impl FromStr for Fx {
    type Err = ParseFxError;

    /// Exact decimal parsing. Venue feeds send prices as strings precisely so
    /// that nobody routes them through a float; neither do we. Extra
    /// fractional digits are accepted only when they are zeros
    /// (`"0.00100000"`), never silently rounded away.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let (neg, body) = match s.as_bytes().first() {
            None => return Err(ParseFxError::Empty),
            Some(b'-') => (true, &s[1..]),
            Some(b'+') => (false, &s[1..]),
            _ => (false, s),
        };
        let (int, frac) = body.split_once('.').unwrap_or((body, ""));
        if int.is_empty() && frac.is_empty() {
            return Err(ParseFxError::Empty);
        }
        let mut raw: i128 = 0;
        for b in int.bytes() {
            if !b.is_ascii_digit() {
                return Err(ParseFxError::InvalidChar);
            }
            raw = raw * 10 + (b - b'0') as i128;
            if raw > i64::MAX as i128 {
                return Err(ParseFxError::Overflow);
            }
        }
        let mut frac_raw: i128 = 0;
        for (i, b) in frac.bytes().enumerate() {
            if !b.is_ascii_digit() {
                return Err(ParseFxError::InvalidChar);
            }
            if i < Fx::DECIMALS as usize {
                frac_raw = frac_raw * 10 + (b - b'0') as i128;
            } else if b != b'0' {
                return Err(ParseFxError::TooPrecise);
            }
        }
        let shown = frac.len().min(Fx::DECIMALS as usize) as u32;
        frac_raw *= 10i128.pow(Fx::DECIMALS - shown);
        let total = raw * SCALE_I128 + frac_raw;
        let total = if neg { -total } else { total };
        i64::try_from(total)
            .map(Fx)
            .map_err(|_| ParseFxError::Overflow)
    }
}

impl Serialize for Fx {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Fx {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct FxVisitor;
        impl Visitor<'_> for FxVisitor {
            type Value = Fx;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a decimal string or an integer")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Fx, E> {
                v.parse().map_err(E::custom)
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Fx, E> {
                v.checked_mul(Fx::SCALE)
                    .map(Fx)
                    .ok_or_else(|| E::custom(ParseFxError::Overflow))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Fx, E> {
                i64::try_from(v)
                    .ok()
                    .and_then(|v| v.checked_mul(Fx::SCALE))
                    .map(Fx)
                    .ok_or_else(|| E::custom(ParseFxError::Overflow))
            }
        }
        d.deserialize_any(FxVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fx(s: &str) -> Fx {
        s.parse().unwrap()
    }

    #[test]
    fn parses_exactly() {
        assert_eq!(fx("101.5").raw(), 101_500_000);
        assert_eq!(fx("-0.000001").raw(), -1);
        assert_eq!(fx(".25").raw(), 250_000);
        assert_eq!(fx("0.00100000"), fx("0.001"));
        assert_eq!("0.0000001".parse::<Fx>(), Err(ParseFxError::TooPrecise));
        assert_eq!("1e5".parse::<Fx>(), Err(ParseFxError::InvalidChar));
        assert_eq!("".parse::<Fx>(), Err(ParseFxError::Empty));
        assert!("99999999999999999999".parse::<Fx>().is_err());
    }

    #[test]
    fn displays_round_trip() {
        for s in ["0", "1", "-1", "101.5", "0.000001", "-42.123456", "65000"] {
            assert_eq!(fx(s).to_string(), s);
        }
    }

    #[test]
    fn mul_does_not_overflow_at_realistic_scale() {
        // 1,000 BTC at $250,000: the raw product is ~2.5e20, past i64.
        let notional = fx("1000").mul_trunc(fx("250000"));
        assert_eq!(notional, fx("250000000"));
    }

    #[test]
    fn rounding_direction_is_explicit() {
        let a = fx("0.000001");
        let b = fx("0.5");
        assert_eq!(a.mul_trunc(b), Fx::ZERO);
        assert_eq!(a.mul_ceil(b), a);
        assert_eq!((-a).mul_ceil(b), -a);
        assert_eq!(fx("1").bps_ceil(1), fx("0.0001"));
        assert_eq!(fx("0.000001").bps_ceil(1), fx("0.000001"));
        assert_eq!(fx("0.000001").bps(1), Fx::ZERO);
    }

    #[test]
    #[should_panic(expected = "Fx overflow")]
    fn overflow_panics_rather_than_wraps() {
        let big = Fx::from_raw(i64::MAX / 2);
        let _ = big.mul_trunc(Fx::from_int(4));
    }

    #[test]
    fn serde_uses_strings() {
        let v = serde_json::to_string(&fx("1.25")).unwrap();
        assert_eq!(v, "\"1.25\"");
        assert_eq!(serde_json::from_str::<Fx>("\"1.25\"").unwrap(), fx("1.25"));
        assert_eq!(serde_json::from_str::<Fx>("3").unwrap(), fx("3"));
    }
}
