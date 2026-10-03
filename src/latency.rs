//! Allocation-free log-linear latency histogram.
//!
//! Eight sub-buckets per power of two gives ≤12.5% relative error from 1ns to
//! u64::MAX in 496 counters. Recording is a `leading_zeros`, a shift and an
//! increment — cheap enough to time every command on the engine thread
//! without the measurement becoming the latency.

use serde::Serialize;

const SUB_BITS: u32 = 3;
const SUB: u64 = 1 << SUB_BITS;
const BUCKETS: usize = ((64 - SUB_BITS + 1) * SUB as u32) as usize;

#[derive(Clone)]
pub struct Histogram {
    counts: Box<[u64; BUCKETS]>,
    total: u64,
    max: u64,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Summary {
    pub count: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn index(v: u64) -> usize {
    if v < SUB {
        return v as usize;
    }
    let msb = 63 - v.leading_zeros();
    let shift = msb - SUB_BITS;
    let sub = (v >> shift) & (SUB - 1);
    ((msb - SUB_BITS + 1) as u64 * SUB + sub) as usize
}

#[inline]
fn lower_bound(i: usize) -> u64 {
    let i = i as u64;
    if i < SUB {
        return i;
    }
    let msb = i / SUB + SUB_BITS as u64 - 1;
    (SUB + i % SUB) << (msb - SUB_BITS as u64)
}

impl Histogram {
    pub fn new() -> Self {
        Histogram {
            counts: Box::new([0; BUCKETS]),
            total: 0,
            max: 0,
        }
    }

    #[inline]
    pub fn record(&mut self, ns: u64) {
        self.counts[index(ns)] += 1;
        self.total += 1;
        self.max = self.max.max(ns);
    }

    pub fn percentile(&self, q: f64) -> u64 {
        if self.total == 0 {
            return 0;
        }
        let target = ((self.total as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= target {
                return lower_bound(i).min(self.max);
            }
        }
        self.max
    }

    pub fn summary(&self) -> Summary {
        Summary {
            count: self.total,
            p50_ns: self.percentile(0.50),
            p99_ns: self.percentile(0.99),
            p999_ns: self.percentile(0.999),
            max_ns: self.max,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_monotone_and_bounded() {
        let mut prev = 0;
        for v in (0..100_000u64).chain([u64::MAX / 3, u64::MAX]) {
            let i = index(v);
            assert!(i < BUCKETS);
            assert!(i >= prev || v > 100_000);
            assert!(lower_bound(i) <= v);
            // Relative error of the bucket lower bound is at most 1/SUB.
            assert!(v - lower_bound(i) <= v / SUB);
            prev = i;
        }
    }

    #[test]
    fn percentiles_are_close() {
        let mut h = Histogram::new();
        for v in 1..=10_000 {
            h.record(v);
        }
        let p50 = h.percentile(0.5) as f64;
        let p99 = h.percentile(0.99) as f64;
        assert!((p50 - 5_000.0).abs() / 5_000.0 < 0.13, "{p50}");
        assert!((p99 - 9_900.0).abs() / 9_900.0 < 0.13, "{p99}");
        assert_eq!(h.summary().max_ns, 10_000);
    }
}
