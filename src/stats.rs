//! Accumulators and the end-of-run report.
//!
//! Two things are worth understanding before reading the numbers this produces:
//!
//! * **Round-trip time is trustworthy.** `rtt = (t3 - t0) - (t2 - t1)` subtracts
//!   the responder's own dwell time, and both halves of each subtraction come
//!   from a single clock, so the offset between the two hosts cancels exactly.
//! * **One-way delay is not.** `t1 - t0` crosses clocks, so it carries whatever
//!   your two machines disagree by. It is reported anyway because its
//!   *variation* is still meaningful: skew is constant over a short run, so it
//!   drops out of jitter and out of the spread between percentiles.

use std::collections::HashSet;
use std::fmt::Write as _;

/// A pile of microsecond samples we can take percentiles over.
#[derive(Default)]
pub struct Latency {
    samples: Vec<u64>,
}

impl Latency {
    pub fn push(&mut self, us: u64) {
        self.samples.push(us);
    }

    pub fn count(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    pub fn mean_ms(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let sum: u128 = self.samples.iter().map(|&v| v as u128).sum();
        (sum as f64 / self.samples.len() as f64) / 1000.0
    }

    /// Nearest-rank percentile, in milliseconds. `p` is 0.0..=1.0.
    pub fn pct_ms(&self, p: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        let rank = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
        sorted[rank - 1] as f64 / 1000.0
    }

    pub fn min_ms(&self) -> f64 {
        self.samples.iter().min().copied().unwrap_or(0) as f64 / 1000.0
    }

    pub fn max_ms(&self) -> f64 {
        self.samples.iter().max().copied().unwrap_or(0) as f64 / 1000.0
    }

    /// One line of the distribution, for the report.
    pub fn line(&self, label: &str) -> String {
        if self.is_empty() {
            return format!("  {label:<26} (no samples)");
        }
        format!(
            "  {:<26} min {:>8.1}  p50 {:>8.1}  p90 {:>8.1}  p99 {:>8.1}  max {:>9.1}  mean {:>8.1}",
            label,
            self.min_ms(),
            self.pct_ms(0.50),
            self.pct_ms(0.90),
            self.pct_ms(0.99),
            self.max_ms(),
            self.mean_ms(),
        )
    }
}

/// Interarrival jitter, smoothed the way RFC 3550 §6.4.1 defines it for RTP.
///
/// Fed the *transit* value (arrival clock minus departure clock), which may be
/// wildly wrong in absolute terms across two unsynced hosts. Only successive
/// differences are used, so the constant offset cancels and the result is a
/// real measurement even without a shared clock.
#[derive(Default)]
pub struct Jitter {
    last_transit: Option<i64>,
    value: f64,
}

impl Jitter {
    pub fn update(&mut self, transit_us: i64) {
        if let Some(prev) = self.last_transit {
            let d = (transit_us - prev).abs() as f64;
            self.value += (d - self.value) / 16.0;
        }
        self.last_transit = Some(transit_us);
    }

    pub fn ms(&self) -> f64 {
        self.value / 1000.0
    }
}

/// Sequence-number bookkeeping: what arrived, what didn't, what arrived twice
/// or out of order.
#[derive(Default)]
pub struct Arrivals {
    seen: HashSet<u64>,
    lowest: Option<u64>,
    highest: Option<u64>,
    pub received: u64,
    pub duplicates: u64,
    pub reordered: u64,
}

impl Arrivals {
    /// Returns false if this sequence number has been seen before.
    pub fn record(&mut self, seq: u64) -> bool {
        self.received += 1;
        if !self.seen.insert(seq) {
            self.duplicates += 1;
            return false;
        }
        self.lowest = Some(self.lowest.map_or(seq, |l| l.min(seq)));
        match self.highest {
            Some(h) if seq < h => self.reordered += 1,
            _ => self.highest = Some(seq),
        }
        true
    }

    pub fn unique(&self) -> u64 {
        self.seen.len() as u64
    }

    /// Loss inferred from the sequence range actually observed. Only an
    /// estimate: anything lost past the last packet to arrive is invisible.
    pub fn inferred_loss(&self) -> (u64, f64) {
        let (Some(lo), Some(hi)) = (self.lowest, self.highest) else {
            return (0, 0.0);
        };
        let span = hi - lo + 1;
        let lost = span.saturating_sub(self.unique());
        (lost, if span == 0 { 0.0 } else { lost as f64 * 100.0 / span as f64 })
    }
}

/// Something the network did to our routes while we were measuring.
pub struct RouteEvent {
    pub at_secs: f64,
    pub what: String,
}

/// Everything one run produced.
#[derive(Default)]
pub struct Run {
    pub sent: u64,
    /// How many probes the pacing loop should have fired in the elapsed time.
    /// A shortfall against `sent` means the node could not keep up with `--rate`.
    pub scheduled: u64,
    pub send_errors: u64,
    pub send_dropped: u64,
    pub rtt: Latency,
    pub forward: Latency,
    pub reverse: Latency,
    pub forward_jitter: Jitter,
    pub reverse_jitter: Jitter,
    pub arrivals: Arrivals,
    pub route_events: Vec<RouteEvent>,
    /// Seconds from route creation to the first death of a route we were using.
    pub first_route_death: Option<f64>,
}

impl Run {
    pub fn note_route_event(&mut self, at_secs: f64, what: impl Into<String>) {
        self.route_events.push(RouteEvent { at_secs, what: what.into() });
    }

    /// Live one-liner, printed once a second so a long run is watchable.
    pub fn tick_line(&self, elapsed: f64) -> String {
        let loss = if self.sent == 0 {
            0.0
        } else {
            (self.sent.saturating_sub(self.rtt.count() as u64)) as f64 * 100.0 / self.sent as f64
        };
        format!(
            "[{elapsed:>6.1}s] sent {:>6}  echo {:>6}  loss {:>5.1}%  rtt p50 {:>7.1}ms  p90 {:>7.1}ms  jit {:>6.1}ms",
            self.sent,
            self.rtt.count(),
            loss,
            self.rtt.pct_ms(0.50),
            self.rtt.pct_ms(0.90),
            self.forward_jitter.ms(),
        )
    }

    pub fn prober_report(&self, params: &str, elapsed: f64) -> String {
        let mut s = String::new();
        let echoed = self.rtt.count() as u64;
        let unanswered = self.sent.saturating_sub(echoed);
        let loss_pct = if self.sent == 0 {
            0.0
        } else {
            unanswered as f64 * 100.0 / self.sent as f64
        };

        let _ = writeln!(s, "\n========== veilid-vc probe report ==========");
        let _ = writeln!(s, "{params}");
        let _ = writeln!(s, "  duration                   {elapsed:.1}s");
        let _ = writeln!(s);
        let _ = writeln!(s, "-- delivery --");
        if self.scheduled > self.sent {
            let _ = writeln!(
                s,
                "  probes scheduled           {}  ({} never left; the node could not hold the rate)",
                self.scheduled,
                self.scheduled - self.sent
            );
        }
        let _ = writeln!(s, "  probes sent                {}", self.sent);
        let _ = writeln!(s, "  echoes received            {echoed}");
        let _ = writeln!(s, "  unanswered                 {unanswered}  ({loss_pct:.1}%)");
        if self.send_errors > 0 {
            let _ = writeln!(s, "  send errors                {}", self.send_errors);
        }
        if self.send_dropped > 0 {
            let _ = writeln!(
                s,
                "  never sent (backpressure)  {}  <- the node could not keep up with --rate",
                self.send_dropped
            );
        }
        if self.arrivals.duplicates > 0 {
            let _ = writeln!(s, "  duplicate echoes           {}", self.arrivals.duplicates);
        }
        if self.arrivals.reordered > 0 {
            let _ = writeln!(
                s,
                "  echoes out of order        {}  ({:.1}%)",
                self.arrivals.reordered,
                self.arrivals.reordered as f64 * 100.0 / echoed.max(1) as f64
            );
        }
        let _ = writeln!(s);
        let _ = writeln!(s, "-- latency, milliseconds --");
        let _ = writeln!(s, "{}", self.rtt.line("round trip (clean)"));
        let _ = writeln!(s, "{}", self.forward.line("forward one-way (skewed)"));
        let _ = writeln!(s, "{}", self.reverse.line("reverse one-way (skewed)"));
        let _ = writeln!(s);
        let _ = writeln!(s, "  forward jitter             {:>8.1} ms", self.forward_jitter.ms());
        let _ = writeln!(s, "  reverse jitter             {:>8.1} ms", self.reverse_jitter.ms());
        let _ = writeln!(s);
        let _ = writeln!(
            s,
            "  One-way figures cross two unsynchronised clocks, so their absolute values\n  \
             include the offset between the machines. Their spread and the jitter figures\n  \
             do not. Round trip is offset-free and is the number to design against."
        );
        s.push_str(&self.route_section());
        s.push_str(&verdict(&self.rtt, self.forward_jitter.ms(), loss_pct));
        s
    }

    pub fn responder_report(&self, params: &str, elapsed: f64) -> String {
        let mut s = String::new();
        let (lost, loss_pct) = self.arrivals.inferred_loss();

        let _ = writeln!(s, "\n========== veilid-vc responder report ==========");
        let _ = writeln!(s, "{params}");
        let _ = writeln!(s, "  duration                   {elapsed:.1}s");
        let _ = writeln!(s);
        let _ = writeln!(s, "-- forward path, as seen from this end --");
        let _ = writeln!(s, "  probes received            {}", self.arrivals.unique());
        let _ = writeln!(s, "  inferred lost              {lost}  ({loss_pct:.1}%)");
        let _ = writeln!(s, "  duplicates                 {}", self.arrivals.duplicates);
        let _ = writeln!(
            s,
            "  out of order               {}  ({:.1}%)",
            self.arrivals.reordered,
            self.arrivals.reordered as f64 * 100.0 / self.arrivals.unique().max(1) as f64
        );
        let _ = writeln!(s, "  echoes sent                {}", self.sent);
        if self.send_errors > 0 {
            let _ = writeln!(s, "  echo send errors           {}", self.send_errors);
        }
        let _ = writeln!(s);
        let _ = writeln!(s, "{}", self.forward.line("forward one-way (skewed)"));
        let _ = writeln!(s, "  forward jitter             {:>8.1} ms", self.forward_jitter.ms());
        let _ = writeln!(s);
        let _ = writeln!(
            s,
            "  Loss here is inferred from gaps in the sequence numbers that did arrive,\n  \
             so anything dropped after the last one to land is invisible. The prober's\n  \
             own count is exact; compare the two."
        );
        s.push_str(&self.route_section());
        s
    }

    fn route_section(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "\n-- routes --");
        match self.first_route_death {
            Some(t) => {
                let _ = writeln!(s, "  first route death at       {t:.1}s");
            }
            None => {
                let _ = writeln!(s, "  no route deaths");
            }
        }
        if self.route_events.is_empty() {
            let _ = writeln!(s, "  (no route churn during the run)");
        } else {
            for ev in &self.route_events {
                let _ = writeln!(s, "  {:>8.1}s  {}", ev.at_secs, ev.what);
            }
        }
        s
    }
}

/// Translate the distribution into the only question that matters for a call.
fn verdict(rtt: &Latency, jitter_ms: f64, loss_pct: f64) -> String {
    let mut s = String::from("\n-- what this means for a call --\n");
    if rtt.is_empty() {
        s.push_str("  Nothing came back. Check that both ends attached and that the blob is current.\n");
        return s;
    }
    // Conversational audio wants one-way mouth-to-ear under ~200ms end to end,
    // which leaves very little once a transport eats half a round trip.
    let owd_p90 = rtt.pct_ms(0.90) / 2.0;
    let headroom = 200.0 - owd_p90;
    let _ = writeln!(
        s,
        "  p90 one-way transport delay is about {owd_p90:.0} ms (half the p90 round trip)."
    );
    let _ = writeln!(
        s,
        "  Conversational audio budgets ~200 ms mouth to ear, so this leaves {headroom:.0} ms\n  \
         for capture, encode, jitter buffer, decode and playout."
    );
    if headroom < 0.0 {
        s.push_str("  That budget is already blown by the transport alone. Media over app_message\n  is not viable on this path; use Veilid for signalling and carry media elsewhere.\n");
    } else if headroom < 60.0 {
        s.push_str("  That is too little to build on. Everything else in the pipeline costs more\n  than that combined.\n");
    } else if jitter_ms > 60.0 || loss_pct > 10.0 {
        s.push_str("  The delay is survivable but the jitter and loss are not; a jitter buffer\n  deep enough to smooth this would spend the headroom you just measured.\n");
    } else {
        s.push_str("  This is within reach for audio. Re-run it at several times of day and on a\n  worse network before trusting it.\n");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_use_nearest_rank() {
        let mut l = Latency::default();
        for i in 1..=100u64 {
            l.push(i * 1000); // 1ms..100ms
        }
        assert_eq!(l.min_ms(), 1.0);
        assert_eq!(l.max_ms(), 100.0);
        assert_eq!(l.pct_ms(0.50), 50.0);
        assert_eq!(l.pct_ms(0.90), 90.0);
        assert_eq!(l.pct_ms(0.99), 99.0);
    }

    #[test]
    fn empty_latency_does_not_panic() {
        let l = Latency::default();
        assert_eq!(l.pct_ms(0.5), 0.0);
        assert_eq!(l.mean_ms(), 0.0);
        assert!(l.line("x").contains("no samples"));
    }

    #[test]
    fn jitter_ignores_a_constant_clock_offset() {
        // A fixed skew of one hour on top of a perfectly steady 100ms transit
        // must still read as zero jitter.
        let skew = 3_600_000_000i64;
        let mut j = Jitter::default();
        for _ in 0..50 {
            j.update(skew + 100_000);
        }
        assert!(j.ms() < 0.001, "jitter was {}", j.ms());
    }

    #[test]
    fn jitter_responds_to_variation() {
        let mut j = Jitter::default();
        for i in 0..200 {
            j.update(100_000 + if i % 2 == 0 { 0 } else { 40_000 });
        }
        assert!(j.ms() > 20.0, "jitter was {}", j.ms());
    }

    #[test]
    fn arrivals_track_loss_reorder_and_dupes() {
        let mut a = Arrivals::default();
        for seq in [0u64, 1, 2, 4, 3, 5, 5, 9] {
            a.record(seq);
        }
        assert_eq!(a.unique(), 7); // 0,1,2,3,4,5,9
        assert_eq!(a.duplicates, 1);
        assert_eq!(a.reordered, 1); // the 3 after the 4
        let (lost, pct) = a.inferred_loss();
        assert_eq!(lost, 3); // 6,7,8 missing from the span 0..=9
        assert!((pct - 30.0).abs() < 0.01);
    }

    #[test]
    fn inferred_loss_is_zero_with_nothing_received() {
        let a = Arrivals::default();
        assert_eq!(a.inferred_loss(), (0, 0.0));
    }

    #[test]
    fn verdict_handles_a_silent_run() {
        let out = verdict(&Latency::default(), 0.0, 100.0);
        assert!(out.contains("Nothing came back"));
    }
}
