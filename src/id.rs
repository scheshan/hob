use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time;
use std::time::SystemTime;

const SHIFT: u64 = 20;
const MASK: u64 = (1 << SHIFT) - 1;

#[derive(Clone)]
pub struct IdGenerator {
    id: Arc<AtomicU64>,
}

impl IdGenerator {
    pub fn new() -> Self {
        Self {
            id: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn generate(&self) -> u64 {
        let mut range = self.generate_n(1);
        range.next().unwrap()
    }

    pub fn generate_n(&self, n: usize) -> IdRange {
        let now = SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let start = self.id.fetch_add(n as u64, Ordering::Relaxed);
        let end = start + n as u64;
        IdRange::new(now as u64, start, end)
    }
}

pub struct IdRange {
    ts: u64,
    start: u64,
    end: u64,
    cur: u64,
}

impl IdRange {
    fn new(ts: u64, start: u64, end: u64) -> Self {
        Self {
            ts,
            start,
            end,
            cur: start,
        }
    }

    pub fn next(&mut self) -> Option<u64> {
        if self.cur == self.end {
            return None;
        }

        self.cur += 1;
        Some(self.ts << SHIFT | (self.cur & MASK))
    }
}

#[cfg(test)]
mod tests {
    use crate::id::IdGenerator;

    #[test]
    fn test_generate() {
        let g = IdGenerator::new();
        assert!(g.generate() > 0);

        let mut range = g.generate_n(10);
        assert_eq!(range.end - range.start, 10);

        for i in 0..10 {
            let opt = range.next();
            assert!(opt.is_some());
            println!("{:?}", opt);
        }
        assert!(range.next().is_none());
    }
}