use crate::Result;
use anyhow::anyhow;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use std::{thread, time};

const SHIFT: u64 = 20;
const MASK: u64 = (1 << SHIFT) - 1;

#[derive(Clone)]
pub struct IdGenerator {
    data: Arc<Mutex<IdGeneratorData>>,
}

impl IdGenerator {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(IdGeneratorData { time: 0, cur: 0 })),
        }
    }

    pub fn generate(&self) -> Result<u64> {
        let mut range = self.generate_n(1)?;
        Ok(range.next().unwrap())
    }

    pub fn generate_n(&self, n: usize) -> Result<IdRange> {
        let n = n as u64;
        if n >= MASK {
            return Err(anyhow!("Cannot generate so many id"));
        }

        loop {
            let mut guard = self.data.lock().unwrap();

            let now = SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            if now != guard.time {
                guard.time = now;
                guard.cur = 0;
            }

            if guard.cur + n >= MASK {
                drop(guard);
                thread::yield_now();
                continue;
            }

            let start = guard.cur;
            guard.cur += n;
            let end = guard.cur;

            return Ok(IdRange::new(now, start, end))
        }
    }
}

struct IdGeneratorData {
    time: u64,
    cur: u64,
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
    use crate::server::id::IdGenerator;

    #[test]
    fn test_generate() {
        let g = IdGenerator::new();
        assert!(g.generate().unwrap() > 0);

        let mut range = g.generate_n(10).unwrap();
        assert_eq!(range.end - range.start, 10);

        for i in 0..10 {
            let opt = range.next();
            assert!(opt.is_some());
            println!("{:?}", opt);
        }
        assert!(range.next().is_none());
    }
}
