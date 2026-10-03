//! Write-ahead command log.
//!
//! Commands — not events, not state — are what get persisted: the engine is
//! deterministic, so the log *is* the state. One JSON object per line keeps
//! it greppable; the engine task appends a whole batch, then `fsync`s once
//! (group commit) before acknowledging any command in it.

use crate::engine::Command;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;

pub struct Wal {
    writer: BufWriter<File>,
}

impl Wal {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Wal {
            writer: BufWriter::with_capacity(1 << 16, file),
        })
    }

    pub fn append(&mut self, cmd: &Command) -> io::Result<()> {
        serde_json::to_writer(&mut self.writer, cmd)?;
        self.writer.write_all(b"\n")
    }

    /// Flushes buffered commands and waits for them to reach the disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()
    }

    /// Reads every command in the log. A malformed *final* line is a write
    /// torn by a crash and is dropped (it was never acknowledged); a
    /// malformed line anywhere else is corruption and is an error.
    pub fn read_all(path: &Path) -> io::Result<Vec<Command>> {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let lines: Vec<String> = BufReader::new(file).lines().collect::<Result<_, _>>()?;
        let mut out = Vec::with_capacity(lines.len());
        for (i, line) in lines.iter().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str(line) {
                Ok(cmd) => out.push(cmd),
                Err(_) if i + 1 == lines.len() => break,
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("wal line {}: {e}", i + 1),
                    ));
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixed::Fx;
    use crate::types::AccountId;

    fn tmp(name: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("pre-wal-{}-{name}", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn round_trips_and_tolerates_a_torn_tail() {
        let p = tmp("torn");
        let cmd = Command::Deposit {
            account: AccountId(7),
            amount: Fx::from_int(5),
        };
        {
            let mut w = Wal::open(&p).unwrap();
            w.append(&cmd).unwrap();
            w.append(&cmd).unwrap();
            w.sync().unwrap();
        }
        std::fs::OpenOptions::new()
            .append(true)
            .open(&p)
            .unwrap()
            .write_all(b"{\"type\":\"depo")
            .unwrap();
        assert_eq!(Wal::read_all(&p).unwrap(), vec![cmd.clone(), cmd]);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn corruption_in_the_middle_is_an_error() {
        let p = tmp("corrupt");
        std::fs::write(
            &p,
            "garbage\n{\"type\":\"deposit\",\"account\":1,\"amount\":\"1\"}\n",
        )
        .unwrap();
        assert!(Wal::read_all(&p).is_err());
        let _ = std::fs::remove_file(&p);
    }
}
