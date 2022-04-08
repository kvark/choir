#![allow(
    renamed_and_removed_lints,
    clippy::new_without_default,
    clippy::unneeded_field_pattern,
    clippy::match_like_matches_macro,
    clippy::manual_strip,
    clippy::if_same_then_else,
    clippy::unknown_clippy_lints
)]
#![warn(
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_qualifications,
    clippy::pattern_type_mismatch
)]

use crossbeam_deque::{Injector, Steal};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

struct Task {
    song: Box<dyn FnOnce() + Send + 'static>,
}

struct Shared {
    injector: Injector<Task>,
}

struct Singer {
    name: String,
    alive: AtomicBool,
}

impl Singer {
    fn work_loop(&self, shared: Arc<Shared>) {
        while self.alive.load(Ordering::Acquire) {
            match shared.injector.steal() {
                Steal::Empty => thread::park(),
                Steal::Success(task) => {
                    (task.song)();
                }
                Steal::Retry => {}
            }
        }
        log::info!("Terminating singer {}", self.name);
    }
}

struct Dossier {
    singer: Arc<Singer>,
    join_handle: thread::JoinHandle<()>,
}

pub struct Choir {
    shared: Arc<Shared>,
    singers: Vec<Dossier>,
}

pub struct SingerHandle {
    singer: Arc<Singer>,
}

pub struct Song {}

impl Choir {
    pub fn new() -> Self {
        let injector = Injector::new();
        Self {
            shared: Arc::new(Shared { injector }),
            singers: Vec::new(),
        }
    }

    pub fn add_singer(&mut self, name: &str) -> SingerHandle {
        let singer = Arc::new(Singer {
            name: name.to_string(),
            alive: AtomicBool::new(true),
        });
        let shared = Arc::clone(&self.shared);
        let singer_clone = Arc::clone(&singer);

        let join_handle = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || singer_clone.work_loop(shared))
            .unwrap();

        self.singers.push(Dossier {
            singer: Arc::clone(&singer),
            join_handle,
        });

        SingerHandle { singer }
    }

    pub fn sing(&self, song: impl FnOnce() + Send + 'static) -> Song {
        self.shared.injector.push(Task {
            song: Box::new(song),
        });
        //TODO: unpark one instead of all
        for dossier in self.singers.iter() {
            dossier.join_handle.thread().unpark();
        }
        Song {}
    }

    pub fn wait_idle(&self) {
        while !self.shared.injector.is_empty() {
            thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for SingerHandle {
    fn drop(&mut self) {
        self.singer.alive.store(false, Ordering::Release);
    }
}

impl Drop for Choir {
    fn drop(&mut self) {
        for dossier in self.singers.iter() {
            dossier.singer.alive.store(false, Ordering::Release);
        }
    }
}
