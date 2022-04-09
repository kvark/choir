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
use sharded_slab::Slab;
use std::{
    mem,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

const MAX_SINGERS: u32 = mem::size_of::<usize>() as u32 * 8;

#[doc(hidden)]
#[derive(Debug)]
pub struct Blocker {
    #[allow(dead_code)]
    id: usize,
}

#[doc(hidden)]
pub enum Continuation {
    Playing { dependents: Vec<Arc<Blocker>> },
    Done,
}

struct Task {
    song: Box<dyn FnOnce() + Send + 'static>,
    continuation: Arc<Mutex<Continuation>>,
}

struct Singer {
    name: String,
    alive: AtomicBool,
}

struct Dossier {
    _singer: Arc<Singer>,
    thread: thread::Thread,
}

struct Shared {
    injector: Injector<Task>,
    dossiers: Slab<Dossier>,
    parked_mask: AtomicUsize,
}

impl Shared {
    fn execute(&self, task: Task) {
        self.injector.push(task);
        // Wake up the first sleeping thread.
        let mask = self.parked_mask.load(Ordering::Relaxed);
        let index = mask.trailing_zeros();
        if index != MAX_SINGERS {
            let dossier = self.dossiers.get(index as usize).unwrap();
            dossier.thread.unpark();
        }
    }

    fn work_loop(&self, singer: Arc<Singer>) {
        let index = self
            .dossiers
            .insert(Dossier {
                _singer: Arc::clone(&singer),
                thread: thread::current(),
            })
            .unwrap();
        while singer.alive.load(Ordering::Acquire) {
            match self.injector.steal() {
                Steal::Empty => {
                    let mask = 1 << index;
                    self.parked_mask.fetch_or(mask, Ordering::Relaxed);
                    thread::park();
                    self.parked_mask.fetch_and(!mask, Ordering::Relaxed);
                }
                Steal::Success(task) => {
                    (task.song)();
                    let dependents = match mem::replace(
                        &mut *task.continuation.lock().unwrap(),
                        Continuation::Done,
                    ) {
                        Continuation::Playing { dependents } => dependents,
                        Continuation::Done => unreachable!(),
                    };
                    for blocker in dependents {
                        if let Ok(_ready) = Arc::try_unwrap(blocker) {
                            //TODO: unblock the dependent task?
                        }
                    }
                }
                Steal::Retry => {}
            }
        }
        log::info!("Terminating singer {}", singer.name);
    }
}

pub struct Choir {
    shared: Arc<Shared>,
    next_id: AtomicUsize,
}

pub struct SingerHandle {
    singer: Arc<Singer>,
    join_handle: Option<thread::JoinHandle<()>>,
}

pub struct Song {
    shared: Arc<Shared>,
    task: Task,
    blocker: Arc<Blocker>,
}

impl AsRef<Mutex<Continuation>> for Song {
    fn as_ref(&self) -> &Mutex<Continuation> {
        &self.task.continuation
    }
}

pub enum PlayingSong {
    Waiting(Song),
    Active {
        continuation: Arc<Mutex<Continuation>>,
    },
}

impl AsRef<Mutex<Continuation>> for PlayingSong {
    fn as_ref(&self) -> &Mutex<Continuation> {
        match *self {
            Self::Waiting(ref song) => &song.task.continuation,
            Self::Active { ref continuation } => continuation,
        }
    }
}

impl Choir {
    pub fn new() -> Self {
        let injector = Injector::new();
        Self {
            shared: Arc::new(Shared {
                injector,
                dossiers: Slab::new(),
                parked_mask: AtomicUsize::new(0),
            }),
            next_id: AtomicUsize::new(1),
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
            .spawn(move || shared.work_loop(singer_clone))
            .unwrap();

        SingerHandle {
            singer,
            join_handle: Some(join_handle),
        }
    }

    pub fn sing_later(&self, song: impl FnOnce() + Send + 'static) -> Song {
        Song {
            shared: Arc::clone(&self.shared),
            task: Task {
                song: Box::new(song),
                continuation: Arc::new(Mutex::new(Continuation::Playing {
                    dependents: Vec::new(),
                })),
            },
            blocker: Arc::new(Blocker {
                id: self.next_id.fetch_add(1, Ordering::AcqRel),
            }),
        }
    }

    pub fn sing_now(&self, song: impl FnOnce() + Send + 'static) -> PlayingSong {
        self.sing_later(song).sing()
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
        let _ = self.join_handle.take().unwrap().join();
    }
}

impl Song {
    pub fn sing(self) -> PlayingSong {
        if Arc::strong_count(&self.blocker) == 1 {
            let continuation = Arc::clone(&self.task.continuation);
            self.shared.execute(self.task);
            PlayingSong::Active { continuation }
        } else {
            PlayingSong::Waiting(self)
        }
    }

    pub fn depend_on<C: AsRef<Mutex<Continuation>>>(&self, other: C) {
        match *other.as_ref().lock().unwrap() {
            Continuation::Playing { ref mut dependents } => {
                dependents.push(Arc::clone(&self.blocker));
            }
            Continuation::Done => {}
        }
    }
}
