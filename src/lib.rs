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
    mem,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
    thread,
    time::Duration,
};

const MAX_SINGERS: u32 = mem::size_of::<usize>() as u32 * 8;

#[doc(hidden)]
pub enum Continuation {
    Playing {
        dependents: Vec<Arc<Task>>,
        baton: Weak<()>,
    },
    Done,
}

#[doc(hidden)]
pub struct Task {
    id: usize,
    song: Box<dyn FnOnce() + Send + Sync + 'static>,
    continuation: Arc<Mutex<Continuation>>,
}

struct Singer {
    name: String,
    alive: AtomicBool,
}

struct SingerContext {
    _inner: Arc<Singer>,
    thread: thread::Thread,
}

struct Conductor {
    injector: Injector<Task>,
    //TODO: use a concurrent growable vector here
    singers: RwLock<Vec<SingerContext>>,
    parked_mask: AtomicUsize,
    // A counter of all the tasks that are alive.
    baton: Arc<()>,
}

impl Conductor {
    fn schedule(&self, task: Task) {
        log::trace!("Task {} is scheduled", task.id);
        self.injector.push(task);
        // Wake up threads until it's scheduled.
        while !self.injector.is_empty() {
            // Take the first sleeping thread.
            let mask = self.parked_mask.load(Ordering::Relaxed);
            let index = mask.trailing_zeros();
            if index == MAX_SINGERS {
                // everybody is busy, give up
                break;
            }
            let singers = self.singers.read().unwrap();
            singers[index as usize].thread.unpark();
        }
    }

    fn work_loop(&self, singer: Arc<Singer>) {
        let index = {
            let mut singers = self.singers.write().unwrap();
            let index = singers.len();
            singers.push(SingerContext {
                _inner: Arc::clone(&singer),
                thread: thread::current(),
            });
            index
        };
        log::info!("Thread[{}] = '{}' started", index, singer.name);

        while singer.alive.load(Ordering::Acquire) {
            match self.injector.steal() {
                Steal::Empty => {
                    log::trace!("Thread '{}' sleeps", singer.name);
                    let mask = 1 << index;
                    self.parked_mask.fetch_or(mask, Ordering::Relaxed);
                    thread::park();
                    self.parked_mask.fetch_and(!mask, Ordering::Relaxed);
                }
                Steal::Success(task) => {
                    log::trace!("Thread '{}' runs task {}", singer.name, task.id);
                    // execute the task
                    (task.song)();
                    // mark the task as done
                    let dependents = match mem::replace(
                        &mut *task.continuation.lock().unwrap(),
                        Continuation::Done,
                    ) {
                        //Note: it's fine at this point to drop `baton`:
                        // if it's the last task, we don't have any dependencies anyway.
                        // Otherwise, the dependencies will have batons of their own.
                        Continuation::Playing {
                            dependents,
                            baton: _,
                        } => dependents,
                        Continuation::Done => unreachable!(),
                    };
                    // unblock dependencies if needed
                    for task in dependents {
                        if let Ok(ready) = Arc::try_unwrap(task) {
                            self.schedule(ready);
                        }
                    }
                }
                Steal::Retry => {}
            }
        }
        log::info!("Thread '{}' dies", singer.name);
    }
}

pub struct Choir {
    conductor: Arc<Conductor>,
    next_id: AtomicUsize,
}

pub struct SingerHandle {
    singer: Arc<Singer>,
    join_handle: Option<thread::JoinHandle<()>>,
}

pub struct Song {
    conductor: Arc<Conductor>,
    task: Arc<Task>,
}

impl AsRef<Mutex<Continuation>> for Song {
    fn as_ref(&self) -> &Mutex<Continuation> {
        &self.task.continuation
    }
}

pub struct PlayingSong {
    continuation: Arc<Mutex<Continuation>>,
}

impl AsRef<Mutex<Continuation>> for PlayingSong {
    fn as_ref(&self) -> &Mutex<Continuation> {
        &self.continuation
    }
}

impl Choir {
    pub fn new() -> Self {
        let injector = Injector::new();
        Self {
            conductor: Arc::new(Conductor {
                injector,
                singers: RwLock::new(Vec::new()),
                parked_mask: AtomicUsize::new(0),
                baton: Arc::new(()),
            }),
            next_id: AtomicUsize::new(1),
        }
    }

    pub fn add_singer(&mut self, name: &str) -> SingerHandle {
        let singer = Arc::new(Singer {
            name: name.to_string(),
            alive: AtomicBool::new(true),
        });
        let conductor = Arc::clone(&self.conductor);
        let singer_clone = Arc::clone(&singer);

        let join_handle = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || conductor.work_loop(singer_clone))
            .unwrap();

        SingerHandle {
            singer,
            join_handle: Some(join_handle),
        }
    }

    fn create_task(&self, song: impl FnOnce() + Send + Sync + 'static) -> Task {
        Task {
            id: self.next_id.fetch_add(1, Ordering::AcqRel),
            song: Box::new(song),
            continuation: Arc::new(Mutex::new(Continuation::Playing {
                dependents: Vec::new(),
                baton: Arc::downgrade(&self.conductor.baton),
            })),
        }
    }

    pub fn sing_later(&self, song: impl FnOnce() + Send + Sync + 'static) -> Song {
        Song {
            conductor: Arc::clone(&self.conductor),
            task: Arc::new(self.create_task(song)),
        }
    }

    pub fn sing_now(&self, song: impl FnOnce() + Send + Sync + 'static) -> PlayingSong {
        const FALLBACK: bool = false;
        if FALLBACK {
            self.sing_later(song).sing()
        } else {
            let task = self.create_task(song);
            let continuation = Arc::clone(&task.continuation);
            self.conductor.schedule(task);
            PlayingSong { continuation }
        }
    }

    pub fn wait_idle(&mut self) {
        while !self.conductor.injector.is_empty() || Arc::weak_count(&self.conductor.baton) != 0 {
            thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for SingerHandle {
    fn drop(&mut self) {
        self.singer.alive.store(false, Ordering::Release);
        let handle = self.join_handle.take().unwrap();
        handle.thread().unpark();
        let _ = handle.join();
    }
}

impl Song {
    pub fn sing(self) -> PlayingSong {
        let continuation = Arc::clone(&self.task.continuation);
        if let Ok(ready) = Arc::try_unwrap(self.task) {
            self.conductor.schedule(ready);
        }
        PlayingSong { continuation }
    }

    pub fn depend_on<C: AsRef<Mutex<Continuation>>>(&self, other: C) {
        match *other.as_ref().lock().unwrap() {
            Continuation::Playing {
                ref mut dependents,
                baton: _,
            } => {
                dependents.push(Arc::clone(&self.task));
            }
            Continuation::Done => {}
        }
    }
}
