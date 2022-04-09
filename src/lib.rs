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

const MAX_WORKERS: usize = mem::size_of::<usize>() * 8;

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
    fun: Box<dyn FnOnce() + Send + Sync + 'static>,
    continuation: Arc<Mutex<Continuation>>,
}

struct Worker {
    name: String,
    alive: AtomicBool,
}

struct WorkerContext {
    _inner: Arc<Worker>,
    thread: thread::Thread,
}

struct Conductor {
    injector: Injector<Task>,
    //TODO: use a concurrent growable vector here
    workers: RwLock<Vec<WorkerContext>>,
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
            let index = mask.trailing_zeros() as usize;
            if index == MAX_WORKERS {
                // everybody is busy, give up
                break;
            }
            let workers = self.workers.read().unwrap();
            workers[index].thread.unpark();
        }
    }

    fn work_loop(&self, worker: Arc<Worker>) {
        let index = {
            let mut workers = self.workers.write().unwrap();
            let index = workers.len();
            assert!(index < MAX_WORKERS);
            workers.push(WorkerContext {
                _inner: Arc::clone(&worker),
                thread: thread::current(),
            });
            index
        };
        log::info!("Thread[{}] = '{}' started", index, worker.name);

        while worker.alive.load(Ordering::Acquire) {
            match self.injector.steal() {
                Steal::Empty => {
                    log::trace!("Thread '{}' sleeps", worker.name);
                    let mask = 1 << index;
                    self.parked_mask.fetch_or(mask, Ordering::Relaxed);
                    thread::park();
                    self.parked_mask.fetch_and(!mask, Ordering::Relaxed);
                }
                Steal::Success(task) => {
                    log::debug!("Task {} runs on thread '{}'", task.id, worker.name);
                    // execute the task
                    (task.fun)();
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
        log::info!("Thread '{}' dies", worker.name);
    }
}

/// Main structure for managing tasks.
pub struct Choir {
    conductor: Arc<Conductor>,
    next_id: AtomicUsize,
}

pub struct WorkerHandle {
    worker: Arc<Worker>,
    join_handle: Option<thread::JoinHandle<()>>,
}

pub struct IdleTask {
    conductor: Arc<Conductor>,
    task: Arc<Task>,
}

impl AsRef<Mutex<Continuation>> for IdleTask {
    fn as_ref(&self) -> &Mutex<Continuation> {
        &self.task.continuation
    }
}

pub struct RunningTask {
    continuation: Arc<Mutex<Continuation>>,
}

impl AsRef<Mutex<Continuation>> for RunningTask {
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
                workers: RwLock::new(Vec::new()),
                parked_mask: AtomicUsize::new(0),
                baton: Arc::new(()),
            }),
            next_id: AtomicUsize::new(1),
        }
    }

    pub fn add_worker(&mut self, name: &str) -> WorkerHandle {
        let worker = Arc::new(Worker {
            name: name.to_string(),
            alive: AtomicBool::new(true),
        });
        let conductor = Arc::clone(&self.conductor);
        let worker_clone = Arc::clone(&worker);

        let join_handle = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || conductor.work_loop(worker_clone))
            .unwrap();

        WorkerHandle {
            worker,
            join_handle: Some(join_handle),
        }
    }

    //Note: `Sync` doesn't seem necessary here, but Rust complains otherwise.
    fn create_task(&self, fun: impl FnOnce() + Send + Sync + 'static) -> Task {
        Task {
            id: self.next_id.fetch_add(1, Ordering::AcqRel),
            fun: Box::new(fun),
            continuation: Arc::new(Mutex::new(Continuation::Playing {
                dependents: Vec::new(),
                baton: Arc::downgrade(&self.conductor.baton),
            })),
        }
    }

    pub fn idle_task(&self, fun: impl FnOnce() + Send + Sync + 'static) -> IdleTask {
        IdleTask {
            conductor: Arc::clone(&self.conductor),
            task: Arc::new(self.create_task(fun)),
        }
    }

    pub fn run_task(&self, fun: impl FnOnce() + Send + Sync + 'static) -> RunningTask {
        const FALLBACK: bool = false;
        if FALLBACK {
            self.idle_task(fun).run()
        } else {
            // fast path skips the making of `Arc<Task>`
            let task = self.create_task(fun);
            let continuation = Arc::clone(&task.continuation);
            self.conductor.schedule(task);
            RunningTask { continuation }
        }
    }

    pub fn wait_idle(&mut self) {
        while !self.conductor.injector.is_empty() || Arc::weak_count(&self.conductor.baton) != 0 {
            thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.worker.alive.store(false, Ordering::Release);
        let handle = self.join_handle.take().unwrap();
        handle.thread().unpark();
        let _ = handle.join();
    }
}

impl IdleTask {
    pub fn run(self) -> RunningTask {
        let continuation = Arc::clone(&self.task.continuation);
        if let Ok(ready) = Arc::try_unwrap(self.task) {
            self.conductor.schedule(ready);
        }
        RunningTask { continuation }
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

// Note: would be nice to log the dropping of `IdleTask`,
// but currently unable to implement `drop` because it's getting
// destructured in `IdleTask::run()`.
