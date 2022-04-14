/*! Task Orchestration Framework.

This framework helps to organize the execution of a program
into a live task graph. In this model, all the work is happening
inside tasks, which are scheduled to run by the `Choir`.

Lifetime of a Task:
  1. Idle: task is just created.
  2. Scheduled: no more dependencies can be added to the task.
  3. Executing: task was dispatched from the queue by one of the workers.
  4. Done: task is retired.
!*/

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
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_qualifications,
    clippy::pattern_type_mismatch
)]
//#![forbid(unsafe_code)]

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
const NO_WORKER: Option<WorkerContext> = None;

#[doc(hidden)]
pub enum Continuation {
    Playing {
        dependents: Vec<Arc<Task>>,
        baton: Weak<()>,
    },
    Done,
}

struct FnBox(Box<dyn FnOnce() + Send + 'static>);

// This is totally safe. See:
// https://internals.rust-lang.org/t/dyn-fnonce-should-always-be-sync/16470
unsafe impl Sync for FnBox {}

#[doc(hidden)]
pub struct Task {
    id: usize,
    fun: FnBox,
    continuation: Arc<Mutex<Continuation>>,
}

struct Worker {
    name: String,
    alive: AtomicBool,
}

struct WorkerContext {
    thread: thread::Thread,
}

struct WorkerPool {
    contexts: [Option<WorkerContext>; MAX_WORKERS],
}

struct Conductor {
    injector: Injector<Task>,
    //TODO: use a concurrent growable vector here
    workers: RwLock<WorkerPool>,
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
            let pool = self.workers.read().unwrap();
            if let Some(context) = pool.contexts[index].as_ref() {
                context.thread.unpark();
            }
        }
    }

    fn work_loop(&self, worker: &Worker) {
        profiling::register_thread!();
        let index = {
            let mut pool = self.workers.write().unwrap();
            let index = pool.contexts.iter_mut().position(|c| c.is_none()).unwrap();
            pool.contexts[index] = Some(WorkerContext {
                thread: thread::current(),
            });
            index
        };
        log::info!("Thread[{}] = '{}' started", index, worker.name);

        while worker.alive.load(Ordering::Acquire) {
            match self.injector.steal() {
                Steal::Empty => {
                    log::trace!("Thread '{}' sleeps", worker.name);
                    profiling::scope!("park");
                    let mask = 1 << index;
                    self.parked_mask.fetch_or(mask, Ordering::Relaxed);
                    thread::park();
                    self.parked_mask.fetch_and(!mask, Ordering::Relaxed);
                }
                Steal::Success(task) => {
                    log::debug!("Task {} runs on thread '{}'", task.id, worker.name);
                    // execute the task
                    {
                        profiling::scope!("execute");
                        (task.fun.0)();
                    }
                    profiling::scope!("unblock");
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
        self.workers.write().unwrap().contexts[index] = None;
    }
}

/// Main structure for managing tasks.
pub struct Choir {
    conductor: Arc<Conductor>,
    next_id: AtomicUsize,
}

/// Handle object holding a worker thread alive.
pub struct WorkerHandle {
    worker: Arc<Worker>,
    join_handle: Option<thread::JoinHandle<()>>,
}

/// Task that is created but not running yet.
pub struct IdleTask {
    conductor: Arc<Conductor>,
    task: Arc<Task>,
}

impl AsRef<Mutex<Continuation>> for IdleTask {
    fn as_ref(&self) -> &Mutex<Continuation> {
        &self.task.continuation
    }
}

/// Task that is already scheduled for running.
pub struct RunningTask {
    continuation: Arc<Mutex<Continuation>>,
}

impl AsRef<Mutex<Continuation>> for RunningTask {
    fn as_ref(&self) -> &Mutex<Continuation> {
        &self.continuation
    }
}

impl Choir {
    /// Create a new task system.
    pub fn new() -> Self {
        let injector = Injector::new();
        Self {
            conductor: Arc::new(Conductor {
                injector,
                workers: RwLock::new(WorkerPool {
                    contexts: [NO_WORKER; MAX_WORKERS],
                }),
                parked_mask: AtomicUsize::new(0),
                baton: Arc::new(()),
            }),
            next_id: AtomicUsize::new(1),
        }
    }

    /// Add a new worker thread.
    ///
    /// Note: A system can't have more than `MAX_WORKERS` workers
    /// enabled at any time.
    pub fn add_worker(&mut self, name: &str) -> WorkerHandle {
        let worker = Arc::new(Worker {
            name: name.to_string(),
            alive: AtomicBool::new(true),
        });
        let conductor = Arc::clone(&self.conductor);
        let worker_clone = Arc::clone(&worker);

        let join_handle = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || conductor.work_loop(&worker_clone))
            .unwrap();

        WorkerHandle {
            worker,
            join_handle: Some(join_handle),
        }
    }

    /// Internal method to create task data.
    fn create_task(&self, fun: impl FnOnce() + Send + 'static) -> Task {
        Task {
            id: self.next_id.fetch_add(1, Ordering::AcqRel),
            fun: FnBox(Box::new(fun)),
            continuation: Arc::new(Mutex::new(Continuation::Playing {
                dependents: Vec::new(),
                baton: Arc::downgrade(&self.conductor.baton),
            })),
        }
    }

    /// Create a task without scheduling it for running. This is used in order
    /// to add dependencies before running the task.
    pub fn idle_task(&self, fun: impl FnOnce() + Send + 'static) -> IdleTask {
        IdleTask {
            conductor: Arc::clone(&self.conductor),
            task: Arc::new(self.create_task(fun)),
        }
    }

    /// Create a task without dependencies and run it instantly.
    #[profiling::function]
    pub fn run_task(&self, fun: impl FnOnce() + Send + 'static) -> RunningTask {
        const FALLBACK: bool = false;
        if FALLBACK {
            // this path has roughly 50% more overhead
            self.idle_task(fun).run()
        } else {
            // fast path skips the making of `Arc<Task>`
            let task = self.create_task(fun);
            let continuation = Arc::clone(&task.continuation);
            self.conductor.schedule(task);
            RunningTask { continuation }
        }
    }

    /// Block until the running queue is empty.
    #[profiling::function]
    pub fn wait_idle(&mut self) {
        while !self.conductor.injector.is_empty() || Arc::weak_count(&self.conductor.baton) != 0 {
            //TODO: is there a better way?
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
    /// Schedule this task for running.
    ///
    /// It will only be executed once the dependencies are fulfilled.
    pub fn run(self) -> RunningTask {
        let continuation = Arc::clone(&self.task.continuation);
        if let Ok(ready) = Arc::try_unwrap(self.task) {
            self.conductor.schedule(ready);
        }
        RunningTask { continuation }
    }

    /// Add a dependency on another task, which is possibly running.
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
