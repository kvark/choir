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
    clippy::unknown_clippy_lints,
    clippy::len_without_is_empty
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

/// Additional utilities.
pub mod util;

use crossbeam_deque::{Injector, Steal};
use std::{mem, ops};

#[cfg(feature = "loom")]
use loom::{sync, thread};
#[cfg(not(feature = "loom"))]
use std::{sync, thread};

use self::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex, RwLock,
};

const MAX_WORKERS: usize = mem::size_of::<usize>() * 8;

#[doc(hidden)]
pub enum Continuation {
    Playing { dependents: Vec<Arc<Task>> },
    Done,
}

/// Index of a sub-task inside a multi-task.
pub type SubIndex = u32;

enum Functor {
    Single(Box<dyn FnOnce() + Send + 'static>),
    Multi(
        ops::Range<SubIndex>,
        Arc<dyn Fn(SubIndex) + Send + Sync + 'static>,
    ),
}

// This is totally safe. See:
// https://internals.rust-lang.org/t/dyn-fnonce-should-always-be-sync/16470
unsafe impl Sync for Functor {}

// See https://github.com/tokio-rs/loom/issues/259
impl Functor {
    #[cfg(feature = "loom")]
    fn from_multi(count: SubIndex, fun: impl Fn(SubIndex) + Send + Sync + 'static) -> Self {
        Self::Multi(0..count, Arc::from_std(std::sync::Arc::from(fun)))
    }
    #[cfg(not(feature = "loom"))]
    fn from_multi(count: SubIndex, fun: impl Fn(SubIndex) + Send + Sync + 'static) -> Self {
        Self::Multi(0..count, Arc::new(fun))
    }
}

#[doc(hidden)]
pub struct Task {
    id: usize,
    functor: Functor,
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
    workers: RwLock<WorkerPool>,
    parked_mask: AtomicUsize,
    main_thread: thread::Thread,
}

impl Conductor {
    fn is_busy(&self) -> bool {
        profiling::scope!("is busy");
        if self.injector.is_empty() {
            let pool = self.workers.read().unwrap();
            let workers_mask = pool.contexts.iter().rev().fold(0usize, |mask, w| {
                (mask << 1) | if w.is_some() { 1 } else { 0 }
            });
            workers_mask != self.parked_mask.load(Ordering::Acquire)
        } else {
            true
        }
    }

    fn schedule(&self, task: Task) {
        log::trace!("Task {} is scheduled", task.id);
        self.injector.push(task);
        // Wake up a thread if there is a sleeping one.
        let mask = self.parked_mask.load(Ordering::Acquire);
        if mask != 0 {
            let index = mask.trailing_zeros() as usize;
            profiling::scope!("unpark");
            let pool = self.workers.read().unwrap();
            if let Some(context) = pool.contexts[index].as_ref() {
                context.thread.unpark();
            }
        }
    }

    fn execute(&self, task: Task, worker_index: usize) -> Option<Arc<Mutex<Continuation>>> {
        match task.functor {
            Functor::Single(fun) => {
                log::debug!("Task {} runs on thread[{}]", task.id, worker_index);
                profiling::scope!("execute");
                (fun)();
                Some(task.continuation)
            }
            Functor::Multi(mut sub_range, mut fun) => {
                log::debug!(
                    "Task {} {{{}}} runs on thread[{}]",
                    task.id,
                    sub_range.start,
                    worker_index,
                );
                debug_assert!(sub_range.start < sub_range.end);
                let middle = (sub_range.end + sub_range.start) >> 1;
                // split the task if needed
                if middle != sub_range.start {
                    let mask = self.parked_mask.load(Ordering::Acquire);
                    if mask != 0 {
                        profiling::scope!("branch");
                        self.injector.push(Task {
                            id: task.id,
                            functor: Functor::Multi(middle..sub_range.end, Arc::clone(&fun)),
                            continuation: Arc::clone(&task.continuation),
                        });
                        let index = mask.trailing_zeros() as usize;
                        log::trace!(
                            "\tsplit out {:?} for thread[{}]",
                            middle..sub_range.end,
                            index
                        );
                        sub_range.end = middle;
                        // wake up the worker
                        let pool = self.workers.read().unwrap();
                        if let Some(context) = pool.contexts[index].as_ref() {
                            context.thread.unpark();
                        }
                    }
                }
                // fun the functor
                {
                    profiling::scope!("execute");
                    (fun)(sub_range.start);
                }
                // are we done yet?
                sub_range.start += 1;
                if sub_range.start == sub_range.end {
                    // return the continuation if this is the last task in the set
                    Arc::get_mut(&mut fun).map(|_| task.continuation)
                } else {
                    // Put it back to the queue, with the next sub-index.
                    // Note: we aren't calling `schedule` because we know at least this very thread
                    // will be able to pick it up, so no need to wake up anybody.
                    self.injector.push(Task {
                        id: task.id,
                        functor: Functor::Multi(sub_range, fun),
                        continuation: task.continuation,
                    });
                    None
                }
            }
        }
    }

    fn finish(&self, continuation: &mut Continuation) {
        profiling::scope!("unblock");
        // mark the task as done
        let dependents = match mem::replace(continuation, Continuation::Done) {
            Continuation::Playing { dependents } => dependents,
            Continuation::Done => unreachable!(),
        };
        // unblock dependencies if needed
        for dependent in dependents {
            if let Ok(ready) = Arc::try_unwrap(dependent) {
                self.schedule(ready);
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
                    log::trace!("Thread[{}] sleeps", index);
                    let mask = 1 << index;
                    self.parked_mask.fetch_or(mask, Ordering::AcqRel);
                    //Note: this is a situation where we are about to sleep,
                    // and a new task is being scheduled at the same time.
                    if self.injector.is_empty() {
                        profiling::scope!("park");
                        self.main_thread.unpark();
                        thread::park();
                    } else {
                        log::trace!("\tno, queue is not empty");
                    }
                    self.parked_mask.fetch_and(!mask, Ordering::AcqRel);
                }
                Steal::Success(task) => {
                    if let Some(continuation) = self.execute(task, index) {
                        self.finish(&mut *continuation.lock().unwrap());
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

enum MaybeArc<T> {
    Unique(T),
    Shared(Arc<T>),
    Null,
}

impl<T> MaybeArc<T> {
    const NULL_ERROR: &'static str = "Value is gone!";

    fn new(value: T) -> Self {
        Self::Unique(value)
    }

    fn share(&mut self) -> Arc<T> {
        let arc = match mem::replace(self, Self::Null) {
            Self::Unique(value) => Arc::new(value),
            Self::Shared(arc) => arc,
            Self::Null => panic!("{}", Self::NULL_ERROR),
        };
        *self = Self::Shared(Arc::clone(&arc));
        arc
    }

    fn as_ref(&self) -> &T {
        match *self {
            Self::Unique(ref value) => value,
            Self::Shared(ref arc) => arc,
            Self::Null => panic!("{}", Self::NULL_ERROR),
        }
    }

    fn extract(&mut self) -> Option<T> {
        match mem::replace(self, Self::Null) {
            Self::Unique(value) => Some(value),
            _ => None,
        }
    }
}

/// Task that is created but not running yet.
/// It will be scheduled on `run()` or on drop.
pub struct IdleTask {
    conductor: Arc<Conductor>,
    task: MaybeArc<Task>,
}

impl AsRef<Mutex<Continuation>> for IdleTask {
    fn as_ref(&self) -> &Mutex<Continuation> {
        &self.task.as_ref().continuation
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
        const NO_WORKER: Option<WorkerContext> = None;
        let injector = Injector::new();
        Self {
            conductor: Arc::new(Conductor {
                injector,
                workers: RwLock::new(WorkerPool {
                    contexts: [NO_WORKER; MAX_WORKERS],
                }),
                parked_mask: AtomicUsize::new(0),
                main_thread: thread::current(),
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
    fn create_task(&self, functor: Functor) -> Task {
        let id = self.next_id.fetch_add(1, Ordering::AcqRel);
        log::trace!("Creating task {}", id);
        Task {
            id,
            functor,
            continuation: Arc::new(Mutex::new(Continuation::Playing {
                dependents: Vec::new(),
            })),
        }
    }

    /// Add a simple task.
    #[profiling::function]
    pub fn add_task(&self, fun: impl FnOnce() + Send + 'static) -> IdleTask {
        let task = self.create_task(Functor::Single(Box::new(fun)));
        IdleTask {
            conductor: Arc::clone(&self.conductor),
            task: MaybeArc::new(task),
        }
    }

    /// Add a task that's executed multiple times.
    #[profiling::function]
    pub fn add_multi_task(
        &self,
        count: SubIndex,
        fun: impl Fn(SubIndex) + Send + Sync + 'static,
    ) -> IdleTask {
        assert_ne!(count, 0);
        let task = self.create_task(Functor::from_multi(count, fun));
        log::trace!("\twith {} instances", count);
        IdleTask {
            conductor: Arc::clone(&self.conductor),
            task: MaybeArc::new(task),
        }
    }

    /// Add a task that's executed on a finite iterator of values.
    #[profiling::function]
    pub fn add_iter_task<I, F>(&self, iter: I, fun: F) -> IdleTask
    where
        I: Iterator,
        I::Item: Send + 'static,
        F: Fn(I::Item) + Send + Sync + 'static,
    {
        let task_data = iter.collect::<util::PerTaskData<_>>();
        self.add_multi_task(task_data.len(), move |index| unsafe {
            fun(task_data.take(index))
        })
    }

    /// Block until the running queue is empty.
    #[profiling::function]
    pub fn wait_idle(&self) {
        profiling::scope!("wait idle");
        assert_eq!(thread::current().id(), self.conductor.main_thread.id());
        while self.conductor.is_busy() {
            // will be woken up by workers finishing
            thread::park();
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
        let task = self.task.as_ref();
        RunningTask {
            continuation: Arc::clone(&task.continuation),
        }
    }

    /// Add a dependency on another task, which is possibly running.
    pub fn depend_on<C: AsRef<Mutex<Continuation>>>(&mut self, other: C) {
        match *other.as_ref().lock().unwrap() {
            Continuation::Playing { ref mut dependents } => {
                dependents.push(self.task.share());
            }
            Continuation::Done => {}
        }
    }
}

impl Drop for IdleTask {
    fn drop(&mut self) {
        if let Some(ready) = self.task.extract() {
            self.conductor.schedule(ready);
        }
    }
}
