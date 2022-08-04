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
use std::{
    fmt, mem, ops,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    thread, time,
};

const BITS_PER_BYTE: usize = 8;
const MAX_WORKERS: usize = mem::size_of::<usize>() * BITS_PER_BYTE;
const JOIN_PARK_TIME: time::Duration = time::Duration::from_millis(10);

#[derive(Debug)]
enum Continuation {
    Playing {
        dependents: Vec<Arc<Task>>,
        waiting_threads: Vec<thread::Thread>,
    },
    Done,
}

/// An object responsible to notify follow-up tasks.
#[derive(Debug)]
pub struct Notifier {
    continuation: Mutex<Continuation>,
}

impl Notifier {
    fn block(&self, task: Arc<Task>) {
        match *self.continuation.lock().unwrap() {
            Continuation::Playing {
                ref mut dependents,
                waiting_threads: _,
            } => {
                dependents.push(task);
            }
            Continuation::Done => {}
        }
    }
}

/// Index of a sub-task inside a multi-task.
pub type SubIndex = u32;

enum Functor {
    Dummy,
    Once(Box<dyn FnOnce(&Notifier) + Send + 'static>),
    Multi(
        ops::Range<SubIndex>,
        Arc<dyn Fn(&Notifier, SubIndex) + Send + Sync + 'static>,
    ),
}

impl fmt::Debug for Functor {
    fn fmt(&self, serializer: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::Dummy => serializer.debug_struct("Dummy").finish(),
            Self::Once(_) => serializer.debug_struct("Once").finish(),
            Self::Multi(ref range, _) => serializer
                .debug_struct("Multi")
                .field("range", range)
                .finish(),
        }
    }
}

// This is totally safe. See:
// https://internals.rust-lang.org/t/dyn-fnonce-should-always-be-sync/16470
unsafe impl Sync for Functor {}

#[derive(Debug)]
struct Task {
    id: usize,
    functor: Functor,
    /// This notifier is only really shared with `RunningTask`
    notifier: Arc<Notifier>,
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
}

impl Conductor {
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

    fn execute(&self, task: Task, worker_index: usize) -> Option<Arc<Notifier>> {
        match task.functor {
            Functor::Dummy => {
                log::debug!("Task {} (dummy) runs on thread[{}]", task.id, worker_index);
                Some(task.notifier)
            }
            Functor::Once(fun) => {
                log::debug!("Task {} runs on thread[{}]", task.id, worker_index);
                profiling::scope!("execute");
                (fun)(&task.notifier);
                Some(task.notifier)
            }
            Functor::Multi(mut sub_range, mut fun) => {
                log::debug!(
                    "Task {} ({}) runs on thread[{}]",
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
                        self.injector.push(Task {
                            id: task.id,
                            functor: Functor::Multi(middle..sub_range.end, Arc::clone(&fun)),
                            notifier: Arc::clone(&task.notifier),
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
                (fun)(&task.notifier, sub_range.start);
                // are we done yet?
                sub_range.start += 1;
                if sub_range.start == sub_range.end {
                    // return the notifier if this is the last task in the set
                    Arc::get_mut(&mut fun).map(|_| task.notifier)
                } else {
                    // Put it back to the queue, with the next sub-index.
                    // Note: we aren't calling `schedule` because we know at least this very thread
                    // will be able to pick it up, so no need to wake up anybody.
                    self.injector.push(Task {
                        id: task.id,
                        functor: Functor::Multi(sub_range, fun),
                        notifier: task.notifier,
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
            Continuation::Playing {
                dependents,
                waiting_threads,
            } => {
                for thread in waiting_threads {
                    thread.unpark();
                }
                dependents
            }
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
                        thread::park();
                    } else {
                        log::trace!("\tno, queue is not empty");
                    }
                    self.parked_mask.fetch_and(!mask, Ordering::AcqRel);
                }
                Steal::Success(task) => {
                    if let Some(notifier) = self.execute(task, index) {
                        self.finish(&mut *notifier.continuation.lock().unwrap());
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

/// Task construct without any functional logic.
pub struct ProtoTask<'c> {
    id: usize,
    #[allow(unused)]
    name: String,
    conductor: &'c Arc<Conductor>,
    notifier: Notifier,
}

/// Task that is created but not running yet.
/// It will be scheduled on `run()` or on drop.
pub struct IdleTask {
    conductor: Arc<Conductor>,
    task: MaybeArc<Task>,
}

/// Ability to block a task that hasn't started yet.
pub trait Dependency {
    /// Prevent the specified task from running until self is done.
    fn block(&self, idle: &mut IdleTask);
}

impl Dependency for ProtoTask<'_> {
    fn block(&self, idle: &mut IdleTask) {
        self.notifier.block(idle.task.share());
    }
}

impl Dependency for IdleTask {
    fn block(&self, idle: &mut IdleTask) {
        self.task.as_ref().notifier.block(idle.task.share());
    }
}

impl ProtoTask<'_> {
    fn fill(self, functor: Functor) -> IdleTask {
        IdleTask {
            conductor: Arc::clone(self.conductor),
            task: MaybeArc::new(Task {
                id: self.id,
                functor,
                notifier: Arc::new(self.notifier),
            }),
        }
    }

    /// Init task with no function.
    /// Can be useful to aggregate dependencies, for example
    /// if a function returns a task handle, and it launches
    /// multiple sub-tasks in parallel.
    pub fn init_dummy(self) -> IdleTask {
        self.fill(Functor::Dummy)
    }

    /// Init task to execute a standalone function.
    /// The function body will be executed once the task is scheduled,
    /// and all of its dependencies are fulfulled.
    pub fn init<F: FnOnce(&Notifier) + Send + 'static>(self, fun: F) -> IdleTask {
        self.fill(Functor::Once(Box::new(fun)))
    }

    /// Init task to execute a function multiple times.
    /// Every invocation is given an index in 0..count
    /// There are no ordering guarantees between the indices.
    pub fn init_multi<F: Fn(&Notifier, SubIndex) + Send + Sync + 'static>(
        self,
        count: SubIndex,
        fun: F,
    ) -> IdleTask {
        self.fill(if count == 0 {
            Functor::Dummy
        } else {
            Functor::Multi(0..count, Arc::new(fun))
        })
    }

    /// Init task to execute a function on each element of a finite iterator.
    /// Similarly to `init_multi`, each invocation is executed
    /// indepdently and can be out of order.
    pub fn init_iter<I, F>(self, iter: I, fun: F) -> IdleTask
    where
        I: Iterator,
        I::Item: Send + 'static,
        F: Fn(I::Item) + Send + Sync + 'static,
    {
        let task_data = iter.collect::<util::PerTaskData<_>>();
        self.init_multi(task_data.len(), move |_, index| unsafe {
            fun(task_data.take(index))
        })
    }
}

/// Task that is already scheduled for running.
#[derive(Clone, Debug)]
pub struct RunningTask {
    notifier: Arc<Notifier>,
}

impl Dependency for RunningTask {
    fn block(&self, idle: &mut IdleTask) {
        self.notifier.block(idle.task.share());
    }
}

impl RunningTask {
    /// Block until the task has finished executing.
    #[profiling::function]
    pub fn join(self) {
        match *self.notifier.continuation.lock().unwrap() {
            Continuation::Playing {
                dependents: _,
                ref mut waiting_threads,
            } => {
                waiting_threads.push(thread::current());
            }
            Continuation::Done => return,
        }
        loop {
            thread::park_timeout(JOIN_PARK_TIME);
            match *self.notifier.continuation.lock().unwrap() {
                Continuation::Playing { .. } => (),
                Continuation::Done => return,
            }
        }
    }
}

impl Default for Choir {
    fn default() -> Self {
        Self::new()
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

    /// Spawn a new task.
    #[profiling::function]
    pub fn spawn(&self, name: &str) -> ProtoTask {
        let id = self.next_id.fetch_add(1, Ordering::AcqRel);
        log::trace!("Creating task {}", id);

        ProtoTask {
            id,
            name: name.to_string(),
            conductor: &self.conductor,
            notifier: Notifier {
                continuation: Mutex::new(Continuation::Playing {
                    dependents: Vec::new(),
                    waiting_threads: Vec::new(),
                }),
            },
        }
    }

    /// Spawn a task that shares a given notifier.
    ///
    /// This is useful because it allows creating tasks on the fly from within
    /// other tasks. Generally, making a task in flight depend on anything is impossible.
    /// But one can create a proxy task from a dependency
    pub fn spawn_proxy(&self, name: &str, notifier: &Notifier) -> ProtoTask {
        let id = self.next_id.fetch_add(1, Ordering::AcqRel);
        log::trace!("Creating proxy task {}", id);

        let dependents = match *notifier.continuation.lock().unwrap() {
            Continuation::Playing {
                ref dependents,
                waiting_threads: _,
            } => dependents.clone(),
            Continuation::Done => unreachable!(),
        };

        ProtoTask {
            id,
            name: name.to_string(),
            conductor: &self.conductor,
            notifier: Notifier {
                continuation: Mutex::new(Continuation::Playing {
                    dependents,
                    waiting_threads: Vec::new(),
                }),
            },
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
            notifier: Arc::clone(&task.notifier),
        }
    }

    /// Add a dependency on another task, which is possibly running.
    pub fn depend_on<D: Dependency>(&mut self, dependency: &D) {
        dependency.block(self);
    }
}

impl Drop for IdleTask {
    fn drop(&mut self) {
        if let Some(ready) = self.task.extract() {
            self.conductor.schedule(ready);
        }
    }
}
