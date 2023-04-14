/*! Task Orchestration Framework.

This framework helps to organize the execution of a program
into a live task graph. In this model, all the work is happening
inside tasks, which are scheduled to run by the `Choir`.

Lifetime of a Task:
  1. Idle: task is just created.
  2. Initialized: function body is assigned.
  3. Scheduled: no more dependencies can be added to the task.
  4. Executing: task was dispatched from the queue by one of the workers.
  5. Done: task is retired.
!*/

#![allow(
    renamed_and_removed_lints,
    clippy::new_without_default,
    clippy::unneeded_field_pattern,
    clippy::match_like_matches_macro,
    clippy::manual_strip,
    clippy::if_same_then_else,
    clippy::unknown_clippy_lints,
    clippy::len_without_is_empty,
    clippy::should_implement_trait
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

/// Better shared pointer.
pub mod arc;
/// Additional utilities.
pub mod util;

use self::arc::Linearc;
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

#[derive(Debug, Default)]
struct Continuation {
    forks: usize,
    dependents: Vec<Linearc<Task>>,
    waiting_threads: Vec<thread::Thread>,
}

/// An object responsible to notify follow-up tasks.
#[derive(Debug)]
pub struct Notifier {
    name: String,
    parent: Option<Arc<Notifier>>,
    continuation: Mutex<Option<Continuation>>,
}

impl fmt::Display for Notifier {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "'{}'", self.name)
    }
}

/// Context of a task execution body.
pub struct ExecutionContext<'a> {
    choir: &'a Arc<Choir>,
    notifier: &'a Arc<Notifier>,
}

impl ExecutionContext<'_> {
    /// Get the running task handle of the current task.
    pub fn self_task(&self) -> RunningTask {
        RunningTask {
            choir: Arc::clone(self.choir),
            notifier: Arc::clone(self.notifier),
        }
    }
    /// Fork the current task.
    ///
    /// This is useful because it allows creating tasks on the fly from within
    /// other tasks. Generally, making a task in flight depend on anything is impossible.
    /// But one can fork a dependency instead.
    pub fn fork(&self, name: impl fmt::Display) -> ProtoTask {
        log::trace!("Forking task {} as '{}'", self.notifier, name);
        ProtoTask {
            choir: self.choir,
            notifier: Notifier {
                name: format!("{}/{}", self.notifier.name, name),
                parent: Some(Arc::clone(self.notifier)),
                continuation: Mutex::new(Some(Continuation::default())),
            },
        }
    }
}

/// Index of a sub-task inside a multi-task.
pub type SubIndex = u32;

enum Functor {
    Dummy,
    Once(Box<dyn FnOnce(ExecutionContext) + Send + 'static>),
    Multi(
        ops::Range<SubIndex>,
        Linearc<dyn Fn(ExecutionContext, SubIndex) + Send + Sync + 'static>,
    ),
}

impl fmt::Debug for Functor {
    fn fmt(&self, serializer: &mut fmt::Formatter) -> fmt::Result {
        match *self {
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
    /// Body of the task.
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

/// Main structure for managing tasks.
pub struct Choir {
    injector: Injector<Task>,
    workers: RwLock<WorkerPool>,
    parked_mask: AtomicUsize,
}

impl Default for Choir {
    fn default() -> Self {
        const NO_WORKER: Option<WorkerContext> = None;
        let injector = Injector::new();
        Self {
            injector,
            workers: RwLock::new(WorkerPool {
                contexts: [NO_WORKER; MAX_WORKERS],
            }),
            parked_mask: AtomicUsize::new(0),
        }
    }
}

impl Choir {
    /// Create a new task system.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Add a new worker thread.
    ///
    /// Note: A system can't have more than `MAX_WORKERS` workers
    /// enabled at any time.
    pub fn add_worker(self: &Arc<Self>, name: &str) -> WorkerHandle {
        let worker = Arc::new(Worker {
            name: name.to_string(),
            alive: AtomicBool::new(true),
        });
        let choir = Arc::clone(self);
        let worker_clone = Arc::clone(&worker);

        let join_handle = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || choir.work_loop(&worker_clone))
            .unwrap();

        WorkerHandle {
            worker,
            join_handle: Some(join_handle),
        }
    }

    /// Spawn a new task.
    #[profiling::function]
    pub fn spawn<'a>(self: &'a Arc<Self>, name: impl fmt::Display) -> ProtoTask<'a> {
        log::trace!("Creating task '{}", name);
        ProtoTask {
            choir: self,
            notifier: Notifier {
                name: format!("{}", name),
                parent: None,
                continuation: Mutex::new(Some(Continuation::default())),
            },
        }
    }

    fn wake_up_one(&self) {
        let mask = self.parked_mask.load(Ordering::Acquire);
        // Wake up a thread if there is a sleeping one.
        if mask != 0 {
            let index = mask.trailing_zeros() as usize;
            profiling::scope!("unpark");
            log::trace!("\twaking up thread[{}]", index);
            let pool = self.workers.read().unwrap();
            if let Some(context) = pool.contexts[index].as_ref() {
                context.thread.unpark();
            }
        }
    }

    fn schedule(&self, task: Task) {
        log::trace!("Task {} is scheduled", task.notifier);
        self.injector.push(task);
        self.wake_up_one();
    }

    fn execute(self: &Arc<Self>, task: Task, worker_index: isize) {
        let execontext = ExecutionContext {
            choir: self,
            notifier: &task.notifier,
        };
        match task.functor {
            Functor::Dummy => {
                log::debug!(
                    "Task {} (dummy) runs on thread[{}]",
                    task.notifier,
                    worker_index
                );
            }
            Functor::Once(fun) => {
                log::debug!("Task {} runs on thread[{}]", task.notifier, worker_index);
                profiling::scope!("execute");
                (fun)(execontext);
            }
            Functor::Multi(mut sub_range, fun) => {
                log::debug!(
                    "Task {} ({}) runs on thread[{}]",
                    task.notifier,
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
                            functor: Functor::Multi(middle..sub_range.end, Linearc::clone(&fun)),
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
                (fun)(execontext, sub_range.start);
                // are we done yet?
                sub_range.start += 1;
                if sub_range.start == sub_range.end {
                    if !Linearc::drop_last(fun) {
                        return;
                    }
                } else {
                    // Put it back to the queue, with the next sub-index.
                    // Note: we aren't calling `schedule` because we know at least this very thread
                    // will be able to pick it up, so no need to wake up anybody.
                    self.injector.push(Task {
                        functor: Functor::Multi(sub_range, fun),
                        notifier: task.notifier,
                    });
                    return;
                }
            }
        }

        let mut notifier_ref = Some(&task.notifier);
        while let Some(notifier) = notifier_ref {
            notifier_ref = self.finish(notifier);
        }
    }

    fn finish<'a>(&self, notifier: &'a Notifier) -> Option<&'a Arc<Notifier>> {
        profiling::scope!("unblock");
        // mark the task as done
        log::trace!("Finishing task {}", notifier);

        let continuation = {
            let mut guard = notifier.continuation.lock().unwrap();
            if let Some(ref mut cont) = *guard {
                if cont.forks != 0 {
                    log::trace!("\t{} forks are still alive", cont.forks);
                    cont.forks -= 1;
                    return None;
                }
            }
            guard.take().unwrap()
        };

        for thread in continuation.waiting_threads {
            log::trace!("\tresolving a join");
            thread.unpark();
        }

        // unblock dependencies if needed
        for dependent in continuation.dependents {
            if let Some(ready) = Linearc::into_inner(dependent) {
                self.schedule(ready);
            }
        }

        notifier.parent.as_ref()
    }

    fn register(&self) -> Option<usize> {
        let mut pool = self.workers.write().unwrap();
        let index = pool.contexts.iter_mut().position(|c| c.is_none())?;
        pool.contexts[index] = Some(WorkerContext {
            thread: thread::current(),
        });
        Some(index)
    }

    fn unregister(&self, index: usize) {
        self.workers.write().unwrap().contexts[index] = None;
        // Avoid a situation where choir is expecting this thread
        // to help with more tasks.
        if !self.injector.is_empty() {
            self.wake_up_one();
        }
    }

    fn work(self: &Arc<Self>, index: usize) {
        match self.injector.steal() {
            Steal::Empty => {
                log::trace!("Thread[{}] sleeps", index);
                let mask = 1 << index;
                self.parked_mask.fetch_or(mask, Ordering::Release);
                if self.injector.is_empty() {
                    // We are on our way to sleep, but there might be
                    // a `schedule()` call running elsewhere,
                    // missing our parked bit.
                    profiling::scope!("park");
                    thread::park();
                }
                self.parked_mask.fetch_and(!mask, Ordering::Release);
            }
            Steal::Success(task) => {
                self.execute(task, index as isize);
            }
            Steal::Retry => {}
        }
    }

    fn work_loop(self: &Arc<Self>, worker: &Worker) {
        profiling::register_thread!();
        let index = self.register().unwrap();
        log::info!("Thread[{}] = '{}' started", index, worker.name);

        while worker.alive.load(Ordering::Acquire) {
            self.work(index);
        }

        log::info!("Thread '{}' dies", worker.name);
        self.unregister(index);
    }
}

/// Handle object holding a worker thread alive.
pub struct WorkerHandle {
    worker: Arc<Worker>,
    join_handle: Option<thread::JoinHandle<()>>,
}

enum MaybeArc<T> {
    Unique(T),
    Shared(Linearc<T>),
    Null,
}

impl<T> MaybeArc<T> {
    const NULL_ERROR: &'static str = "Value is gone!";

    fn new(value: T) -> Self {
        Self::Unique(value)
    }

    fn share(&mut self) -> Linearc<T> {
        let arc = match mem::replace(self, Self::Null) {
            Self::Unique(value) => Linearc::new(value),
            Self::Shared(arc) => arc,
            Self::Null => panic!("{}", Self::NULL_ERROR),
        };
        *self = Self::Shared(Linearc::clone(&arc));
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
            // No dependencies, can be launched now.
            Self::Unique(value) => Some(value),
            // There are dependencies, potentially all resolved now.
            Self::Shared(arc) => Linearc::into_inner(arc),
            // Already been extracted.
            Self::Null => None,
        }
    }
}

/// Task construct without any functional logic.
pub struct ProtoTask<'c> {
    choir: &'c Arc<Choir>,
    notifier: Notifier,
}

/// Task that is created but not running yet.
/// It will be scheduled on `run()` or on drop.
/// The 'a lifetime is responsible for the data
/// in the closure of the task function.
pub struct IdleTask<'a> {
    choir: Arc<Choir>,
    task: MaybeArc<Task>,
    _lifetime: &'a (),
}

impl AsRef<Notifier> for IdleTask<'_> {
    fn as_ref(&self) -> &Notifier {
        &self.task.as_ref().notifier
    }
}

//HACK: turns out, it's impossible to re-implement `Arc` in Rust stable userspace,
// Because `Arc` relies on `trait Unsized` and `std::ops::CoerceUnsized`, which
// are still nightly-only behind a feature.
impl<'a> Linearc<dyn Fn(ExecutionContext, SubIndex) + Send + Sync + 'a> {
    fn new_unsized(fun: impl Fn(ExecutionContext, SubIndex) + Send + Sync + 'a) -> Self {
        Self::from_inner(Box::new(arc::LinearcInner {
            ref_count: AtomicUsize::new(1),
            data: fun,
        }))
    }
}

impl ProtoTask<'_> {
    fn fill<'a>(self, functor: Functor) -> IdleTask<'a> {
        // Only register the fork here, so that nothing happens if a `ProtoTask` is dropped.
        if let Some(ref parent_notifier) = self.notifier.parent {
            parent_notifier
                .continuation
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .forks += 1;
        }
        IdleTask {
            choir: Arc::clone(self.choir),
            task: MaybeArc::new(Task {
                functor,
                notifier: Arc::new(self.notifier),
            }),
            _lifetime: &(),
        }
    }

    /// Init task with no function.
    /// Can be useful to aggregate dependencies, for example
    /// if a function returns a task handle, and it launches
    /// multiple sub-tasks in parallel.
    pub fn init_dummy(self) -> IdleTask<'static> {
        self.fill(Functor::Dummy)
    }

    /// Init task to execute a standalone function.
    /// The function body will be executed once the task is scheduled,
    /// and all of its dependencies are fulfilled.
    pub fn init<'a, F: FnOnce(ExecutionContext) + Send + 'a>(self, fun: F) -> IdleTask<'a> {
        let b: Box<dyn FnOnce(ExecutionContext) + Send + 'a> = Box::new(fun);
        // Transmute is for the lifetime bound only: it's stored as `'static`,
        // but the only way to run it is `run_attached`, which would be blocking.
        self.fill(Functor::Once(unsafe { mem::transmute(b) }))
    }

    /// Init task to execute a function multiple times.
    /// Every invocation is given an index in 0..count
    /// There are no ordering guarantees between the indices.
    pub fn init_multi<'a, F: Fn(ExecutionContext, SubIndex) + Send + Sync + 'a>(
        self,
        count: SubIndex,
        fun: F,
    ) -> IdleTask<'a> {
        self.fill(if count == 0 {
            Functor::Dummy
        } else {
            let arc: Linearc<dyn Fn(ExecutionContext, SubIndex) + Send + Sync + 'a> =
                Linearc::new_unsized(fun);
            Functor::Multi(0..count, unsafe { mem::transmute(arc) })
        })
    }

    /// Init task to execute a function on each element of a finite iterator.
    /// Similarly to `init_multi`, each invocation is executed
    /// indepdently and can be out of order.
    pub fn init_iter<'a, I, F>(self, iter: I, fun: F) -> IdleTask<'a>
    where
        I: Iterator,
        I::Item: Send + 'a,
        F: Fn(I::Item) + Send + Sync + 'a,
    {
        let task_data = iter.collect::<util::PerTaskData<_>>();
        self.init_multi(task_data.len(), move |_, index| unsafe {
            fun(task_data.take(index))
        })
    }
}

/// Task that is already scheduled for running.
#[derive(Clone)]
pub struct RunningTask {
    choir: Arc<Choir>,
    notifier: Arc<Notifier>,
}

impl fmt::Debug for RunningTask {
    fn fmt(&self, serializer: &mut fmt::Formatter) -> fmt::Result {
        self.notifier.fmt(serializer)
    }
}

impl AsRef<Notifier> for RunningTask {
    fn as_ref(&self) -> &Notifier {
        &self.notifier
    }
}

impl RunningTask {
    /// Return true if this task is done.
    pub fn is_done(&self) -> bool {
        self.notifier.continuation.lock().unwrap().is_none()
    }

    /// Block until the task has finished executing.
    #[profiling::function]
    pub fn join(self) {
        log::debug!("Joining {}", self.notifier);
        match *self.notifier.continuation.lock().unwrap() {
            Some(ref mut cont) => {
                cont.waiting_threads.push(thread::current());
            }
            None => return,
        }
        loop {
            log::trace!("Parking for {}", self.notifier);
            thread::park();
            if self.is_done() {
                break;
            }
        }
    }

    /// Block until the task has finished executing.
    /// Also, use the current thread to help in the meantime.
    #[profiling::function]
    pub fn join_active(self) {
        match *self.notifier.continuation.lock().unwrap() {
            Some(ref mut cont) => {
                cont.waiting_threads.push(thread::current());
            }
            None => return,
        }
        let index = self.choir.register().unwrap();
        log::info!("Join thread[{}] started", index);

        loop {
            self.choir.work(index);
            if self.is_done() {
                break;
            }
        }

        log::info!("Thread[{}] is released", index);
        self.choir.unregister(index);
    }

    /// Block until the task has finished executing, with timeout.
    /// Panics and prints helpful info if the timeout is reached.
    pub fn join_debug(self, timeout: time::Duration) {
        log::debug!("Joining {}", self.notifier);
        match *self.notifier.continuation.lock().unwrap() {
            Some(ref mut cont) => {
                cont.waiting_threads.push(thread::current());
            }
            None => return,
        }
        let instant = time::Instant::now();
        loop {
            log::trace!("Parking for {}", self.notifier);
            thread::park_timeout(timeout);
            if self.notifier.continuation.lock().unwrap().is_none() {
                return;
            }
            if instant.elapsed() > timeout {
                println!("Join timeout reached for {}", self.notifier);
                println!(
                    "Continuation: {:?}",
                    self.notifier.continuation.lock().unwrap()
                );
                panic!("");
            }
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

impl IdleTask<'static> {
    /// Schedule this task for running.
    ///
    /// It will only be executed once the dependencies are fulfilled.
    pub fn run(self) -> RunningTask {
        let task = self.task.as_ref();
        RunningTask {
            choir: Arc::clone(&self.choir),
            notifier: Arc::clone(&task.notifier),
        }
    }
}

impl<'a> IdleTask<'a> {
    /// Run the task now and block until it's executed.
    /// Use the current thread to help the choir in the meantime.
    pub fn run_attached(mut self) {
        let task = self.task.as_ref();
        let notifier = Arc::clone(&task.notifier);

        if let Some(ready) = self.task.extract() {
            // Task has no dependencies. No need to join the pool,
            // just execute it right here instead.
            self.choir.execute(ready, -1);
        } else {
            RunningTask {
                choir: Arc::clone(&self.choir),
                notifier,
            }
            .join_active();
        }
    }

    /// Add a dependency on another task, which is possibly running.
    pub fn depend_on<D: AsRef<Notifier>>(&mut self, dependency: &D) {
        if let Some(ref mut cont) = *dependency.as_ref().continuation.lock().unwrap() {
            cont.dependents.push(self.task.share());
        }
    }
}

impl Drop for IdleTask<'_> {
    fn drop(&mut self) {
        if let Some(ready) = self.task.extract() {
            self.choir.schedule(ready);
        }
    }
}
