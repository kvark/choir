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
    borrow::Cow,
    fmt, mem, ops,
    sync::{
        atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering},
        Arc, Condvar, Mutex, RwLock,
    },
    thread, time,
};

const BITS_PER_BYTE: usize = 8;
const MAX_WORKERS: usize = mem::size_of::<usize>() * BITS_PER_BYTE;

/// Name to be associated with a task.
pub type Name = Cow<'static, str>;

#[derive(Debug)]
struct WaitingThread {
    // The conditional variable lives on the stack of the
    // thread that is currently waiting, during the lifetime
    // of this pointer, so it's safe.
    condvar: *const Condvar,
}
unsafe impl Send for WaitingThread {}
unsafe impl Sync for WaitingThread {}

#[derive(Debug, Default)]
struct Continuation {
    parent: Option<Arc<Notifier>>,
    forks: usize,
    dependents: Vec<Linearc<Task>>,
    waiting_threads: Vec<WaitingThread>,
}

impl Continuation {
    fn unpark_waiting(&mut self) {
        for wt in self.waiting_threads.drain(..) {
            log::trace!("\tresolving a join");
            unsafe {
                (*wt.condvar).notify_all();
            }
        }
    }
}

/// An object responsible to notify follow-up tasks.
#[derive(Debug)]
pub struct Notifier {
    name: Name,
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
    worker_index: isize,
}

impl<'a> ExecutionContext<'a> {
    /// Get the running task handle of the current task.
    pub fn self_task(&self) -> RunningTask {
        RunningTask {
            choir: Arc::clone(self.choir),
            notifier: Arc::clone(self.notifier),
        }
    }

    /// Return the main choir.
    pub fn choir(&self) -> &'a Arc<Choir> {
        self.choir
    }

    /// Fork the current task.
    ///
    /// The new task will block all the same dependents as the currently executing task.
    ///
    /// This is useful because it allows creating tasks on the fly from within
    /// other tasks. Generally, making a task in flight depend on anything is impossible.
    /// But one can fork a dependency instead.
    pub fn fork<N: Into<Name>>(&self, name: N) -> ProtoTask {
        let name = name.into();
        log::trace!("Forking task {} as '{}'", self.notifier, name);
        ProtoTask {
            choir: self.choir,
            name,
            parent: Some(Arc::clone(self.notifier)),
            dependents: Vec::new(),
        }
    }

    /// Register an existing task as a fork.
    ///
    /// It will block all the dependents of the currently executing task.
    ///
    /// Will panic if the other task is already a fork of something.
    pub fn add_fork<D: AsRef<Notifier>>(&self, other: &D) {
        if let Some(ref mut continuation) = *other.as_ref().continuation.lock().unwrap() {
            assert!(continuation.parent.is_none());
            continuation.parent = Some(Arc::clone(self.notifier));
            self.notifier
                .continuation
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .forks += 1;
        }
    }
}

impl Drop for ExecutionContext<'_> {
    fn drop(&mut self) {
        if thread::panicking() {
            self.choir.issue_panic(self.worker_index);
            let mut guard = self.notifier.continuation.lock().unwrap();
            if let Some(mut cont) = guard.take() {
                cont.unpark_waiting();
            }
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

struct WorkerContext {}

struct WorkerPool {
    contexts: [Option<WorkerContext>; MAX_WORKERS],
}

/// Return type for `join` functions that have to detect panics.
/// Does actual panic on drop if any of the tasks have panicked.
/// Note: an idiomatic `Result` is not used because it's not actionable.
pub struct MaybePanic {
    worker_index: isize,
}
impl Drop for MaybePanic {
    fn drop(&mut self) {
        assert_eq!(
            self.worker_index, -1,
            "Panic occurred on worker {}",
            self.worker_index,
        );
    }
}

/// Main structure for managing tasks.
pub struct Choir {
    injector: Injector<Task>,
    condvar: Condvar,
    parked_mask_mutex: Mutex<usize>,
    workers: RwLock<WorkerPool>,
    panic_worker: AtomicIsize,
}

impl Default for Choir {
    fn default() -> Self {
        const NO_WORKER: Option<WorkerContext> = None;
        let injector = Injector::new();
        Self {
            injector,
            condvar: Condvar::default(),
            parked_mask_mutex: Mutex::new(0),
            workers: RwLock::new(WorkerPool {
                contexts: [NO_WORKER; MAX_WORKERS],
            }),
            panic_worker: AtomicIsize::new(-1),
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
        let worker_clone = Arc::clone(&worker);
        let choir = Arc::clone(self);

        let join_handle = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || choir.work_loop(&worker_clone))
            .unwrap();

        WorkerHandle {
            worker,
            join_handle: Some(join_handle),
            choir: Arc::clone(self),
        }
    }

    /// Spawn a new task.
    pub fn spawn<'a, N: Into<Name>>(self: &'a Arc<Self>, name: N) -> ProtoTask<'a> {
        let name = name.into();
        log::trace!("Creating task '{}", name);
        ProtoTask {
            choir: self,
            name,
            parent: None,
            dependents: Vec::new(),
        }
    }

    fn schedule(&self, task: Task) {
        log::trace!("Task {} is scheduled", task.notifier);
        self.injector.push(task);
        self.condvar.notify_one();
    }

    fn execute(self: &Arc<Self>, task: Task, worker_index: isize) {
        let execontext = ExecutionContext {
            choir: self,
            notifier: &task.notifier,
            worker_index,
        };
        match task.functor {
            Functor::Dummy => {
                log::debug!(
                    "Task {} (dummy) runs on thread[{}]",
                    task.notifier,
                    worker_index
                );
                drop(execontext);
            }
            Functor::Once(fun) => {
                log::debug!("Task {} runs on thread[{}]", task.notifier, worker_index);
                profiling::scope!(task.notifier.name.as_ref());
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
                if middle != sub_range.start && *self.parked_mask_mutex.lock().unwrap() != 0 {
                    self.injector.push(Task {
                        functor: Functor::Multi(middle..sub_range.end, Linearc::clone(&fun)),
                        notifier: Arc::clone(&task.notifier),
                    });
                    log::trace!("\tsplit out {:?}", middle..sub_range.end);
                    sub_range.end = middle;
                    self.condvar.notify_one();
                }
                // fun the functor
                {
                    profiling::scope!(task.notifier.name.as_ref());
                    (fun)(execontext, sub_range.start);
                }
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

        let mut notifier_opt = Some(task.notifier);
        while let Some(notifier) = notifier_opt {
            notifier_opt = self.finish(&notifier);
        }
    }

    #[profiling::function]
    fn finish(&self, notifier: &Notifier) -> Option<Arc<Notifier>> {
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
            let mut cont = guard.take().unwrap();
            //Note: this is important to do within the lock,
            // so that anything waiting for the unpark signal
            // is guaranteed to be notified.
            cont.unpark_waiting();
            cont
        };

        // unblock dependencies if needed
        for dependent in continuation.dependents {
            if let Some(ready) = Linearc::into_inner(dependent) {
                self.schedule(ready);
            }
        }

        continuation.parent
    }

    fn register(&self) -> Option<usize> {
        let mut pool = self.workers.write().unwrap();
        let index = pool.contexts.iter_mut().position(|c| c.is_none())?;
        pool.contexts[index] = Some(WorkerContext {});
        Some(index)
    }

    fn unregister(&self, index: usize) {
        self.workers.write().unwrap().contexts[index] = None;
        // Avoid a situation where choir is expecting this thread
        // to help with more tasks.
        self.condvar.notify_one();
    }

    fn work_loop(self: &Arc<Self>, worker: &Worker) {
        profiling::register_thread!();
        let index = self.register().unwrap();
        log::info!("Thread[{}] = '{}' started", index, worker.name);

        loop {
            match self.injector.steal() {
                Steal::Empty => {
                    log::trace!("Thread[{}] sleeps", index);
                    let mask = 1 << index;
                    let mut parked_mask = self.parked_mask_mutex.lock().unwrap();
                    if !worker.alive.load(Ordering::Acquire) {
                        break;
                    }
                    *parked_mask |= mask;
                    parked_mask = self.condvar.wait(parked_mask).unwrap();
                    *parked_mask &= !mask;
                }
                Steal::Success(task) => {
                    self.execute(task, index as isize);
                }
                Steal::Retry => {}
            }
        }

        log::info!("Thread '{}' dies", worker.name);
        self.unregister(index);
    }

    fn flush_queue(&self) {
        let mut num_tasks = 0;
        loop {
            match self.injector.steal() {
                Steal::Empty => {
                    break;
                }
                Steal::Success(task) => {
                    num_tasks += 1;
                    let mut guard = task.notifier.continuation.lock().unwrap();
                    if let Some(mut cont) = guard.take() {
                        cont.unpark_waiting();
                    }
                }
                Steal::Retry => {}
            }
        }
        log::trace!("\tflushed {} tasks down the drain", num_tasks);
    }

    fn issue_panic(&self, worker_index: isize) {
        log::debug!("panic on worker {}", worker_index);
        self.panic_worker.store(worker_index, Ordering::Release);
        self.flush_queue();
    }

    /// Check if any of the workers terminated with panic.
    pub fn check_panic(&self) -> MaybePanic {
        let worker_index = self.panic_worker.load(Ordering::Acquire);
        MaybePanic { worker_index }
    }
}

/// Handle object holding a worker thread alive.
pub struct WorkerHandle {
    worker: Arc<Worker>,
    join_handle: Option<thread::JoinHandle<()>>,
    choir: Arc<Choir>,
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
    name: Name,
    parent: Option<Arc<Notifier>>,
    dependents: Vec<Linearc<Task>>,
}

/// Task that is created but not running yet.
/// It will be scheduled on `run()` or on drop.
/// The 'a lifetime is responsible for the data
/// in the closure of the task function.
pub struct IdleTask {
    choir: Arc<Choir>,
    task: MaybeArc<Task>,
}

impl AsRef<Notifier> for IdleTask {
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

impl Drop for ProtoTask<'_> {
    fn drop(&mut self) {
        for dependent in self.dependents.drain(..) {
            if let Some(ready) = Linearc::into_inner(dependent) {
                self.choir.schedule(ready);
            }
        }
    }
}

impl ProtoTask<'_> {
    /// Block another task from starting until this one is finished.
    /// This is the reverse of `depend_on` but could be done faster because
    /// the `self` task is not yet shared with anything.
    pub fn as_blocker_for(mut self, other: &mut IdleTask) -> Self {
        self.dependents.push(other.task.share());
        self
    }

    fn fill(mut self, functor: Functor) -> IdleTask {
        // Only register the fork here, so that nothing happens if a `ProtoTask` is dropped.
        if let Some(ref parent_notifier) = self.parent {
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
                notifier: Arc::new(Notifier {
                    name: mem::take(&mut self.name),
                    continuation: Mutex::new(Some(Continuation {
                        parent: self.parent.take(),
                        forks: 0,
                        dependents: mem::take(&mut self.dependents),
                        waiting_threads: Vec::new(),
                    })),
                }),
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
    /// and all of its dependencies are fulfilled.
    pub fn init<F: FnOnce(ExecutionContext) + Send + 'static>(self, fun: F) -> IdleTask {
        let b: Box<dyn FnOnce(ExecutionContext) + Send + 'static> = Box::new(fun);
        // Transmute is for the lifetime bound only: it's stored as `'static`,
        // but the only way to run it is `run_attached`, which would be blocking.
        self.fill(Functor::Once(unsafe { mem::transmute(b) }))
    }

    /// Init task to execute a function multiple times.
    /// Every invocation is given an index in 0..count
    /// There are no ordering guarantees between the indices.
    pub fn init_multi<F: Fn(ExecutionContext, SubIndex) + Send + Sync + 'static>(
        self,
        count: SubIndex,
        fun: F,
    ) -> IdleTask {
        self.fill(if count == 0 {
            Functor::Dummy
        } else {
            let arc: Linearc<dyn Fn(ExecutionContext, SubIndex) + Send + Sync + 'static> =
                Linearc::new_unsized(fun);
            Functor::Multi(0..count, unsafe { mem::transmute(arc) })
        })
    }

    /// Init task to execute a function on each element of a finite iterator.
    /// Similarly to `init_multi`, each invocation is executed
    /// indepdently and can be out of order.
    pub fn init_iter<I, F>(self, iter: I, fun: F) -> IdleTask
    where
        I: Iterator,
        I::Item: Send + 'static,
        F: Fn(ExecutionContext, I::Item) + Send + Sync + 'static,
    {
        let task_data = iter.collect::<util::PerTaskData<_>>();
        self.init_multi(task_data.len(), move |exe_context, index| unsafe {
            fun(exe_context, task_data.take(index))
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
    pub fn join(&self) -> MaybePanic {
        log::debug!("Joining {}", self.notifier);
        let mut guard = self.notifier.continuation.lock().unwrap();
        // This code is a bit magical,
        // and it's amazing to see it compiling and working.
        if let Some(ref mut cont) = *guard {
            let condvar = Condvar::new();
            cont.waiting_threads
                .push(WaitingThread { condvar: &condvar });
            let _ = condvar.wait_while(guard, |cont| cont.is_some());
        }
        self.choir.check_panic()
    }

    /// Block until the task has finished executing.
    /// Also, use the current thread to help in the meantime.
    #[profiling::function]
    pub fn join_active(&self) -> MaybePanic {
        let condvar;
        match *self.notifier.continuation.lock().unwrap() {
            Some(ref mut cont) => {
                condvar = Condvar::new();
                cont.waiting_threads
                    .push(WaitingThread { condvar: &condvar });
            }
            None => return self.choir.check_panic(),
        }
        let index = self.choir.register().unwrap();
        log::info!("Join thread[{}] started", index);

        loop {
            let is_done = match self.choir.injector.steal() {
                Steal::Empty => {
                    log::trace!("Thread[{}] sleeps", index);
                    let guard = self.notifier.continuation.lock().unwrap();
                    if guard.is_some() {
                        let guard = condvar.wait(guard).unwrap();
                        guard.is_none()
                    } else {
                        false
                    }
                }
                Steal::Success(task) => {
                    self.choir.execute(task, index as isize);
                    self.is_done()
                }
                Steal::Retry => false,
            };
            if is_done {
                break;
            }
        }

        log::info!("Thread[{}] is released", index);
        self.choir.unregister(index);
        self.choir.check_panic()
    }

    /// Block until the task has finished executing, with timeout.
    /// Panics and prints helpful info if the timeout is reached.
    pub fn join_debug(&self, timeout: time::Duration) -> MaybePanic {
        log::debug!("Joining {}", self.notifier);
        let mut guard = self.notifier.continuation.lock().unwrap();
        if let Some(ref mut cont) = *guard {
            let condvar = Condvar::new();
            cont.waiting_threads
                .push(WaitingThread { condvar: &condvar });
            let (guard, wait_result) = condvar
                .wait_timeout_while(guard, timeout, |cont| cont.is_some())
                .unwrap();
            if wait_result.timed_out() {
                println!("Join timeout reached for {}", self.notifier);
                println!("Continuation: {:?}", guard);
                panic!("");
            }
        }
        self.choir.check_panic()
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        self.worker.alive.store(false, Ordering::Release);
        let handle = self.join_handle.take().unwrap();
        // make sure it wakes up and checks if it's still alive
        // Locking the mutex is required to guarantee that the worker loop
        // actually receives the notification.
        if let Ok(_guard) = self.choir.parked_mask_mutex.lock() {
            self.choir.condvar.notify_all();
        }
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
            choir: Arc::clone(&self.choir),
            notifier: Arc::clone(&task.notifier),
        }
    }

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
    #[profiling::function]
    pub fn depend_on<D: AsRef<Notifier>>(&mut self, dependency: &D) {
        if let Some(ref mut cont) = *dependency.as_ref().continuation.lock().unwrap() {
            cont.dependents.push(self.task.share());
        }
    }
}

impl Drop for IdleTask {
    fn drop(&mut self) {
        if let Some(ready) = self.task.extract() {
            self.choir.schedule(ready);
        }
    }
}
