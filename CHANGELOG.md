# Change Log

## v0.6 (TBD)
  - redesigned fork semantics that's more robust and supports `join()`
  - support non-`'static` function bodies via `run_attached()`
  - truly joining the choir when waiting on a task via `join_active()`
  - always use `Arc<Choir>`

## v0.5 (15-08-2022)
  - all functors accept an argument of `ExecutionContext`
  - ability to fork tasks from a functor body
  - `RunningTask::join()` instead of `Choir::wait_all`
  - intermediate `ProtoTask` type
  - `impl Clone for RunningTask`
  - everything implements `Debug`
  - new `Linearc` type for linearized `Arc`
  - no more spontaneous blocking - the task synchronization is fixed

### v0.4.2 (07-06-2022)
  - dummy tasks support

## v0.4 (01-06-2022)
  - iterator tasks support
  - auto-schedule idle tasks on drop
  - replace `run_xx` and `idle_xx` calls by just `add_xx`

## v0.3 (23-04-2022)
  - no `Sync` bound
  - multi-tasks
  - profiling integration
  - benchmarks

## v0.2 (10-04-2022)
  - task dependencies
  - proper names for things
  - variable worker count

## v0.1 (07-04-2022)
  - basic task execution
