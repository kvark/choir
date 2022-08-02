# Change Log

## v0.5 (TBD)
  - all functors accept an argument of `Notifier`
  - ability to spawn proxy tasks from a functor body
  - `RunningTask::join()` instead of `Choir::wait_all`
  - intermediate `ProtoTask` type
  - `impl Clone for RunningTask`
  - everything implements `Debug`

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
