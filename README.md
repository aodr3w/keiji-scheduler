# KEIJI-SCHEDULER

## about

keiji-scheduler is a Go program responsible for managing the execution cycle of tasks within the keiji task scheduling system. It reads tasks from a database and schedules each task to run in its own goroutine at an interval unique to that task. Additionally, keiji-scheduler listens for messages over a TCP bus, allowing it to gracefully terminate, disable, or delete tasks based on directives received.

## features

- Concurrent Task Execution: Each task runs in its own goroutine, ensuring parallel processing.

- Customizable Intervals: Tasks can be scheduled to run at intervals specific to their requirements.

- Database-Driven: Tasks are stored and managed in a database, allowing for dynamic updates and easy management.

- TCP Bus Integration: Supports external control via a TCP bus for terminating, disabling, or deleting tasks.

- Graceful Termination: Each task checks a termination channel between intervals, allowing for smooth shutdowns.

- Context-based System Termination: System-level shutdowns are managed through context propagation, ensuring all tasks are properly terminated.


## INSTALLATION

`go install github.com/aodr3w/keiji-scheduler@latest`


## START

`keiji-scheduler`

This will start the scheduler, which will automatically begin reading tasks from the configured database, scheduling them according to their specified intervals, and listening to the TCP bus for control messages.


## LOGGING

- logs are written to `$HOME/.keiji/logs/services/scheduler/scheduler.log`


## TCP Bus Commands

keiji-scheduler listens to a TCP bus for the following commands:

**disable**: Disables a specific task, preventing it from running in the future.

**stop**: Immediately stops a currently running task. unless the task is also disabled it will be picked-up immediately therfore this command is useful when trying to restart a task due to changes in the tasks execution binary.

**delete**: Deletes a task, including its binaries and logs, and removes it from the database.

Each task has a termination channel that it checks between intervals to determine if it should stop, disable, or delete itself based on the received command.


## Integration with Keiji

`keiji-scheduler` is part of the broader keiji task scheduling system. You can find more information about how it integrates with other components of keiji by visiting the main repository:


## LICENSE

keiji-scheduler is open-source software licensed under the MIT License. See the LICENSE file for more details.