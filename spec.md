1. Problem Definition

1.1 User Problem

Developers need a simple, composable CLI tool that:

* Reacts to incoming “notifications”
* Executes a user-defined shell command per notification
* Supports flexible ways to generate and send notifications
* Handles concurrency, restarts, and automation loops cleanly

Existing tools (e.g., watch, entr, message queues) are either:

* Too rigid
* Not composable
* Or require external infrastructure

1.2 Solution Overview

A CLI tool (notify) that:

* Starts a foreground instance bound to a unique name
* Receives notifications via an internal Unix socket
* Executes a shell command per notification
* Provides multiple ways to generate notifications:
    * Direct stdin input
    * Output of another command
    * Recursively running commands

⸻

2. Target Users

* Developers automating workflows
* CLI power users
* DevOps engineers
* Tool builders needing lightweight orchestration

⸻

3. Scope

Included

* Single-machine operation
* Named instances using Unix sockets
* Queueing and restart execution modes
* Notification delivery via CLI commands
* Recursive command execution loops

Excluded

* Distributed systems / networking
* Authentication / multi-user isolation
* Persistent storage
* Daemonization

⸻

4. Core Concepts

4.1 Instance

A running process started via:

notify start <name> "<command>"

* Runs in foreground
* Bound to <name>
* Creates a Unix socket:

/tmp/notify-<name>.sock

4.2 Notification

A byte stream sent to an instance:

* Delivered via socket
* Passed to command via stdin

4.3 Execution Modes

Default (Queue Mode)

* Notifications processed strictly sequentially
* FIFO ordering guaranteed

Restart Mode

* If a new notification arrives:
    * Kill current process
    * Start next notification
* Queue is preserved (not collapsed)

⸻

5. CLI Interface

5.1 notify start

Syntax

notify start <name> "<command>" [--restart]

Behavior

* Starts instance bound to <name>
* Fails if socket already exists
* Runs attached to terminal
* Stops on:
    * Ctrl-C
    * notify stop <name>

Execution Contract

For each notification:

* Spawn a new process
* No shared state between runs
* Notification bytes → stdin

⸻

5.2 notify stop

Syntax

notify stop <name>

Behavior

* Connects to instance socket
* Sends shutdown signal
* Instance exits gracefully

⸻

5.3 notify send (direct)

Syntax

notify send <name> [--prefix "<text>"]

Input

* stdin only

Behavior

* Reads stdin fully
* Prepends prefix (if provided)
* Sends bytes to instance

Prefix Rules

* Literal string
* No templating
* No escaping
* Concatenated directly before stdin bytes

⸻

5.4 notify send --command

Syntax

notify send <name> --command "<cmd>" [--recursive] [--prefix "<text>"]

Behavior

1. Run <cmd>
2. Capture stdout only
3. Send as notification (with optional prefix)

stderr Handling

* stderr of <cmd> is forwarded to:
    * stderr of the instance command

⸻

5.5 Recursive Mode

Behavior

When --recursive is used:

Loop:

1. Run command
2. Send stdout as notification
3. Repeat

Failure Handling

* Stop after 3 consecutive non-zero exits
* Reset failure count after a successful run

⸻

6. Notification Processing

6.1 Size Limit

* Max size: 50 KB
* If exceeded:
    * Truncate to 50 KB
    * Write full payload to temp file:

/tmp/notify-<random>.log

Truncation Message

Append EXACTLY:

Notification over 50kb and truncated. View the entire notification here: <path>

* Appended to truncated payload
* Included in stdin passed to command

⸻

6.2 Delivery Guarantees

* FIFO ordering (queue mode)
* No loss unless instance stops
* Restart mode preserves queue

⸻

7. Execution Model

7.1 Process Lifecycle

For each notification:

* Spawn child process
* Pipe stdin
* Wait for exit (unless restart interruption)

7.2 Restart Mode Behavior

When new notification arrives:

1. Send termination signal to current process
2. Wait for termination
3. Start next notification

⸻

8. IPC Design

8.1 Transport

* Unix domain socket
* Path:

/tmp/notify-<name>.sock

8.2 Protocol (internal)

* Raw byte stream per notification
* Framing required (implementation detail, e.g., length-prefixed)

8.3 Visibility

* Socket is not exposed to users
* Only CLI interacts with it

⸻

9. Error Handling

9.1 Start Errors

* Name already exists → fail
* Socket creation failure → fail

9.2 Send Errors

* Instance not found → error
* Broken pipe → error

9.3 Command Errors

* Non-zero exit:
    * Ignored in normal operation
    * Counted in recursive mode

⸻

10. Edge Cases

10.1 Large Input

* 50KB → truncate + temp file

10.2 Rapid Notifications

* Queue grows unbounded (in-memory)

10.3 Restart Storm

* Rapid arrivals repeatedly kill process
* Queue still preserved

10.4 Socket Cleanup

* On crash:
    * Stale socket must be handled on next start

10.5 Concurrent Sends

* Multiple send calls must be safe

⸻

11. Non-Functional Requirements

11.1 Performance

* Low latency (<10ms overhead per notification)
* Efficient socket handling
* Minimal memory copying

11.2 Scalability

* Designed for:
    * Hundreds of queued notifications
* Not designed for:
    * High-throughput streaming systems

11.3 Reliability

* No message corruption
* Deterministic ordering

11.4 Security

* Socket permissions restrict access to same user
* No remote access

11.5 Accessibility

* CLI-only
* Predictable UNIX-style behavior

⸻

12. Dependencies

* POSIX system
* Unix domain sockets
* Shell execution environment

⸻

13. Assumptions

* Single-user environment
* /tmp is writable
* Commands are trusted (no sandboxing)

⸻

14. Acceptance Criteria

Instance Management

* Starting instance creates socket
* Duplicate name fails
* Ctrl-C stops instance
* notify stop stops instance

Notification Delivery

* Notifications delivered in order
* Prefix applied correctly
* stdin passed exactly

Execution

* Each notification runs fresh process
* No shared state between runs

Restart Mode

* Running process is killed on new notification
* Queue is preserved

Command Send

* stdout used as notification
* stderr forwarded to instance stderr

Recursive Mode

* Repeats execution
* Stops after 3 consecutive failures

Size Handling

* >50KB truncated
* Temp file created
* Message appended correctly

IPC

* Socket communication works reliably
* Concurrent sends handled correctly
