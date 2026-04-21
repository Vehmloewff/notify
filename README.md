# notify

`notify` is a small command-line tool for routing text notifications into shell commands.

Start a named instance with a command you want to run, then send input to it later from anywhere. It is useful for lightweight automation, local event handling, piping command output into another workflow, and building simple notification-driven scripts without standing up a full service.

## Features

- Start one or more named notification instances
- Send notification text over stdin
- Automatically target the only running instance when just one exists
- Queue notifications and process them in order
- Optional restart mode to replace the currently running job with the newest notification
- Run ad hoc commands and send their stdout as notifications
- Optional recursive command mode for repeating commands until 3 consecutive failures
- List running instances and see pending command requests
- Stop an entire instance or cancel a specific pending command
- Handles large notification payloads by truncating the live payload and saving the full contents to a temp file

## Installation

Build from source:

```bash
go build -o notify
```

Optional: move the binary somewhere on your `PATH`.

## Quick start

Start an instance that prints each notification:

```bash
notify start --name demo 'cat'
```

In another terminal, send a notification:

```bash
echo 'hello world' | notify send --name demo
```

The running `demo` instance receives `hello world` on stdin and runs its configured command.

Stop the instance when you are done:

```bash
notify stop --name demo
```

## Usage

```bash
notify start [--name <name>] [--restart] "<command>"
notify stop [--name <name>] [--command <id>]
notify send [--name <name>] [--prefix "<text>"]
notify command [--name <name>] [--recursive] [--prefix "<text>"] "<cmd>"
notify list
```

## Commands

### `notify start`

Start a new instance.

```bash
notify start --name logs 'while read line; do printf "[%s]\n" "$line"; done'
```

Options:

- `--name <name>`: set the instance name
- `--restart`: when a new notification arrives, stop the currently running command and start again with the newest payload

If `--name` is omitted, `notify` generates a random name and prints it.

### `notify send`

Send a notification to a running instance. Notification content is read from stdin.

```bash
printf 'deploy complete' | notify send --name logs
```

Options:

- `--name <name>`: target a specific instance
- `--prefix "<text>"`: prepend text to the notification payload before it is delivered

If exactly one instance is running, `--name` can be omitted.

### `notify command`

Run a shell command and send its stdout to a running instance as a notification.

```bash
notify command --name logs 'date'
```

Options:

- `--name <name>`: target a specific instance
- `--prefix "<text>"`: prepend text to the captured stdout
- `--recursive`: rerun the command after each execution until it fails 3 times in a row

Without `--recursive`, the command runs once.

With `--recursive`, `notify` keeps executing the command, sending each successful or failed run's stdout as a new notification payload, and only stops after 3 consecutive failures.

Examples:

Run once:

```bash
notify command --name logs --prefix 'build: ' 'make -s version'
```

Poll repeatedly until the command starts failing consistently:

```bash
notify command --name logs --recursive 'date'
```

This is useful for simple polling loops, repeated checks, or producers that should keep feeding notifications into an instance until the source command becomes unstable or unavailable.

### `notify list`

List running instances and any pending command requests.

```bash
notify list
```

Example output:

```text
demo
  - [1] sleep 2; printf done
  - [2] watch-something (recursive)
```

### `notify stop`

Stop an entire instance:

```bash
notify stop --name demo
```

Or stop just one pending command by id:

```bash
notify stop --name demo --command 2
```

If exactly one instance is running, `--name` can be omitted.

## Examples

### Print every notification

```bash
notify start --name printer 'cat'
```

Then:

```bash
echo 'one' | notify send --name printer
echo 'two' | notify send --name printer
```

### Restart long-running work when newer input arrives

```bash
notify start --name latest --restart 'your-command-that-reads-stdin'
```

Now each new notification replaces the current run instead of waiting for the queue to drain.

### Turn command output into notifications

```bash
notify start --name inbox 'cat'
notify command --name inbox 'git rev-parse --short HEAD'
```

### Work with a single unnamed instance

```bash
notify start 'cat'
```

This prints the generated instance name. If it is the only running instance, later commands can omit `--name`:

```bash
echo 'hello' | notify send
notify list
notify stop
```

## Common workflows

- Forward command output into another local script
- Trigger local shell automation from piped text
- Keep a simple queue of text-driven jobs
- Replace a running job with newer input using `--restart`
- Schedule or loop producers that feed a single consumer instance
- Use `notify command --recursive` for simple polling or repeated command-driven notifications

## Notes

- `notify send` reads from stdin
- `notify command` uses the command's stdout as the notification payload
- Large payloads are truncated for delivery and the full contents are saved to a temporary file path included in the message

## Help

```bash
notify --help
notify start --help
notify send --help
notify command --help
notify list --help
notify stop --help
```
