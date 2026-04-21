package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func executeCLI(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		writeRootHelp(stdout)
		return nil
	}

	switch args[0] {
	case "--help", "-h", "help":
		writeRootHelp(stdout)
		return nil
	case "start":
		return runStart(args[1:], stdout, stderr)
	case "stop":
		return runStop(args[1:], stdout)
	case "send":
		return runSend(args[1:], stdin, stdout)
	case "command":
		return runCommand(args[1:], stdout)
	case "list":
		return runList(args[1:], stdout)
	default:
		return usageError()
	}
}

func runStart(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	restart := fs.Bool("restart", false, "kill running command when a new notification arrives")
	name := fs.String("name", "", "instance name")
	showHelp := fs.Bool("help", false, "show help")
	fs.BoolVar(showHelp, "h", false, "show help")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showHelp {
		writeStartHelp(stdout)
		return nil
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: notify start [--name <name>] [--restart] \"<command>\"")
	}
	resolvedName := *name
	if resolvedName == "" {
		var err error
		resolvedName, err = randomInstanceName()
		if err != nil {
			return err
		}
	}
	if err := validateName(resolvedName); err != nil {
		return err
	}

	server := &server{
		name:    resolvedName,
		command: fs.Arg(0),
		restart: *restart,
		stdout:  stdout,
		stderr:  stderr,
	}
	listener, err := prepareListener(resolvedName)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(stdout, resolvedName)
	return server.runWithListener(listener)
}

func runStop(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("stop", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	name := fs.String("name", "", "instance name")
	showHelp := fs.Bool("help", false, "show help")
	fs.BoolVar(showHelp, "h", false, "show help")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showHelp {
		writeStopHelp(stdout)
		return nil
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: notify stop [--name <name>]")
	}
	resolvedName, err := resolveTargetName(*name)
	if err != nil {
		return err
	}
	return sendStop(resolvedName)
}

func runSend(args []string, stdin io.Reader, stdout io.Writer) error {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	name := fs.String("name", "", "instance name")
	prefix := fs.String("prefix", "", "literal prefix to prepend to notification")
	showHelp := fs.Bool("help", false, "show help")
	fs.BoolVar(showHelp, "h", false, "show help")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showHelp {
		writeSendHelp(stdout)
		return nil
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected trailing arguments: %s", strings.Join(fs.Args(), " "))
	}
	resolvedName, err := resolveTargetName(*name)
	if err != nil {
		return err
	}

	body, err := io.ReadAll(stdin)
	if err != nil {
		return err
	}
	payload, err := preparePayload(*prefix, body)
	if err != nil {
		return err
	}
	return sendNotification(resolvedName, notification{stdin: payload})
}

func runCommand(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("command", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	name := fs.String("name", "", "instance name")
	prefix := fs.String("prefix", "", "literal prefix to prepend to notification")
	recursive := fs.Bool("recursive", false, "repeat command until 3 consecutive failures")
	showHelp := fs.Bool("help", false, "show help")
	fs.BoolVar(showHelp, "h", false, "show help")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showHelp {
		writeCommandHelp(stdout)
		return nil
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: notify command [--name <name>] [--recursive] [--prefix \"<text>\"] \"<cmd>\"")
	}
	resolvedName, err := resolveTargetName(*name)
	if err != nil {
		return err
	}
	return sendCommandRequest(resolvedName, commandRequest{
		command:   fs.Arg(0),
		prefix:    *prefix,
		recursive: *recursive,
	})
}

func runList(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	showHelp := fs.Bool("help", false, "show help")
	fs.BoolVar(showHelp, "h", false, "show help")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *showHelp {
		writeListHelp(stdout)
		return nil
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: notify list")
	}
	instances, err := listLiveInstances()
	if err != nil {
		return err
	}
	for _, name := range instances {
		_, _ = fmt.Fprintln(stdout, name)
	}
	return nil
}

func usageError() error {
	return errors.New("usage: notify <start|stop|send|command|list> ...")
}

func writeRootHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "notify - react to notifications with shell commands")
	_, _ = fmt.Fprintln(w, "")
	_, _ = fmt.Fprintln(w, "Usage:")
	_, _ = fmt.Fprintln(w, "  notify start [--name <name>] [--restart] \"<command>\"")
	_, _ = fmt.Fprintln(w, "  notify stop [--name <name>]")
	_, _ = fmt.Fprintln(w, "  notify send [--name <name>] [--prefix \"<text>\"]")
	_, _ = fmt.Fprintln(w, "  notify command [--name <name>] [--recursive] [--prefix \"<text>\"] \"<cmd>\"")
	_, _ = fmt.Fprintln(w, "  notify list")
}

func writeStartHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Usage:")
	_, _ = fmt.Fprintln(w, "  notify start [--name <name>] [--restart] \"<command>\"")
	_, _ = fmt.Fprintln(w, "")
	_, _ = fmt.Fprintln(w, "Options:")
	_, _ = fmt.Fprintln(w, "  --name <name>  set the instance name; otherwise a random name is generated")
	_, _ = fmt.Fprintln(w, "  --restart      kill the running command when a new notification arrives")
}

func writeStopHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Usage:")
	_, _ = fmt.Fprintln(w, "  notify stop [--name <name>]")
	_, _ = fmt.Fprintln(w, "")
	_, _ = fmt.Fprintln(w, "If --name is omitted and exactly one instance is running, that instance is used.")
}

func writeListHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Usage:")
	_, _ = fmt.Fprintln(w, "  notify list")
	_, _ = fmt.Fprintln(w, "")
	_, _ = fmt.Fprintln(w, "Print all running instance names, one per line.")
}

func writeSendHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Usage:")
	_, _ = fmt.Fprintln(w, "  notify send [--name <name>] [--prefix \"<text>\"]")
	_, _ = fmt.Fprintln(w, "")
	_, _ = fmt.Fprintln(w, "Options:")
	_, _ = fmt.Fprintln(w, "  --name <name>      target a specific instance; optional if exactly one instance is running")
	_, _ = fmt.Fprintln(w, "  --prefix <text>    prepend literal text to the notification payload")
}

func writeCommandHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Usage:")
	_, _ = fmt.Fprintln(w, "  notify command [--name <name>] [--recursive] [--prefix \"<text>\"] \"<cmd>\"")
	_, _ = fmt.Fprintln(w, "")
	_, _ = fmt.Fprintln(w, "Options:")
	_, _ = fmt.Fprintln(w, "  --name <name>      target a specific instance; optional if exactly one instance is running")
	_, _ = fmt.Fprintln(w, "  --prefix <text>    prepend literal text to the notification payload")
	_, _ = fmt.Fprintln(w, "  --recursive        repeat the command until 3 consecutive failures")
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("instance name must not be empty")
	}
	if strings.Contains(name, "/") || strings.Contains(name, string(os.PathSeparator)) {
		return fmt.Errorf("instance name must not contain path separators")
	}
	return nil
}
