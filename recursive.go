package main

import "context"

func runCommandRequestLoop(ctx context.Context, req commandRequest, emit func(notification)) {
	consecutiveFailures := 0
	for {
		if ctx.Err() != nil {
			return
		}

		result := runShellCaptureContext(ctx, req.command)
		if ctx.Err() != nil || result.canceled {
			return
		}

		payload, err := preparePayload(req.prefix, result.stdout)
		if err == nil {
			emit(notification{stdin: payload, forwardedStderr: result.stderr})
		}

		if result.err != nil {
			consecutiveFailures++
			if !req.recursive || consecutiveFailures >= 3 {
				return
			}
			continue
		}

		consecutiveFailures = 0
		if !req.recursive {
			return
		}
	}
}
