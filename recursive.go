package main

func runCommandRequestLoop(req commandRequest, emit func(notification)) {
	consecutiveFailures := 0
	for {
		result := runShellCapture(req.command)
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
