package main

import (
	"fmt"
	"os"
)

const maxNotificationSize = 50 * 1024

func preparePayload(prefix string, body []byte) ([]byte, error) {
	full := append([]byte(prefix), body...)
	if len(full) <= maxNotificationSize {
		return full, nil
	}

	file, err := os.CreateTemp(os.TempDir(), "notify-*.log")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if _, err := file.Write(full); err != nil {
		return nil, err
	}

	notice := fmt.Sprintf("Notification over 50kb and truncated. View the entire notification here: %s", file.Name())
	truncated := append([]byte(nil), full[:maxNotificationSize]...)
	truncated = append(truncated, []byte(notice)...)
	return truncated, nil
}
