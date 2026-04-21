package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func randomInstanceName() (string, error) {
	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func listLiveInstances() ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(socketDir(), "*.sock"))
	if err != nil {
		return nil, err
	}

	instances := make([]string, 0, len(matches))
	for _, path := range matches {
		name, ok := instanceNameFromSocketPath(path)
		if !ok {
			continue
		}
		conn, err := net.Dial("unix", path)
		if err != nil {
			_ = os.Remove(path)
			continue
		}
		_ = conn.Close()
		instances = append(instances, name)
	}
	sort.Strings(instances)
	return instances, nil
}

func listInstanceStatuses() ([]instanceStatus, error) {
	instances, err := listLiveInstances()
	if err != nil {
		return nil, err
	}

	statuses := make([]instanceStatus, 0, len(instances))
	for _, name := range instances {
		status, err := queryInstanceStatus(name)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func resolveTargetName(explicit string) (string, error) {
	if explicit != "" {
		if err := validateName(explicit); err != nil {
			return "", err
		}
		return explicit, nil
	}

	instances, err := listLiveInstances()
	if err != nil {
		return "", err
	}
	switch len(instances) {
	case 0:
		return "", fmt.Errorf("no running notify instances found; use --name or start an instance")
	case 1:
		return instances[0], nil
	default:
		return "", fmt.Errorf("multiple notify instances found (%s); use --name", strings.Join(instances, ", "))
	}
}

func instanceNameFromSocketPath(path string) (string, bool) {
	base := filepath.Base(path)
	if !strings.HasSuffix(base, ".sock") {
		return "", false
	}
	name := strings.TrimSuffix(base, ".sock")
	if name == "" {
		return "", false
	}
	return name, true
}
