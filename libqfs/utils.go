// Copyright (c) 2018 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package libqfs

// This package will be included in the static library for libqfs, and so should be
// as self contained as possible to minimize the final file size

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

const ApiPath = "api"

const InodeIdApi = 2

func fileIsApi(stat os.FileInfo) bool {
	stat_t, ok := stat.Sys().(*syscall.Stat_t)
	if ok && stat_t.Ino == InodeIdApi {
		// No real filesystem is likely to give out inode 2
		// for a random file but quantumfs reserves that
		// inode for all the api files.
		return true
	}

	return false
}

func findApiPathEnvironment() string {
	path := os.Getenv("QUANTUMFS_API_PATH")
	if path == "" {
		return ""
	}

	if !strings.HasSuffix(path, fmt.Sprintf("%c%s", os.PathSeparator, ApiPath)) {
		return ""
	}

	stat, err := os.Lstat(path)
	if err != nil {
		return ""
	}

	if !fileIsApi(stat) {
		return ""
	}

	return path
}

// Returns the path where QuantumFS is mounted at or "" on failure.
func FindQuantumFsMountPath() string {
	// We are look in /proc/self/mountinfo for a line which indicates that
	// QuantumFS is mounted. That line looks like:
	//
	// 138 30 0:32 / /mnt/quantumfs rw,relatime - fuse.QuantumFS QuantumFS ...
	//
	// Where the number after the colon (0:32) is the FUSE connection number.
	// Since it's possible for any filesystem to me bind-mounted to multiple
	// locations we use that number to discriminate between multiple QuantumFS
	// mounts.

	mountfile, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return ""
	}

	mountinfo := bufio.NewReader(mountfile)

	var path string
	connectionId := int64(-1)

	for {
		bline, _, err := mountinfo.ReadLine()
		if err != nil {
			break
		}

		line := string(bline)

		if !strings.Contains(line, "fuse.QuantumFS") {
			continue
		}

		fields := strings.SplitN(line, " ", 6)
		connectionS := strings.Split(fields[2], ":")[1]
		connectionI, err := strconv.ParseInt(connectionS, 10, 64)
		if err != nil {
			// We cannot parse this line, but we also know we have a
			// QuantumFS mount. Play it safe and fail searching for a
			// mount.
			return ""
		}
		path = fields[4]

		if connectionId != -1 && connectionId != connectionI {
			// We have a previous QuantumFS mount which doesn't match
			// this one, thus we have more than one mount. Give up.
			return ""
		}

		connectionId = connectionI
	}

	if connectionId == -1 {
		// We didn't find a mount
		return ""
	}

	// We've found precisely one mount, ensure it contains the api file.
	apiPath := fmt.Sprintf("%s%c%s", path, os.PathSeparator, ApiPath)
	stat, err := os.Lstat(apiPath)
	if err != nil || !fileIsApi(stat) {
		return ""
	}

	return path
}

func findApiPathMount() string {
	mountPath := FindQuantumFsMountPath()
	if mountPath == "" {
		return ""
	}
	return fmt.Sprintf("%s%c%s", mountPath, os.PathSeparator, ApiPath)
}

func findApiPathUpwards() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	directories := strings.Split(cwd, "/")
	path := ""

	for {
		path = strings.Join(directories, "/") + "/" + ApiPath
		stat, _ := os.Lstat(path)
		if stat != nil && !stat.IsDir() {
			if fileIsApi(stat) {
				return path, nil
			}
		}

		// We need to keep iterating up regardless of whether Lstat errors
		if len(directories) == 1 {
			// We didn't find anything and hit the root, give up
			return "", fmt.Errorf("Couldn't find api file")
		}
		directories = directories[:len(directories)-1]
	}
}

func FindApiPath() (string, error) {
	path := findApiPathEnvironment()
	if path != "" {
		return path, nil
	}

	path = findApiPathMount()
	if path != "" {
		return path, nil
	}

	path, err := findApiPathUpwards()
	if err != nil {
		return "", err
	}

	if path != "" {
		return path, nil
	}

	// Give up
	return "", fmt.Errorf("Unable to find API file")
}
