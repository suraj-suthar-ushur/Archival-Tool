package utils

import (
	"bytes"
	"os/exec"
	"strconv"
	"time"
)

func CompressedTimeFormat(timeparam time.Time) string {
	curr_time := timeparam

	name := strconv.Itoa(curr_time.Year())
	if int(curr_time.Month()) < 10 {
		name += "0" + strconv.Itoa(int(curr_time.Month()))
	} else {
		name += strconv.Itoa(int(curr_time.Month()))
	}
	if int(curr_time.Day()) < 10 {
		name += "0" + strconv.Itoa(int(curr_time.Day()))
	} else {
		name += strconv.Itoa(int(curr_time.Day()))
	}
	if int(curr_time.Hour()) < 10 {
		name += "0" + strconv.Itoa(int(curr_time.Hour()))
	} else {
		name += strconv.Itoa(int(curr_time.Hour()))
	}
	if int(curr_time.Minute()) < 10 {
		name += "0" + strconv.Itoa(int(curr_time.Minute()))
	} else {
		name += strconv.Itoa(int(curr_time.Minute()))
	}
	if int(curr_time.Second()) < 10 {
		name += "0" + strconv.Itoa(int(curr_time.Second()))
	} else {
		name += strconv.Itoa(int(curr_time.Second()))
	}
	return name
}

func Shellout(command string) (string, string, error) {
	CMD_SHELL := "bash"
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command(CMD_SHELL, "-c", command)

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}
