package utils

import (
	"bytes"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var initLogger sync.Once
var logger *zap.SugaredLogger

func Logger() *zap.SugaredLogger {
	return logger
}

func InitLogger(filename string) {
	initLogger.Do(func() { logger = NewLogger(filename) })
}

const ARCHIVAL_LOG_PATH = "./"

func NewLogger(filename string) *zap.SugaredLogger {
	os.MkdirAll(ARCHIVAL_LOG_PATH, 0755)
	cfg := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(zapcore.DebugLevel),
		OutputPaths:      []string{ARCHIVAL_LOG_PATH + filename},
		ErrorOutputPaths: []string{ARCHIVAL_LOG_PATH + filename},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey: "message",

			TimeKey:    "timestamp",
			EncodeTime: zapcore.ISO8601TimeEncoder,

			LevelKey:    "level",
			EncodeLevel: zapcore.CapitalLevelEncoder,

			CallerKey:    "context",
			EncodeCaller: zapcore.FullCallerEncoder,
		},
	}
	logger, _ := cfg.Build()
	return logger.Sugar()
}

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
