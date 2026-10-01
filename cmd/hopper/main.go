package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/flexer2006/hopper/internal/fxx/fapi"
	"github.com/flexer2006/hopper/internal/fxx/fworker"
	"github.com/flexer2006/hopper/internal/platform"
)

const (
	exitFail = 1
	exitIO   = 2
)

func main() {
	err := run(os.Args[1:])
	if err != nil {
		_, writeErr := os.Stderr.WriteString("hopper: " + err.Error() + "\n")
		if writeErr != nil {
			os.Exit(exitIO)
		}

		os.Exit(exitFail)
	}
}

func run(args []string) error {
	mode, err := platform.ParseMode(args)
	if errors.Is(err, platform.ErrHelp) {
		_, writeErr := os.Stderr.WriteString(platform.Usage)
		if writeErr != nil {
			return fmt.Errorf("write usage: %w", writeErr)
		}

		return nil
	}

	if err != nil {
		return err
	}

	switch mode {
	case platform.ModeAPI:
		return fapi.Run()

	case platform.ModeWorker:
		return fworker.Run()

	default:
		return platform.ErrInvalidMode
	}
}
