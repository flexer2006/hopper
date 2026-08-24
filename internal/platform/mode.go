package platform

import (
	"fmt"
)

const (
	ModeAPI    = "api"
	ModeWorker = "worker"
	Usage      = "Usage: hopper <api|worker>\n" +
		"  api     HTTP ingress process\n" +
		"  worker  broker consumer process\n"
)

func ParseMode(args []string) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("%w: missing mode", ErrInvalidMode)
	}

	switch args[0] {
	case "-h", "--help", "help":
		return "", ErrHelp
	case ModeAPI, ModeWorker:
		if len(args) > 1 {
			return "", fmt.Errorf("%w: unexpected extra arguments", ErrInvalidMode)
		}

		return args[0], nil
	default:
		return "", fmt.Errorf("%w: %q (want api or worker)", ErrInvalidMode, args[0])
	}
}
