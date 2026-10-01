// Package platform loads process configuration and runs one Fx graph.
//
// [Load] reads YAML from HOPPER_CONFIG_FILE. A non-empty environment variable
// overrides the same YAML value. Load rejects a token shorter than 32 bytes
// and a claim lease below [Config.AttemptBudget]. [ParseMode] accepts only api
// or worker. [NewLogger] writes JSON to stdout and omits stack traces unless
// LogStackTraces is set. [RunProcess] starts the graph, waits for a signal,
// and stops within the configured timeout.
package platform
