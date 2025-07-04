package internal

import (
	"os"
	"strconv"
)

// IsTestContainersEnabled returns a bool value indicating if test containers are
// enabled allowing integration tests to be executed.
//
// IsTestContainersEnabled checks for the presence of the environment variable
// `TESTCONTAINERS_ENABLED`. If the environment variable is present and has a
// value of `1`, `t`, `T`, `true`, `True`, or `TRUE` test containers is considered
// available and enabled.
func IsTestContainersEnabled() bool {
	enabled, _ := strconv.ParseBool(os.Getenv("TESTCONTAINERS_ENABLED"))
	return enabled
}
