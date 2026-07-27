package goflush

import (
	"errors"
)

// Drop is a sentinel error returned by MapErr and MuxErr callbacks to silently
// discard an event without emitting a value or propagating an error downstream.
var Drop = errors.New("dropped event")
