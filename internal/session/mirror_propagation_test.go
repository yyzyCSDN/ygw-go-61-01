package session

import "errors"

// errBatchMirrorDown is the canned error used by the mirror-propagation tests
// to drive a failing MirrorSink.  It is declared here so both mirror_fail_test
// and mirror_renew_test can reference it without coupling the fixtures.
var errBatchMirrorDown = errors.New("mirror down (batch)")
