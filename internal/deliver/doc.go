// Package deliver describes one outbound attempt.
//
// [ClassifyPost] turns a transport error or an HTTP status into an outcome and
// a failure class. [ClassifyLocal] treats a blocked destination and a missing
// or empty DNS result as non-retryable, and a response past the body limit as
// retryable. [HTTP] is the POST the worker calls. The package does not store
// the job.
package deliver
