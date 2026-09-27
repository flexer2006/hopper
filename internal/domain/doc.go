// Package domain holds the Job aggregate and the rules for changing its status.
//
// Status changes mutate the job in memory. They do not persist it, publish it,
// or decide whether a delivery is due. [Job.Claim] applies the delivery-start
// cap. [Job.RecordFailure] returns the next status and queue. [AdmitTarget]
// checks the URL and, for a literal IP, [AddrDenied]. Hostnames are not resolved.
package domain
