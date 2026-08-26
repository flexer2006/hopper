package persist

import "go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

func DurableWriteConcern() *writeconcern.WriteConcern {
	journal := true

	wc := new(writeconcern.WriteConcern)
	wc.W = writeconcern.WCMajority
	wc.Journal = &journal

	return wc
}
