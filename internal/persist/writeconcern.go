package persist

import "go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

func DurableWriteConcern() *writeconcern.WriteConcern {
	journal := true

	return &writeconcern.WriteConcern{
		W:       writeconcern.WCMajority,
		Journal: &journal,
	}
}
