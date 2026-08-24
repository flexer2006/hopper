package persist

import (
	"net/url"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func ClientOptions(uri string) *options.ClientOptions {
	return options.Client().ApplyURI(uri).SetWriteConcern(DurableWriteConcern())
}

func URIHasReplicaSet(uri string) bool {
	parsed, err := url.Parse(uri)
	if err != nil {
		return false
	}

	return parsed.Query().Get("replicaSet") != ""
}

func SetNameFromHello(hello bson.M) (string, error) {
	name, ok := hello["setName"].(string)
	if !ok || name == "" {
		return "", ErrStandalone
	}

	return name, nil
}
