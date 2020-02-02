package cassandraschema

import (
	"github.com/gocql/gocql"
	"log"
)

func GetSession(hosts string, username string, password string) *gocql.Session {
	cluster := gocql.NewCluster(hosts)
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
	cluster.Keyspace = "system_schema"
	session, _ := cluster.CreateSession()
	return session
}

func errorTrapIterable(iter *gocql.Iter) {
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}

func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

