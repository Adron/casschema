package cassandraschema

import "github.com/gocql/gocql"

type CassandraCluster struct {
	Keyspaces []CassandraKeyspace
}

type CassandraKeyspace struct {
	Name          string
	DurableWrites bool
	Replication   map[string]string
	CassandraTables []CassandraTable
}

type CassandraColumn struct {
	Name string
	DataType string
	Kind string
	ClusterOrder string
	Position string
	Keyspace string
	Table string
}

type CassandraTable struct {
	Id gocql.UUID
	Name string
	KeyspaceName string
	CassandraColumns []CassandraColumn
}

type AuthDetails struct {
	Hosts    string
	Username string
	Password string
}
