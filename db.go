package cassandraschema

import (
	"github.com/gocql/gocql"
)

func GetKeyspaces(hosts string, username string, password string) []CassandraKeyspace {
	var cassieKeySpaces []CassandraKeyspace

	session := GetSession(hosts, username, password)
	defer session.Close()

	iter := session.Query(`SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces;`).Iter()

	var name string
	var writes bool
	var replication map[string]string

	for iter.Scan(&name, &writes, &replication){
		keySpace := CassandraKeyspace{
			Name:          name,
			DurableWrites: writes,
			Replication:   replication,
		}
		cassieKeySpaces = append(cassieKeySpaces, keySpace)
	}
	errorTrapIterable(iter)
	return cassieKeySpaces
}

func GetTables(hosts string, username string, password string) []CassandraTable {
	var cassieTables []CassandraTable

	session := GetSession(hosts, username, password)
	defer session.Close()

	iter := session.Query(`SELECT id, keyspace_name, table_name FROM system_schema.tables;`).Iter()
	var id gocql.UUID
	var keySpaceName string
	var tableName string

	for iter.Scan(&id, &keySpaceName, &tableName) {
		table := CassandraTable{
			Id:           id,
			Name:         tableName,
			KeyspaceName: keySpaceName,
		}
		cassieTables = append(cassieTables, table)
	}
	errorTrapIterable(iter)
	return cassieTables
}

func GetColumns(hosts string, username string, password string) []CassandraColumn {
	var cassieColumns []CassandraColumn

	session := GetSession(hosts, username, password)
	defer session.Close()

	iter := session.Query(`SELECT keyspace_name, table_name, column_name, clustering_order, type, kind, position FROM system_schema.columns`).Iter()

	var columnKeyspace, columnTable, columnName, clusteringOrder, columnType, columnKind, columnPosition string

	for iter.Scan(&columnKeyspace, &columnTable, &columnName, &clusteringOrder, &columnType, &columnKind, &columnPosition) {
		column := CassandraColumn{
			Name:         columnName,
			DataType:     columnType,
			Kind:         columnKind,
			ClusterOrder: clusteringOrder,
			Position:     columnPosition,
			Keyspace:     columnKeyspace,
			Table:        columnTable,
		}
		cassieColumns = append(cassieColumns, column)
	}
	errorTrapIterable(iter)

	return cassieColumns
}


