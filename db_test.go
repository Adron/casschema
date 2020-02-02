package cassandraschema

import (
	"log"
	"testing"
)

func detailsForTests()AuthDetails {
	return AuthDetails{
		Hosts:    "localhost",
		Username: "cassandra",
		Password: "cassandra",
	}
}

func TestBuildClusterSchemaIgnoresTables(t *testing.T) {
	var ignoreList []string
	ignoreList = append(ignoreList, "system_schema")
	ignoreList = append(ignoreList, "system_traces")
	ignoreList = append(ignoreList, "system_auth")
	ignoreList = append(ignoreList, "system_distributed")
	ignoreList = append(ignoreList, "system")
	databaseCluster := BuildClusterSchema(detailsForTests(),ignoreList)
	foundIgnoreKeyspace := false

	for _, keySpace := range databaseCluster.Keyspaces {
		for _, ignoreName := range ignoreList {
			if keySpace.Name == ignoreName {
				foundIgnoreKeyspace = true
			}
		}
	}

	if foundIgnoreKeyspace {
		t.Errorf("It wasn't ignored, something is amiss, fix the processing?")
	}
}

func TestBuildClusterSchemaIgnoresTable(t *testing.T) {
	var ignoreList []string
	ignoreList = append(ignoreList, "system_schema")
	databaseCluster := BuildClusterSchema(detailsForTests(),ignoreList)
	foundIgnoreKeyspace := false

	for _, keySpace := range databaseCluster.Keyspaces {
		for _, ignoreName := range ignoreList {
			if keySpace.Name == ignoreName {
				foundIgnoreKeyspace = true
			}
		}
	}

	if foundIgnoreKeyspace {
		t.Errorf("It wasn't ignored, something is amiss, fix the processing?")
	}
}

func TestBuildClusterSchema(t *testing.T) {
	var ignoreList []string
	databaseCluster := BuildClusterSchema(detailsForTests(),ignoreList)
	if len(databaseCluster.Keyspaces) == 0 || len(databaseCluster.Keyspaces[0].CassandraTables) == 0 {
		t.Errorf("Well, something is amiss, did you setup the testing database?")
	}
}

func TestGetColumns(t *testing.T) {
	columns := GetColumns(detailsForTests())
	if 1 > len(columns){
		t.Errorf("No columns found, yup, this is terribly wrong too! Is there even a databass?")
	}
}

func TestGetTables(t *testing.T) {
	tables := GetTables(detailsForTests())
	if 1 > len(tables) {
		t.Errorf("No tables found, something is terribly wrong!")
	}
}

func TestGetKeyspaces(t *testing.T) {
	keySpaces := GetKeyspaces(detailsForTests())
	if 1 > len(keySpaces) {
		t.Errorf("No Keyspaces found or retrieval failed.")
	}
}

func TestGetSession(t *testing.T) {
	expectedSession := GetSession(detailsForTests().Hosts, detailsForTests().Username, detailsForTests().Password)
	defer expectedSession.Close()
	iter := expectedSession.Query(`SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces;`).Iter()
	var keyspaceName string
	var dureableWrites bool
	var replication map[string]string
	var cassieKeyspaces []CassandraKeyspace

	for iter.Scan(&keyspaceName, &dureableWrites, &replication){
		keySpace := CassandraKeyspace{
			Name:          keyspaceName,
			DurableWrites: dureableWrites,
			Replication:   replication,
		}
		cassieKeyspaces = append(cassieKeyspaces, keySpace)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	success := false
	count := 0
	for _, v := range cassieKeyspaces {
		if v.Name == "system_schema" || v.Name == "system_auth" || v.Name == "system_distributed" || v.Name == "system" || v.Name == "system_traces" {
			count = count + 1
		}
	}
	if count == 5 {
		success = true
	}
	if success != true {
		t.Errorf("Failed, incorrect number of system keyspaces. %v found. Success set to %v.", count, success)
	}
}
