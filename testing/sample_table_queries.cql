CREATE KEYSPACE IF NOT EXISTS keyspace_mcspacey
    WITH REPLICATION = {
        'class': 'NetworkTopologyStrategy',
        'datacenter1' : 1
        };

CREATE TABLE IF NOT EXISTS keyspace_mcspacey.keyFACE (
     pkColumn uuid PRIMARY KEY,
     someData text
);

SELECT pkColumn, somedata FROM keyspace_mcspacey.keyFACE;

DROP KEYSPACE keyspace_mcspacey;