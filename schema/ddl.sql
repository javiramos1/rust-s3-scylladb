CREATE KEYSPACE IF NOT EXISTS graph WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
CREATE TABLE IF NOT EXISTS graph.nodes (
   id uuid,
   ingestion_id text,
   name text,
   url text,
   item_type text,
   direction text,
   relation text,
   relates_to text,
   tags list<frozen<tuple<text,text>>>,
   PRIMARY KEY (id, direction, relation, relates_to)
) WITH comment = 'Nodes Table' AND caching = {'enabled': 'true'} 
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND CLUSTERING ORDER BY (direction ASC, relation ASC, relates_to DESC);