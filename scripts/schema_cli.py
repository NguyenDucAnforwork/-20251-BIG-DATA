CREATE KEYSPACE IF NOT EXISTS payments WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};
CREATE TABLE IF NOT EXISTS payments.agg_5m_by_merchant (
merchant_id text,
status text,
window_start timestamp,
window_end timestamp,
cnt bigint,
sum_amount double,
n_users bigint,
PRIMARY KEY ((merchant_id), window_start, status)
) WITH CLUSTERING ORDER BY (window_start ASC);