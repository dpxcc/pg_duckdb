namespace pgduckdb {
bool PostgresDidWrites();
void RegisterDuckdbXactCallback();
void UnregisterDuckdbXactCallback();
bool IsInTransactionBlock(bool top_level = true);
} // namespace pgduckdb
