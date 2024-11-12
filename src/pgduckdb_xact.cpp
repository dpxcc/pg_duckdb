#include "duckdb/common/exception.hpp"
#include "pgduckdb/pgduckdb_duckdb.hpp"
#include "pgduckdb/pgduckdb_utils.hpp"

extern "C" {
#include "postgres.h"
#include "access/xact.h" // RegisterXactCallback, XactEvent, SubXactEvent, SubTransactionId
#include "access/xlog.h" // XactLastRecEnd
}

namespace pgduckdb {

bool
PostgresDidWrites() {
	return XactLastRecEnd != InvalidXLogRecPtr;
}

static void
DuckdbXactCallback_Cpp(XactEvent event, void *arg) {
	auto connection = DuckDBManager::GetConnectionUnsafe();
	auto &context = *connection->context;
	if (!context.transaction.HasActiveTransaction()) {
		return;
	}

	switch (event) {
	case XACT_EVENT_PRE_COMMIT:
	case XACT_EVENT_PARALLEL_PRE_COMMIT:
		if (IsInTransactionBlock(true)) {
			if (PostgresDidWrites() && DuckdbDidWrites(context)) {
				throw duckdb::NotImplementedException(
				    "Writing to DuckDB and Postgres tables in the same transaction block is not supported");
			}
		}
		// Commit the DuckDB transaction too
		context.transaction.Commit();
		break;

	case XACT_EVENT_ABORT:
	case XACT_EVENT_PARALLEL_ABORT:
		// Abort the DuckDB transaction too
		context.transaction.Rollback(nullptr);
		break;

	case XACT_EVENT_PREPARE:
	case XACT_EVENT_PRE_PREPARE:
		// Throw an error for prepare events
		throw duckdb::NotImplementedException("Prepared transactions are not implemented in DuckDB.");

	case XACT_EVENT_COMMIT:
	case XACT_EVENT_PARALLEL_COMMIT:
		// No action needed for commit event, we already did committed the
		// DuckDB transaction in the PRE_COMMIT event. We don't commit the
		// DuckDB transaction here, because any failure to commit would
		// then turn into a Postgres PANIC (i.e. a crash). To quote the
		// relevant Postgres comment:
		// > Note that if an error is raised here, it's too late to abort
		// > the transaction. This should be just noncritical resource
		// > releasing.
		break;

	default:
		// Fail hard if future PG versions introduce a new event
		throw duckdb::NotImplementedException("Not implemented XactEvent: %d", event);
	}
}

static void
DuckdbXactCallback(XactEvent event, void *arg) {
	InvokeCPPFunc(DuckdbXactCallback_Cpp, event, arg);
}

/*
 * Throws an error when starting a new subtransaction in a DuckDB transaction.
 * Existing subtransactions are handled at creation of the DuckDB connection.
 * Throwing here for every event type is problematic, because that would also
 * cause a failure in the resulting sovepoint abort event. Which in turn would
 * cause the postgres error stack to overflow.
 */
static void
DuckdbSubXactCallback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void *arg) {
	auto connection = DuckDBManager::GetConnectionUnsafe();
	auto &context = *connection->context;
	if (!context.transaction.HasActiveTransaction()) {
		return;
	}

	if (event == SUBXACT_EVENT_START_SUB) {
		elog(ERROR, "SAVEPOINT is not supported in DuckDB");
	}
}

static bool transaction_handler_configured = false;
void
RegisterDuckdbXactCallback() {
	if (transaction_handler_configured) {
		return;
	}
	PostgresFunctionGuard(RegisterXactCallback, DuckdbXactCallback, nullptr);
	PostgresFunctionGuard(RegisterSubXactCallback, DuckdbSubXactCallback, nullptr);
	transaction_handler_configured = true;
}

void
UnregisterDuckdbXactCallback() {
	if (!transaction_handler_configured) {
		return;
	}
	PostgresFunctionGuard(UnregisterXactCallback, DuckdbXactCallback, nullptr);
	PostgresFunctionGuard(UnregisterSubXactCallback, DuckdbSubXactCallback, nullptr);
	transaction_handler_configured = false;
}

bool
IsInTransactionBlock(bool is_top_level = true) {
	return PostgresFunctionGuard(::IsInTransactionBlock, is_top_level);
}
} // namespace pgduckdb
