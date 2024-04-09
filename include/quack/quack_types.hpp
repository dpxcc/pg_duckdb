#pragma once

#include "duckdb.hpp"

extern "C" {
#include "postgres.h"
#include "executor/tuptable.h"
}

namespace quack {
duckdb::LogicalType ConvertPostgresToDuckColumnType(Oid type);
void ConvertPostgresToDuckValue(Datum value, duckdb::Vector &result, idx_t offset);
void ConvertDuckToPostgresValue(TupleTableSlot *slot, duckdb::Value &value, idx_t col);
} // namespace quack