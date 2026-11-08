#pragma once
#include "asterorm/core/error.hpp"
#include <libpq-fe.h>

namespace asterorm::pg::diagnostics {
inline db_error make_error_from_pgresult(PGconn* conn, PGresult* res, db_error_kind fallback) {
    db_error err;
    if (!res) {
        err.kind = fallback;
        if (conn) {
            err.message = PQerrorMessage(conn);
        }
        if (err.message.empty()) {
            err.message = "PostgreSQL operation failed without a result";
        }
        return err;
    }

    const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
    if (sqlstate) {
        err.sqlstate = sqlstate;
    }
    err.kind = err.sqlstate.empty() ? fallback : classify_sqlstate(err.sqlstate);

    const char* msg = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
    if (msg) {
        err.message = msg;
    } else if (conn) {
        err.message = PQerrorMessage(conn);
    }

    const char* detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
    if (detail) {
        err.detail = detail;
    }

    const char* hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
    if (hint) {
        err.hint = hint;
    }

    const char* table = PQresultErrorField(res, PG_DIAG_TABLE_NAME);
    if (table) {
        err.table = table;
    }

    const char* col = PQresultErrorField(res, PG_DIAG_COLUMN_NAME);
    if (col) {
        err.column = col;
    }

    const char* constraint = PQresultErrorField(res, PG_DIAG_CONSTRAINT_NAME);
    if (constraint) {
        err.constraint = constraint;
    }

    return err;
}

} // namespace asterorm::pg::diagnostics
