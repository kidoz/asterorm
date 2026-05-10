#include "asterorm/sqlite/driver.hpp"

#include <sqlite3.h>

#include "asterorm/core/error.hpp"

namespace asterorm::sqlite {

asterorm::result<connection> driver::connect(const std::string& conninfo) const {
    sqlite3* db = nullptr;
    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI;
    int rc = sqlite3_open_v2(conninfo.c_str(), &db, flags, nullptr);

    if (rc != SQLITE_OK) {
        db_error err;
        err.kind = db_error_kind::connection_failed;
        err.sqlstate = std::to_string(rc);
        err.message = db ? sqlite3_errmsg(db) : sqlite3_errstr(rc);
        if (db) {
            sqlite3_close_v2(db);
        }
        return std::unexpected(err);
    }

    // Sensible defaults: enforce foreign keys and wait briefly on locks so
    // pooled callers do not see SQLITE_BUSY on transient writer contention.
    sqlite3_busy_timeout(db, 5000);
    char* errmsg = nullptr;
    if (sqlite3_exec(db, "PRAGMA foreign_keys = ON;", nullptr, nullptr, &errmsg) != SQLITE_OK) {
        db_error err;
        err.kind = db_error_kind::connection_failed;
        err.message = errmsg ? errmsg : "failed to enable foreign keys";
        sqlite3_free(errmsg);
        sqlite3_close_v2(db);
        return std::unexpected(err);
    }

    return connection{db};
}

} // namespace asterorm::sqlite
