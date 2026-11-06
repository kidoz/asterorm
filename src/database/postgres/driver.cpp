#include "asterorm/postgres/driver.hpp"

#include <libpq-fe.h>

namespace asterorm::pg {

asterorm::result<connection> driver::connect(const std::string& conninfo) const {
    PGconn* conn = PQconnectdb(conninfo.c_str());

    if (PQstatus(conn) != CONNECTION_OK) {
        db_error err;
        err.kind = db_error_kind::connection_failed;
        err.message = PQerrorMessage(conn);
        PQfinish(conn);
        return std::unexpected(err);
    }

    return connection{conn};
}

} // namespace asterorm::pg
