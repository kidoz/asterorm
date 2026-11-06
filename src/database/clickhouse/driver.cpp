#include "asterorm/clickhouse/driver.hpp"
#include "asterorm/core/error.hpp"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <clickhouse/client.h>
#pragma GCC diagnostic pop

namespace asterorm::ch {

asterorm::result<connection> driver::connect(const std::string& conninfo) const {
    // Very basic parsing for host/port. Assume conninfo is "host:port" or just "host".
    std::string host = "localhost";
    int port = 9000;

    // Find colon for port
    auto colon_pos = conninfo.find(':');
    if (colon_pos != std::string::npos) {
        host = conninfo.substr(0, colon_pos);
        try {
            port = std::stoi(conninfo.substr(colon_pos + 1));
        } catch (...) {
            // ignore
        }
    } else if (!conninfo.empty()) {
        host = conninfo;
    }

    try {
        clickhouse::ClientOptions options;
        options.SetHost(host);
        options.SetPort(port);
        auto client = std::make_unique<clickhouse::Client>(options);
        return connection{std::move(client)};
    } catch (const std::exception& e) {
        return std::unexpected(
            db_error{db_error_kind::connection_failed, e.what(), "", "", "", "", "", ""});
    }
}

} // namespace asterorm::ch