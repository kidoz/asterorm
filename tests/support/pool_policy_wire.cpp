#include "asterorm/pool/connection_pool.hpp"
#include "asterorm/postgres/driver.hpp"

int main(int count, char** arguments) {
    if (count != 3) {
        return 90;
    }
    asterorm::pool_config config;
    config.min_size = 0;
    config.max_size = 1;
    config.conninfo =
        "host=127.0.0.1 port=" + std::string{arguments[1]} +
        " user=pool_probe dbname=pool_probe sslmode=disable gssencmode=disable connect_timeout=2";
    const std::string_view mode{arguments[2]};
    if (mode == "off" || mode == "profile") {
        config.statement_caching = asterorm::statement_cache_policy::off;
    }
    if (mode == "zero") {
        config.prepared_statement_cache_size = 0;
    }
    if (mode == "profile") {
        config.profile = asterorm::pool_profile::transaction_pool;
    }
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    auto held = pool.acquire();
    if (!held) {
        return 1;
    }
    auto selected = (*held)->execute_prepared("SELECT 41", {});
    if (!selected) {
        return 2;
    }
    return selected->get_int64(0, 0) == 41 && (*held)->prepared_statement_count() == 0 ? 0 : 3;
}
