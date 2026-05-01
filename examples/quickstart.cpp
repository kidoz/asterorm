// Quickstart: end-to-end CRUD cycle against PostgreSQL.
//
// Connects using ASTERORM_TEST_CONNINFO (same convention as integration tests)
// or falls back to the local docker-compose dev DB. Prints each step and the
// observed query timing via the session's query_observer hook.
//
// Build: meson compile -C buildDir quickstart
// Run:   DYLD_LIBRARY_PATH=$PWD/buildDir/src ./buildDir/examples/quickstart

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>

#include "asterorm/core/observer.hpp"
#include "asterorm/postgres/driver.hpp"
#include "asterorm/repository.hpp"
#include "asterorm/session.hpp"

struct user {
    std::optional<int> id;
    std::string email;
    std::string name;
    bool active{true};
};

template <> struct asterorm::entity_traits<user> {
    static constexpr const char* table = "asterorm_quickstart_users";
    static constexpr auto primary_key = asterorm::pk<&user::id>("id");

    static constexpr auto columns = std::make_tuple(
        asterorm::column<&user::id>("id", asterorm::generated::by_default),
        asterorm::column<&user::email>("email"), asterorm::column<&user::name>("name"),
        asterorm::column<&user::active>("active"));
};

int main() {
    const char* env = std::getenv("ASTERORM_TEST_CONNINFO");
    std::string conninfo = env ? env
                               : "host=127.0.0.1 port=5432 dbname=orm_test user=orm_test "
                                 "password=orm_test sslmode=disable";

    asterorm::pool_config cfg;
    cfg.conninfo = conninfo;
    cfg.min_size = 1;
    cfg.max_size = 2;

    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, cfg};
    asterorm::session<decltype(pool)> db{pool};

    db.set_observer([](const asterorm::query_event& e) {
        std::cout << "[sql " << e.elapsed.count() / 1000 << "us] " << e.sql;
        if (e.error)
            std::cout << "  -> error: " << e.error->message;
        std::cout << '\n';
    });

    auto probe = pool.acquire();
    if (!probe.has_value()) {
        std::cerr << "could not connect: " << probe.error().message << '\n';
        return 1;
    }
    (void)(*probe)->execute("DROP TABLE IF EXISTS asterorm_quickstart_users");
    (void)(*probe)->execute(
        "CREATE TABLE asterorm_quickstart_users ("
        "id SERIAL PRIMARY KEY, email TEXT UNIQUE, name TEXT, active BOOLEAN DEFAULT TRUE)");
    probe.value().release_to_pool();

    asterorm::repository repo{db};

    user alice{.email = "alice@example.com", .name = "Alice"};
    if (auto r = repo.insert(alice); !r) {
        std::cerr << "insert failed: " << r.error().message << '\n';
        return 1;
    }
    std::cout << "inserted user id=" << *alice.id << '\n';

    auto loaded = repo.find<user>(*alice.id);
    if (!loaded) {
        std::cerr << "find failed: " << loaded.error().message << '\n';
        return 1;
    }
    std::cout << "loaded: " << loaded->name << " <" << loaded->email << ">\n";

    loaded->name = "Alice Cooper";
    if (auto r = repo.update(*loaded); !r) {
        std::cerr << "update failed: " << r.error().message << '\n';
        return 1;
    }

    auto refreshed = repo.find<user>(*alice.id);
    std::cout << "after update: " << refreshed->name << '\n';

    if (auto r = repo.erase<user>(*alice.id); !r) {
        std::cerr << "erase failed: " << r.error().message << '\n';
        return 1;
    }
    std::cout << "erased\n";
    return 0;
}
