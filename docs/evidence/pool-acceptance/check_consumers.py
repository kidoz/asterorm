"""Freshly install and run the combined pool acceptance guide on SQLite, PostgreSQL and MySQL."""
import os
from pathlib import Path
import shlex
import subprocess
import tempfile


MAIN = r'''
#include <asterorm/postgres/driver.hpp>
#include <asterorm/sqlite/driver.hpp>
#include <asterorm/mysql/driver.hpp>
#include <cstdlib>

template <typename Driver>
bool check_guides(Driver driver, const std::string& settings, bool postgres) {
    auto ownership = pool_example(driver, settings);
    auto health = pool_health_example(driver, settings);
    auto aging = pool_aging_example(driver, settings);
    auto cache = pool_cache_example(driver, settings);
    auto profile = pool_cache_example(driver, settings, true);
    if (!ownership || *ownership != 41 || !aging || *aging != 42 || !cache || *cache != 42)
        return false;
    if (postgres ? (!health || *health != 43 || !profile || *profile != 42)
                 : (health || profile || health.error().kind != asterorm::db_error_kind::query_failed ||
                    profile.error().kind != asterorm::db_error_kind::query_failed))
        return false;
    asterorm::pool_config config;
    config.conninfo = settings;
    config.min_size = 0;
    config.max_size = 1;
    config.statement_caching = asterorm::statement_cache_policy::off;
    config.pre_ping = postgres;
    config.max_lifetime = std::chrono::seconds{1};
    config.leak_threshold = std::chrono::milliseconds{20};
    config.idle_timeout = std::chrono::milliseconds{5};
    config.maintenance_interval = std::chrono::milliseconds{1};
    asterorm::connection_pool<Driver> pool{driver, config};
    auto held = pool.acquire();
    if (!held) return false;
    std::vector<asterorm::pool_diagnostic> records;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
    do {
        records = pool.take_diagnostics();
        if (!records.empty()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    } while (std::chrono::steady_clock::now() < deadline);
    if (records.size() != 1 || pool.stats().overdue_leases != 1) return false;
    pool.close();
    auto selected = (*held)->execute_prepared("SELECT $1", {"42"});
    if (!selected || selected->get_int64(0, 0) != 42 || (*held)->prepared_statement_count() != 0)
        return false;
    held->release_to_pool();
    auto closed = pool.acquire();
    auto snapshot = pool.stats();
    return !closed && closed.error().kind == asterorm::db_error_kind::pool_closed &&
        snapshot.total == 0 && snapshot.retirements == 1 && snapshot.acquires_succeeded == 1 &&
        snapshot.acquires_failed == 1 && pool.take_diagnostics().empty();
}
int main() {
    return check_guides(asterorm::sqlite::driver{}, ":memory:", false) &&
        check_guides(asterorm::pg::driver{}, std::getenv("ASTERORM_TEST_CONNINFO"), true) &&
        check_guides(asterorm::mysql::driver{}, std::getenv("ASTERORM_MYSQL_TEST_CONNINFO"), false) ? 0 : 1;
}
'''

CMAKE = '''cmake_minimum_required(VERSION 3.20)
project(PoolAcceptanceConsumer LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)
find_package(AsterORM CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE AsterORM::asterorm)
'''


def main():
    root = Path(__file__).resolve().parents[3]
    workspace = Path(tempfile.mkdtemp(prefix="asterorm-pool-acceptance-consumers-"))
    prefix = workspace / "install"
    environment = os.environ.copy()
    environment.setdefault("ASTERORM_TEST_CONNINFO", "host=127.0.0.1 port=5432 dbname=orm_test "
                           "user=orm_test password=orm_test sslmode=disable")
    environment.setdefault("ASTERORM_MYSQL_TEST_CONNINFO", "host=127.0.0.1 port=3307 dbname=orm_test user=orm_test password=orm_test")
    dependencies = [Path("/opt/homebrew/opt") / name for name in ("libpq", "sqlite", "mysql-client")]
    dependencies = [path for path in dependencies if path.exists()]
    paths = [prefix / "lib/pkgconfig", *(path / "lib/pkgconfig" for path in dependencies)]
    environment["PKG_CONFIG_PATH"] = os.pathsep.join(map(str, paths))
    if os.environ.get("PKG_CONFIG_PATH"):
        environment["PKG_CONFIG_PATH"] += os.pathsep + os.environ["PKG_CONFIG_PATH"]

    def run(arguments, **options):
        subprocess.run(arguments, check=True, env=environment, **options)

    run(["meson", "setup", str(workspace / "build"), str(root), "--prefix", str(prefix),
         "-Dtests=false", "-Dexamples=false", "-Dpostgres=enabled", "-Dsqlite=enabled",
         "-Dmysql=enabled", "-Dclickhouse=disabled"], stdout=subprocess.DEVNULL)
    run(["meson", "compile", "-C", str(workspace / "build")], stdout=subprocess.DEVNULL)
    run(["meson", "install", "-C", str(workspace / "build"), "--no-rebuild"], stdout=subprocess.DEVNULL)
    snippets = []
    for name in ('pool-ownership-and-shutdown', 'pool-health-and-reuse',
                 'pool-aging-and-diagnostics', 'pool-cache-and-profiles'):
        guide = (root / 'docs/how-to' / (name + '.md')).read_text()
        blocks = [part.split('```', 1)[0] for part in guide.split('```cpp\n')[1:]]
        if len(blocks) != 1:
            raise RuntimeError('Expected one complete example: ' + name)
        snippets.extend(blocks)
    source = workspace / "main.cpp"
    source.write_text('\n'.join(snippets) + MAIN)
    (workspace / "CMakeLists.txt").write_text(CMAKE)
    run(["cmake", "-S", str(workspace), "-B", str(workspace / "cmake"),
         "-DCMAKE_PREFIX_PATH=" + ";".join(map(str, [prefix, *dependencies]))])
    run(["cmake", "--build", str(workspace / "cmake")])
    run([str(workspace / "cmake/consumer")])
    print("Installed CMake combined pool acceptance guide: all four guides and composed close/health/aging/metrics on SQLite/PG/MySQL passed")
    flags = subprocess.check_output(["pkg-config", "--cflags", "--libs", "asterorm"],
                                    env=environment, text=True)
    assert "catch" not in flags.lower(), flags
    executable = workspace / "pkg-consumer"
    run([*shlex.split(environment.get("CXX", "c++")), "-std=c++23", str(source),
         *shlex.split(flags), "-Wl,-rpath," + str(prefix / "lib"), "-o", str(executable)])
    run([str(executable)])
    print("Installed shared pkg-config combined pool acceptance guide: SQLite/PG/MySQL passed; no Catch2 dependency")
    print("Temporary install and consumer directory:", workspace)


if __name__ == "__main__":
    main()
