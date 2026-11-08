"""Freshly install and run the combined advanced-query acceptance on SQLite, PostgreSQL and MySQL."""
import os
import runpy
import shutil
from pathlib import Path
import shlex
import subprocess
import tempfile


COMPOSITION_MAIN = r'''
#include "composition.hpp"
#include <asterorm/postgres/driver.hpp>
#include <asterorm/sqlite/driver.hpp>
#include <asterorm/mysql/driver.hpp>
#include <cstdlib>
#include <iostream>
template <typename Driver> bool check_composition(Driver driver, std::string settings) {
    using namespace asterorm;
    using namespace asterorm::acceptance;
    pool_config config;
    config.conninfo = std::move(settings);
    config.min_size = 0;
    config.max_size = 1;
    connection_pool<Driver> pool{std::move(driver), config};
    session db{pool};
    constexpr auto target = sql::dialect_for_session<decltype(db)>();
    if constexpr (target != sql::dialect::postgres) {
        auto unsupported = composed_query(true, target != sql::dialect::mysql);
        auto rejected = sql::query_as<composed_row>(db, unsupported);
        if (rejected || rejected.error().kind != db_error_kind::parse_failed || pool.stats().total != 0)
            return false;
    }
    auto prepared = prepare_composed_rows(db);
    if (!prepared) { std::cerr << prepared.error().message << "\n"; return false; }
    int events{};
    db.set_observer([&](const query_event&) { ++events; });
    auto query = composed_query();
    auto typed = composed_query(false, false, true);
    auto rows = sql::query_as<composed_row>(db, query);
    auto typed_rows = sql::query_as<composed_row>(db, typed);
    const std::vector<composed_row> expected{{4,1,4,70,std::nullopt},
        {99,std::nullopt,9,std::nullopt,std::nullopt}, {99,std::nullopt,9,std::nullopt,std::nullopt}};
    if (!rows || !typed_rows || *rows != expected || *typed_rows != expected || events != 2)
        return false;
    if constexpr (target != sql::dialect::mysql) {
        auto filtered = composed_query(true, false, true, false);
        auto totals = sql::query_as<composed_row>(db, filtered);
        if (!totals || totals->size()!=7 || totals->at(0).total || totals->at(4).total!=150)
            return false;
    }
    if constexpr (target == sql::dialect::postgres) {
        auto distinct = composed_query(true, true, true, false);
        auto winners = sql::query_as<composed_row>(db, distinct);
        if (!winners || winners->size()!=5 || winners->at(0).id!=3 ||
            winners->at(1).id!=4 || winners->at(2).id!=9 || winners->at(1).total!=60)
            return false;
    }
    return pool.stats().in_use == 0;
}
int main() {
    return check_composition(asterorm::sqlite::driver{}, ":memory:") &&
        check_composition(asterorm::pg::driver{}, std::getenv("ASTERORM_TEST_CONNINFO")) &&
        check_composition(asterorm::mysql::driver{}, std::getenv("ASTERORM_MYSQL_TEST_CONNINFO")) ? 0 : 1;
}
'''
GUIDES = {
    'ordinary-cte': 'common-table-expressions',
    'set-operations': 'set-operations',
    'recursive-cte': 'recursive-common-table-expressions',
    'window-functions': 'window-functions',
    'aggregate-extensions': 'aggregate-extensions',
}
CMAKE = '''cmake_minimum_required(VERSION 3.20)
project(AdvancedQueryAcceptance LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)
find_package(AsterORM CONFIG REQUIRED)
'''


def main():
    root = Path(__file__).resolve().parents[3]
    workspace = Path(tempfile.mkdtemp(prefix="asterorm-advanced-consumers-"))
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
    sources = {}
    for evidence, guide in GUIDES.items():
        document = (root / 'docs/how-to' / (guide + '.md')).read_text()
        snippets = [part.split('```', 1)[0] for part in document.split('```cpp\n')[1:]]
        if len(snippets) != 1:
            raise RuntimeError('Expected one complete example in ' + guide)
        fixture = runpy.run_path(str(root / 'docs/evidence' / evidence / 'check_consumers.py'))
        sources[evidence] = snippets[0] + fixture['MAIN']
    sources['composition'] = COMPOSITION_MAIN
    shutil.copyfile(root / 'docs/evidence/advanced-query-acceptance/composition.hpp',
                    workspace / 'composition.hpp')
    targets = {}
    cmake = CMAKE
    for index, (name, contents) in enumerate(sources.items()):
        target = 'consumer_' + str(index)
        targets[name] = target
        (workspace / (target + '.cpp')).write_text(contents)
        cmake += f'add_executable({target} {target}.cpp)\n'
        cmake += f'target_link_libraries({target} PRIVATE AsterORM::asterorm)\n'
    (workspace / 'CMakeLists.txt').write_text(cmake)
    run(["cmake", "-S", str(workspace), "-B", str(workspace / "cmake"),
         "-DCMAKE_PREFIX_PATH=" + ";".join(map(str, [prefix, *dependencies]))])
    run(["cmake", "--build", str(workspace / "cmake")])
    for name, target in targets.items():
        run([str(workspace / 'cmake' / target)])
        print('Installed CMake:', name, 'passed on SQLite/PG/MySQL', flush=True)
    flags = subprocess.check_output(['pkg-config', '--cflags', '--libs', 'asterorm'],
                                    env=environment, text=True)
    for name, target in targets.items():
        executable = workspace / (target + '-pkg')
        run([*shlex.split(environment.get('CXX', 'c++')), '-std=c++23',
             str(workspace / (target + '.cpp')), *shlex.split(flags),
             '-Wl,-rpath,' + str(prefix / 'lib'), '-o', str(executable)])
        run([str(executable)])
        print('Installed shared pkg-config:', name, 'passed on SQLite/PG/MySQL', flush=True)
    print('All six consumers passed for both packages; tests/examples disabled; no Catch2 dependency')
    print('Temporary install and consumer directory:', workspace)


if __name__ == '__main__':
    main()
