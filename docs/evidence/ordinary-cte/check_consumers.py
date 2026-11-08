"""Freshly install and run the ordinary CTE guide on SQLite, PostgreSQL and MySQL."""
import os
from pathlib import Path
import shlex
import subprocess
import tempfile


MAIN = r'''
#include <asterorm/sqlite/driver.hpp>
#include <asterorm/postgres/driver.hpp>
#include <asterorm/mysql/driver.hpp>
#include <cstdlib>
#include <iostream>
template <typename Driver> bool check(Driver driver, std::string settings) {
    auto rows = cte_example(std::move(driver), std::move(settings));
    if (!rows) { std::cerr << rows.error().message << "\n"; return false; }
    return rows->size() == 2 && rows->at(0).id == 2 && !rows->at(0).note &&
        rows->at(1).id == 3 && rows->at(1).note == "third";
}
int main() {
    const auto* postgres = std::getenv("ASTERORM_TEST_CONNINFO");
    const auto* mysql = std::getenv("ASTERORM_MYSQL_TEST_CONNINFO");
    return check(asterorm::sqlite::driver{}, ":memory:") &&
        check(asterorm::pg::driver{}, postgres) &&
        check(asterorm::mysql::driver{}, mysql) ? 0 : 1;
}
'''
CMAKE = '''cmake_minimum_required(VERSION 3.20)
project(OrdinaryCteConsumer LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)
find_package(AsterORM CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE AsterORM::asterorm)
'''


def main():
    root = Path(__file__).resolve().parents[3]
    workspace = Path(tempfile.mkdtemp(prefix="asterorm-cte-consumers-"))
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
    guide = (root / "docs/how-to/common-table-expressions.md").read_text()
    snippets = [part.split("```", 1)[0] for part in guide.split("```cpp\n")[1:]]
    if len(snippets) != 1:
        raise RuntimeError("Expected the complete CTE guide")
    source = workspace / "main.cpp"
    source.write_text(snippets[0] + MAIN)
    (workspace / "CMakeLists.txt").write_text(CMAKE)
    run(["cmake", "-S", str(workspace), "-B", str(workspace / "cmake"),
         "-DCMAKE_PREFIX_PATH=" + ";".join(map(str, [prefix, *dependencies]))])
    run(["cmake", "--build", str(workspace / "cmake")])
    run([str(workspace / "cmake/consumer")])
    print("Installed CMake CTE guide: SQLite/PG/MySQL binding, named rows and NULL passed")
    flags = subprocess.check_output(["pkg-config", "--cflags", "--libs", "asterorm"],
                                    env=environment, text=True)
    executable = workspace / "pkg-consumer"
    run([*shlex.split(environment.get("CXX", "c++")), "-std=c++23", str(source),
         *shlex.split(flags), "-Wl,-rpath," + str(prefix / "lib"), "-o", str(executable)])
    run([str(executable)])
    print("Installed shared pkg-config CTE guide: SQLite/PG/MySQL passed; no Catch2 dependency")
    print("Temporary install and consumer directory:", workspace)


if __name__ == "__main__":
    main()
