"""Freshly install and run the optional transactional scope guide on SQLite and PG."""
import os
from pathlib import Path
import shlex
import subprocess
import tempfile


MAIN = r'''
#include <asterorm/postgres/driver.hpp>
#include <cstdlib>
int main() {
    auto sqlite = sqlite_scope_example();
    if (!sqlite || *sqlite != 1) return 1;
    const auto* settings = std::getenv("ASTERORM_TEST_CONNINFO");
    if (!settings) return 2;
    auto postgres = count_after_test_rollback(asterorm::pg::driver{}, settings);
    return postgres && *postgres == 1 ? 0 : 3;
}
'''
CMAKE = '''cmake_minimum_required(VERSION 3.20)
project(TransactionalScopeConsumer LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)
find_package(AsterORM CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE AsterORM::asterorm)
'''


def main():
    root = Path(__file__).resolve().parents[3]
    workspace = Path(tempfile.mkdtemp(prefix="asterorm-test-scope-consumers-"))
    prefix = workspace / "install"
    environment = os.environ.copy()
    environment.setdefault("ASTERORM_TEST_CONNINFO", "host=127.0.0.1 port=5432 dbname=orm_test "
                           "user=orm_test password=orm_test sslmode=disable")
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
    guide = (root / "docs/how-to/transactional-test-scopes.md").read_text()
    snippets = [part.split("```", 1)[0] for part in guide.split("```cpp\n")[1:]]
    if len(snippets) != 1:
        raise RuntimeError("Expected the complete transactional scope guide")
    source = workspace / "main.cpp"
    source.write_text(snippets[0] + MAIN)
    (workspace / "CMakeLists.txt").write_text(CMAKE)
    run(["cmake", "-S", str(workspace), "-B", str(workspace / "cmake"),
         "-DCMAKE_PREFIX_PATH=" + ";".join(map(str, [prefix, *dependencies]))])
    run(["cmake", "--build", str(workspace / "cmake")])
    run([str(workspace / "cmake/consumer")])
    print("Installed CMake scope guide: SQLite/PG rollback and retained baseline passed")
    flags = subprocess.check_output(["pkg-config", "--cflags", "--libs", "asterorm"],
                                    env=environment, text=True)
    executable = workspace / "pkg-consumer"
    run([*shlex.split(environment.get("CXX", "c++")), "-std=c++23", str(source),
         *shlex.split(flags), "-Wl,-rpath," + str(prefix / "lib"), "-o", str(executable)])
    run([str(executable)])
    print("Installed shared pkg-config scope guide: SQLite/PG baseline passed; no Catch2 dependency")
    print("Temporary install and consumer directory:", workspace)


if __name__ == "__main__":
    main()
