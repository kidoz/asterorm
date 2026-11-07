"""Install to a temporary prefix and run both streaming guide examples as consumers."""
import os
from pathlib import Path
import shlex
import subprocess
import tempfile


MAIN = r'''
#include <cstdlib>
int main() {
    const auto* configured = std::getenv("ASTERORM_TEST_CONNINFO");
    if (!configured) return 1;
    asterorm::pool_config config;
    config.conninfo = configured;
    config.min_size = 0;
    config.max_size = 1;
    asterorm::connection_pool<asterorm::pg::driver> pool{asterorm::pg::driver{}, config};
    StreamSession db{pool};
    auto setup = db.with_connection([](auto& connection) -> asterorm::result<void> {
        auto created = connection.execute("CREATE TEMP TABLE stream_jobs(id INTEGER,name TEXT,active BOOLEAN)");
        if (!created) return std::unexpected(created.error());
        auto seeded = connection.execute("INSERT INTO stream_jobs VALUES(1,'first',TRUE),(2,NULL,FALSE),(3,'third',TRUE)");
        if (!seeded) return std::unexpected(seeded.error());
        return {};
    });
    if (!setup) return 2;
    for (bool cursor : {false, true}) {
        int selects = 0;
        bool failed = false;
        bool pinned = false;
        db.set_observer([&](const asterorm::query_event& event) {
            // Application BEGIN/COMMIT remain ordinary observed operations.
            if (event.sql.starts_with("SELECT \"id\"")) {
                ++selects;
                failed = event.error != nullptr;
                pinned = pool.stats().in_use == 1;
            }
        });
        auto count = cursor ? count_active_jobs_in_batches(db) : count_active_jobs(db);
        if (!count || *count != 2 || selects != 1 || failed || !pinned || pool.stats().in_use != 0) return 3;
        db.set_observer({}); // captured locals have loop scope
        auto portals = db.native_scalar<int>("SELECT count(*) FROM pg_cursors WHERE starts_with(name,'_asterorm_cursor_')");
        if (!portals || *portals != 0) return 4;
    }
    return 0;
}
'''

CMAKE = '''cmake_minimum_required(VERSION 3.20)
project(StreamingConsumer LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)
find_package(AsterORM CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE AsterORM::asterorm)
'''


def main():
    root = Path(__file__).resolve().parents[3]
    workspace = Path(tempfile.mkdtemp(prefix="asterorm-streaming-consumers-"))
    prefix = workspace / "install"
    environment = os.environ.copy()
    # The same local service defaults as the normal integration suite; never print settings.
    environment.setdefault("ASTERORM_TEST_CONNINFO",
                           "host=127.0.0.1 port=5432 dbname=orm_test "
                           "user=orm_test password=orm_test sslmode=disable")
    dependencies = [Path("/opt/homebrew/opt") / name
                    for name in ("libpq", "sqlite", "mysql-client")]
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
    run(["meson", "install", "-C", str(workspace / "build"), "--no-rebuild"],
        stdout=subprocess.DEVNULL)
    guide = (root / "docs/how-to/stream-entities.md").read_text()
    snippets = [part.split("```", 1)[0] for part in guide.split("```cpp\n")[1:]]
    if len(snippets) != 2:
        raise RuntimeError("Expected both complete streaming guide examples")
    source = workspace / "main.cpp"
    source.write_text("\n".join(snippets) + MAIN)
    (workspace / "CMakeLists.txt").write_text(CMAKE)
    run(["cmake", "-S", str(workspace), "-B", str(workspace / "cmake"),
         "-DCMAKE_PREFIX_PATH=" + ";".join(map(str, [prefix, *dependencies]))])
    run(["cmake", "--build", str(workspace / "cmake")])
    run([str(workspace / "cmake/consumer")])
    print("Installed CMake single-row/cursor guide consumers: passed")
    flags = subprocess.check_output(["pkg-config", "--cflags", "--libs", "asterorm"],
                                    env=environment, text=True)
    executable = workspace / "pkg-consumer"
    run([*shlex.split(environment.get("CXX", "c++")), "-std=c++23", str(source),
         *shlex.split(flags), "-Wl,-rpath," + str(prefix / "lib"), "-o", str(executable)])
    run([str(executable)])
    print("Installed shared pkg-config single-row/cursor guide consumers: passed")
    print("Temporary install and consumer directory:", workspace)


if __name__ == "__main__":
    main()
