"""Build/install into a temporary prefix and run CMake/pkg-config SQLite consumers."""
import os
from pathlib import Path
import shlex
import subprocess
import tempfile


SOURCE = r'''
#include <asterorm/repository.hpp>
#include <asterorm/sqlite/driver.hpp>
struct Item { int id{}; bool active{}; int price{1}; };
struct ArchivedItem { int id{}; int price{}; };
template <> struct asterorm::entity_traits<Item> {
    static constexpr const char* table = "items";
    static constexpr auto primary_key = asterorm::pk<&Item::id>("id");
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&Item::id>("id"), asterorm::column<&Item::active>("active"),
        asterorm::column<&Item::price>("price"));
};
template <> struct asterorm::entity_traits<ArchivedItem> {
    static constexpr const char* table = "archived_items";
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&ArchivedItem::id>("id"),
        asterorm::column<&ArchivedItem::price>("price"));
};
int main() {
    asterorm::pool_config config;
    config.conninfo = ":memory:";
    config.max_size = 1;
    asterorm::connection_pool<asterorm::sqlite::driver> pool{asterorm::sqlite::driver{}, config};
    asterorm::session db{pool};
    asterorm::repository repo{db};
    if (!db.with_connection([](auto& connection) {
        return connection.execute("CREATE TABLE items(id INTEGER PRIMARY KEY,active BOOLEAN,price INTEGER)");
    })) return 1;
    if (!db.with_connection([](auto& connection) {
        return connection.execute("INSERT INTO items VALUES(1,TRUE,1)");
    })) return 2;
    namespace sql = asterorm::sql;
    if (repo.update_where<Item>(sql::col("id") == sql::val(1),
                               {{"active", sql::val(false)}}) != 1) return 3;
    if (repo.update_where<Item>(sql::col("id") == sql::val(1),
        {{"price", sql::multiply(sql::add(sql::col("price"), sql::val(2)), sql::val(3))}}) != 1) return 4;
    if (!db.with_connection([](auto& connection) {
        return connection.execute("CREATE TABLE archived_items(id INTEGER PRIMARY KEY,price INTEGER)");
    })) return 5;
    if (repo.insert_select<ArchivedItem>(sql::select({"id", "price"}).from("items")
        .where(sql::col("id") == sql::val(1)).build()) != 1) return 6;
    return repo.delete_where<ArchivedItem>(sql::col("price") == sql::val(9)) == 1 ? 0 : 7;
}
'''

CMAKE = '''cmake_minimum_required(VERSION 3.20)
project(BulkConsumer LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)
find_package(AsterORM CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE AsterORM::asterorm)
'''


def main():
    root = Path(__file__).resolve().parents[3]
    workspace = Path(tempfile.mkdtemp(prefix="asterorm-bulk-consumers-"))
    prefix = workspace / "install"
    environment = os.environ.copy()
    # Match the project's optional Homebrew dependency discovery on macOS.
    dependency_prefixes = [
        Path("/opt/homebrew/opt/libpq"),
        Path("/opt/homebrew/opt/sqlite"),
        Path("/opt/homebrew/opt/mysql-client"),
    ]
    dependency_prefixes = [path for path in dependency_prefixes if path.exists()]
    pkg_paths = [prefix / "lib/pkgconfig"]
    pkg_paths.extend(path / "lib/pkgconfig" for path in dependency_prefixes)
    environment["PKG_CONFIG_PATH"] = os.pathsep.join(map(str, pkg_paths))
    if os.environ.get("PKG_CONFIG_PATH"):
        environment["PKG_CONFIG_PATH"] += os.pathsep + os.environ["PKG_CONFIG_PATH"]

    def run(arguments, **options):
        subprocess.run(arguments, check=True, env=environment, **options)

    run([
        "meson", "setup", str(workspace / "build"), str(root), "--prefix", str(prefix),
        "-Dtests=false", "-Dexamples=false", "-Dmysql=enabled", "-Dpostgres=enabled",
        "-Dsqlite=enabled", "-Dclickhouse=disabled",
    ], stdout=subprocess.DEVNULL)
    run(["meson", "compile", "-C", str(workspace / "build")], stdout=subprocess.DEVNULL)
    run(["meson", "install", "-C", str(workspace / "build"), "--no-rebuild"],
        stdout=subprocess.DEVNULL)
    source = workspace / "main.cpp"
    source.write_text(SOURCE)
    (workspace / "CMakeLists.txt").write_text(CMAKE)
    search_prefix = ";".join(map(str, [prefix, *dependency_prefixes]))
    run(["cmake", "-S", str(workspace), "-B", str(workspace / "cmake"),
         "-DCMAKE_PREFIX_PATH=" + search_prefix])
    run(["cmake", "--build", str(workspace / "cmake")])
    run([str(workspace / "cmake/consumer")])
    print("Installed CMake consumer: passed")
    flags = subprocess.check_output(
        ["pkg-config", "--cflags", "--libs", "asterorm"], env=environment, text=True)
    run([*shlex.split(environment.get("CXX", "c++")), "-std=c++23", str(source),
         *shlex.split(flags), "-Wl,-rpath," + str(prefix / "lib"),
         "-o", str(workspace / "pkg-consumer")])
    run([str(workspace / "pkg-consumer")])
    print("Installed pkg-config consumer: passed")
    requirements = subprocess.check_output(
        ["pkg-config", "--print-requires-private", "asterorm"], env=environment, text=True)
    if "sqlite3 >= 3.37.0" not in requirements:
        raise RuntimeError("Missing SQLite 3.37+ pkg-config private requirement")
    print("pkg-config SQLite 3.37+ private requirement: passed")
    print("Temporary install and consumer directory:", workspace)


if __name__ == "__main__":
    main()
