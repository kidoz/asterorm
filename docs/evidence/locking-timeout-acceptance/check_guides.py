"""Syntax-check the five public locking/runtime guide examples without running SQL."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-root", type=Path, default=Path("buildDir"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    build = args.build_root.resolve()
    commands = json.loads((build / "compile_commands.json").read_text())
    entry = next(item for item in commands if item["file"].endswith("/integration/test_statement_timeout.cpp"))
    command = []
    skip = False
    for argument in shlex.split(entry["command"]):
        if skip:
            skip = False
            continue
        if argument in ("-o", "-MF", "-MQ", "-MT"):
            skip = True
            continue
        if argument in ("-c", "-MD", "-MMD", entry["file"]):
            continue
        command.append(argument)
    source = '''
#include "asterorm/clickhouse/driver.hpp"
#include "asterorm/migration.hpp"
#include "asterorm/postgres/driver.hpp"
#include "asterorm/session.hpp"
#include "asterorm/sql/typed_query.hpp"
struct Job { int id{}; };
template <> struct asterorm::entity_traits<Job> {
    static constexpr const char* table = "jobs";
    static constexpr auto columns = std::make_tuple(asterorm::column<&Job::id>("id"));
};
using Session = asterorm::session<asterorm::connection_pool<asterorm::pg::driver>>;
static_assert(!asterorm::sql::supports_select_locking(asterorm::ch::connection::sql_dialect));
static_assert(!asterorm::detail::has_statement_timeout<asterorm::ch::connection>);
static_assert(!asterorm::detail::has_advisory_scope<asterorm::ch::connection>);
void check_clickhouse(asterorm::session<asterorm::connection_pool<asterorm::ch::driver>>& db) {
    auto timeout = db.with_statement_timeout(std::chrono::milliseconds{1},
        [](auto&) -> asterorm::result<void> { return {}; });
    auto lock = db.with_advisory_lock(1, [](auto&) -> asterorm::result<void> { return {}; });
    (void)timeout;
    (void)lock;
}
'''
    guides = [
        ("select-locking.md", "select_locking", ""),
        ("advisory-locking.md", "advisory_locking", "(void)refreshed;"),
        ("statement-timeouts.md", "statement_timeouts", "(void)completed;"),
        ("pooling-and-observability.md", "pooling", ""),
        ("migrations.md", "migrations", ""),
    ]
    for filename, function, footer in guides:
        text = (root / "docs/how-to" / filename).read_text()
        snippet = text.split("```cpp\n", 1)[1].split("```", 1)[0]
        source += f"asterorm::result<void> check_{function}([[maybe_unused]] Session& db) {{\n"
        source += snippet + "\n" + footer + "\nreturn {};\n}\n"
    run = subprocess.run([*command, "-x", "c++", "-fsyntax-only", "-"],
                         input=source, text=True, cwd=entry["directory"])
    print(f"Guide syntax checks: {len(guides)} plus ClickHouse rejection; compiler exit: {run.returncode}")
    return run.returncode


if __name__ == "__main__":
    raise SystemExit(main())
