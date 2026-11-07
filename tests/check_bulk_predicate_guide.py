"""Syntax-check the public mutation example with the configured SQLite headers."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-root", type=Path, required=True)
    arguments = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    commands = json.loads((arguments.build_root / "compile_commands.json").read_text())
    entry = next(item for item in commands if item["file"].endswith("/integration/test_sqlite_crud.cpp"))
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
    guide = (root / "docs/how-to/bulk-predicate-mutations.md").read_text()
    snippet = guide.split("```cpp\n", 1)[1].split("```", 1)[0]
    source = '''
#include "asterorm/repository.hpp"
#include "asterorm/sqlite/driver.hpp"
struct Job { int id{}; int cutoff{}; bool active{}; std::optional<std::string> note; };
template <> struct asterorm::entity_traits<Job> {
    static constexpr const char* table = "jobs";
    static constexpr auto primary_key = asterorm::pk<&Job::id>("id");
    static constexpr auto columns = std::make_tuple(asterorm::column<&Job::id>("id"),
        asterorm::column<&Job::cutoff>("cutoff"), asterorm::column<&Job::active>("active"),
        asterorm::column<&Job::note>("note"));
};
using Session = asterorm::session<asterorm::connection_pool<asterorm::sqlite::driver>>;
asterorm::result<void> example(Session& db) {
''' + snippet + '\nreturn {};\n}\n'
    return subprocess.run([*command, "-x", "c++", "-fsyntax-only", "-"],
                          cwd=entry["directory"], input=source, text=True).returncode


if __name__ == "__main__":
    raise SystemExit(main())
