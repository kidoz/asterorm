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
    snippets = [part.split("```", 1)[0] for part in guide.split("```cpp\n")[1:]]
    source = '''
#include "asterorm/repository.hpp"
#include "asterorm/sqlite/driver.hpp"
struct Job { int id{}; int cutoff{}; int price{}; bool active{}; std::optional<std::string> note; };
struct ArchivedJob { std::optional<std::int64_t> archive_id; int source_id{}; int price{}; std::optional<std::string> note; };
template <> struct asterorm::entity_traits<ArchivedJob> {
    static constexpr const char* table = "archived_jobs";
    static constexpr auto columns = std::make_tuple(
        asterorm::column<&ArchivedJob::archive_id>("archive_id", asterorm::generated::by_default),
        asterorm::column<&ArchivedJob::source_id>("source_id"), asterorm::column<&ArchivedJob::price>("price"),
        asterorm::column<&ArchivedJob::note>("note"));
};
template <> struct asterorm::entity_traits<Job> {
    static constexpr const char* table = "jobs";
    static constexpr auto primary_key = asterorm::pk<&Job::id>("id");
    static constexpr auto columns = std::make_tuple(asterorm::column<&Job::id>("id"),
        asterorm::column<&Job::cutoff>("cutoff"), asterorm::column<&Job::price>("price"), asterorm::column<&Job::active>("active"),
        asterorm::column<&Job::note>("note"));
};
using Session = asterorm::session<asterorm::connection_pool<asterorm::sqlite::driver>>;
'''
    for index, snippet in enumerate(snippets):
        source += f'asterorm::result<void> example_{index}(Session& db) {{\n' + snippet + '\nreturn {};\n}\n'
    return subprocess.run([*command, "-x", "c++", "-fsyntax-only", "-"],
                          cwd=entry["directory"], input=source, text=True).returncode


if __name__ == "__main__":
    raise SystemExit(main())
