"""Compile and run the historical pool inventory against one configured native build."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-root', type=Path, default=Path('buildDir'))
    arguments = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    build = arguments.build_root.resolve()
    workspace = Path(tempfile.mkdtemp(prefix='asterorm-pool-inventory-'))
    entries = json.loads((build / 'compile_commands.json').read_text())
    source = Path(__file__).resolve().with_name('inventory.cpp')
    names = ('integration/test_sql.cpp', 'integration/test_sqlite_crud.cpp',
             'integration/test_mysql_crud.cpp')
    selected = [next(entry for entry in entries if entry['file'].endswith(name)) for name in names]
    compiler = shlex.split(selected[0]['command'])[0]
    includes = []
    for entry in selected:
        for token in shlex.split(entry['command']):
            if token.startswith('-I') and token not in includes:
                includes.append(token)
    targets = json.loads(subprocess.check_output(['meson', 'introspect', str(build), '--targets'],
                                                text=True))
    libraries = [next(target['filename'][0] for target in targets if target['name'] == name)
                 for name in ('asterorm_pg', 'asterorm_sqlite', 'asterorm_mysql', 'asterorm_core')]
    executable = workspace / 'inventory'
    object_file = workspace / 'inventory.o'
    command = [compiler, *includes, '-std=c++23', '-O0', '-g', '-Wall', '-Wextra', '-Wpedantic',
               '-Werror', '-pthread', '-c', str(source), '-o', str(object_file)]
    link = [compiler, str(object_file), *libraries, '-pthread',
               *['-Wl,-rpath,' + str(path) for path in sorted({Path(lib).parent for lib in libraries})],
               '-o', str(executable)]
    database = [dict(directory=str(build), file=str(source), arguments=command)]
    commands = dict(compile=command, link=link, run=[str(executable)])
    print('Inventory workspace:', workspace, flush=True)
    subprocess.run(command, cwd=build, check=True)
    subprocess.run(link, cwd=build, check=True)
    subprocess.run([str(executable)], cwd=root, check=True, timeout=30)
    thread_source = source.with_name('thread_probe.cpp')
    thread_object = workspace / 'thread_probe.o'
    thread_compile = [str(thread_source) if token == str(source) else
                      str(thread_object) if token == str(object_file) else token for token in command]
    database.append(dict(directory=str(build), file=str(thread_source), arguments=thread_compile))
    with (workspace / 'thread_probe.log').open('w') as log:
        thread_result = subprocess.run(thread_compile, cwd=build, stdout=log, stderr=subprocess.STDOUT)
    print('toolchain.jthread_compile_exit=' + str(thread_result.returncode), flush=True)
    commands.update(thread_compile=thread_compile, thread_compile_exit=thread_result.returncode)
    if not thread_result.returncode:
        thread_executable = workspace / 'thread_probe'
        thread_link = [compiler, str(thread_object), '-pthread', '-o', str(thread_executable)]
        subprocess.run(thread_link, cwd=build, check=True)
        subprocess.run([str(thread_executable)], check=True, timeout=5)
        commands.update(thread_link=thread_link, thread_run=[str(thread_executable)])
    (workspace / 'compile_commands.json').write_text(json.dumps(database, indent=2) + '\n')
    (workspace / 'commands.json').write_text(json.dumps(commands, indent=2) + '\n')


if __name__ == '__main__':
    main()
