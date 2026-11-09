"""Build, install and run isolated CMake/core, CMake/umbrella and pkg-config consumers."""
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile


def main():
    root = Path(__file__).resolve().parents[3]
    work = Path(tempfile.mkdtemp(prefix='asterorm-type-consumers-'))
    print('Workspace:', work, flush=True)
    prefix = work / 'install'
    build = work / 'library'

    def run(command, **kwargs):
        print(shlex.join(map(str, command)), flush=True)
        subprocess.run(list(map(str, command)), check=True, **kwargs)

    run(['meson', 'setup', build, root, '--prefix', prefix,
         '-Dtests=false', '-Dexamples=false', '-Dpostgres=enabled',
         '-Dsqlite=enabled', '-Dmysql=disabled'])
    run(['meson', 'compile', '-C', build, '-j4'])
    run(['meson', 'install', '-C', build, '--no-rebuild'])
    source = root / 'docs/evidence/postgres-type-foundations/consumer.cpp'
    consumer = work / 'consumer'
    consumer.mkdir()
    (consumer / 'CMakeLists.txt').write_text('''cmake_minimum_required(VERSION 3.20)
project(type_consumer LANGUAGES CXX)
find_package(AsterORM CONFIG REQUIRED)
foreach(component IN ITEMS core asterorm)
  add_executable(${component}_consumer "''' + str(source) + '''")
  target_compile_features(${component}_consumer PRIVATE cxx_std_23)
  target_link_libraries(${component}_consumer PRIVATE AsterORM::${component})
endforeach()
''')
    cmake_build = work / 'cmake'
    run(['cmake', '-S', consumer, '-B', cmake_build, f'-DCMAKE_PREFIX_PATH={prefix}'])
    run(['cmake', '--build', cmake_build, '-j4'])
    run([cmake_build / 'core_consumer'])
    run([cmake_build / 'asterorm_consumer'])
    environment = dict(os.environ)
    pkg_paths = [str(prefix / 'lib/pkgconfig')]
    if shutil.which('brew'):
        brew_prefix = subprocess.check_output(['brew', '--prefix', 'libpq'], text=True).strip()
        pkg_paths.append(str(Path(brew_prefix) / 'lib/pkgconfig'))
    pkg_paths.append(environment.get('PKG_CONFIG_PATH', ''))
    environment['PKG_CONFIG_PATH'] = os.pathsep.join(pkg_paths)
    flags = subprocess.check_output(['pkg-config', '--cflags', '--libs', 'asterorm'],
                                    env=environment, text=True)
    executable = work / 'pkg_consumer'
    run(['c++', '-std=c++23', '-Wall', '-Wextra', '-Werror', source, '-o', executable,
         *shlex.split(flags), f'-Wl,-rpath,{prefix / "lib"}'])
    run([executable])
    print('All 3 installed consumers passed.', flush=True)


if __name__ == '__main__':
    main()
