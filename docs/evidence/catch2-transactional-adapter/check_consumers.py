"""Install AsterORM and separately supplied Catch2, then run the literal public guide."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile


CMAKE = '''cmake_minimum_required(VERSION 3.20)
project(TransactionalCatch2Consumer LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)
find_package(AsterORM CONFIG REQUIRED)
find_package(Catch2 3 CONFIG REQUIRED)
add_executable(consumer main.cpp)
target_link_libraries(consumer PRIVATE AsterORM::asterorm Catch2::Catch2)
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-root", type=Path, default=Path("buildDir"))
    parser.add_argument("--catch2-source", type=Path)
    arguments = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    info = json.loads((arguments.build_root / "meson-info/intro-projectinfo.json").read_text())
    if arguments.catch2_source:
        catch_source = arguments.catch2_source.resolve()
    else:
        catch_project = next(project for project in info["subprojects"] if project["name"] == "catch2")
        catch_source = root / info["subproject_dir"] / ("Catch2-" + catch_project["version"])
    workspace = Path(tempfile.mkdtemp(prefix="asterorm-catch2-consumers-"))
    print("Temporary installs and consumers:", workspace, flush=True)
    prefix = workspace / "install"
    environment = os.environ.copy()
    dependencies = [Path("/opt/homebrew/opt") / name for name in ("libpq", "sqlite", "mysql-client")]
    dependencies = [path for path in dependencies if path.exists()]
    pkg_paths = [prefix / "lib/pkgconfig", prefix / "share/pkgconfig",
                 *(path / "lib/pkgconfig" for path in dependencies)]
    environment["PKG_CONFIG_PATH"] = os.pathsep.join(map(str, pkg_paths))
    if os.environ.get("PKG_CONFIG_PATH"):
        environment["PKG_CONFIG_PATH"] += os.pathsep + os.environ["PKG_CONFIG_PATH"]

    def run(command, **options):
        subprocess.run(command, check=True, env=environment, **options)

    run(["meson", "setup", str(workspace / "asterorm"), str(root), "--prefix", str(prefix),
         "-Dtests=false", "-Dexamples=false", "-Dpostgres=enabled", "-Dsqlite=enabled",
         "-Dmysql=enabled", "-Dclickhouse=disabled"], stdout=subprocess.DEVNULL)
    run(["meson", "compile", "-C", str(workspace / "asterorm")], stdout=subprocess.DEVNULL)
    run(["meson", "install", "-C", str(workspace / "asterorm"), "--no-rebuild"], stdout=subprocess.DEVNULL)
    # Normal AsterORM packaging must not discover/link Catch2, even when the optional header is installed.
    assert not list((workspace / "asterorm/subprojects").glob("Catch2*"))
    run(["cmake", "-S", str(catch_source), "-B", str(workspace / "catch2"),
         "-DCMAKE_INSTALL_PREFIX=" + str(prefix), "-DCMAKE_INSTALL_LIBDIR=lib",
         "-DCATCH_BUILD_TESTING=OFF", "-DCATCH_INSTALL_DOCS=OFF", "-DCATCH_INSTALL_EXTRAS=OFF"],
        stdout=subprocess.DEVNULL)
    run(["cmake", "--build", str(workspace / "catch2"), "--parallel", "4"], stdout=subprocess.DEVNULL)
    run(["cmake", "--install", str(workspace / "catch2")], stdout=subprocess.DEVNULL)
    guide = (root / "docs/how-to/catch2-transactional-tests.md").read_text()
    snippets = [part.split("```", 1)[0] for part in guide.split("```cpp\n")[1:]]
    if len(snippets) != 1:
        raise RuntimeError("Expected the complete Catch2 transactional guide")
    source = workspace / "main.cpp"
    source.write_text(snippets[0])
    (workspace / "CMakeLists.txt").write_text(CMAKE)
    run(["cmake", "-S", str(workspace), "-B", str(workspace / "consumer"),
         "-DCMAKE_PREFIX_PATH=" + ";".join(map(str, [prefix, *dependencies]))])
    run(["cmake", "--build", str(workspace / "consumer")])
    run([str(workspace / "consumer/consumer")])
    print("Installed CMake guide: four isolated paths passed with separately supplied Catch2")
    flags = subprocess.check_output(["pkg-config", "--cflags", "--libs", "asterorm", "catch2"],
                                    env=environment, text=True)
    executable = workspace / "pkg-consumer"
    run([*shlex.split(environment.get("CXX", "c++")), "-std=c++23", str(source),
         *shlex.split(flags), "-Wl,-rpath," + str(prefix / "lib"), "-o", str(executable)])
    run([str(executable)])
    print("Installed shared pkg-config guide: four isolated paths passed; no second main")
    print("Temporary installs and consumers:", workspace)


if __name__ == "__main__":
    main()
