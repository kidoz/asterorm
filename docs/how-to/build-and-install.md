# Build, install, and link the library

Use Meson 1.1.0 or newer, Python 3, a build backend such as Ninja, and a C++23
compiler and standard library supporting `std::expected`. Install development
headers for each enabled database adapter. See [build options](../reference/build.md)
for the complete switch list.

## Select adapters

PostgreSQL and SQLite are enabled by default and both dependencies are required
in that configuration:

```sh
meson setup buildDir
meson compile -C buildDir
```

For SQLite alone in a fresh directory:

```sh
meson setup build-sqlite -Dpostgres=disabled -Dsqlite=enabled
meson compile -C build-sqlite
```

To change an existing configuration, use `meson configure buildDir` with the
desired `-D` options, then compile again. Feature value `auto` allows an adapter
to be omitted when its dependency is unavailable; `enabled` requires it.

For MySQL, enable `-Dmysql=enabled`. Meson uses a system `mysqlclient` if found,
otherwise the pinned MariaDB Connector/C wrap. The fallback requires CMake,
OpenSSL and zlib development files. `--force-fallback-for=mysqlclient` selects
the bundled client; `--wrap-mode=nofallback` requires a system client.

If dependency discovery fails, set `PKG_CONFIG_PATH` to the dependency's
`lib/pkgconfig` directory. Meson also checks conventional Homebrew prefixes for
libpq, SQLite, and mysql-client.

## Install to a local prefix

For a fresh installation build:

```sh
meson setup build-install --prefix="$PWD/build-install/install" --libdir=lib \
  -Dtests=false -Dexamples=false
meson compile -C build-install
meson install -C build-install
```

Installation includes public headers, `libasterorm_core`, enabled adapter
libraries, `asterorm.pc`, and CMake package configuration files. The current
CMake package expects shared libraries under `<prefix>/lib`; retain the shared
library default and `--libdir=lib` when using it.

## Link a CMake application

In your application's `CMakeLists.txt`:

```cmake
cmake_minimum_required(VERSION 3.20)
project(my_app LANGUAGES CXX)
find_package(AsterORM CONFIG REQUIRED)
add_executable(my_app main.cpp)
target_compile_features(my_app PRIVATE cxx_std_23)
target_link_libraries(my_app PRIVATE AsterORM::asterorm)
```

Configure with the installation prefix:

```sh
cmake -S /path/to/my_app -B /path/to/my_app/build \
  -DCMAKE_PREFIX_PATH="$PWD/build-install/install"
cmake --build /path/to/my_app/build
```

You can link a specific installed adapter, such as `AsterORM::sqlite`, instead
of the umbrella target. The adapter brings in `AsterORM::core`.

## Link using pkg-config

```sh
export PKG_CONFIG_PATH="$PWD/build-install/install/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
c++ -std=c++23 /path/to/main.cpp $(pkg-config --cflags --libs asterorm) \
  -Wl,-rpath,"$PWD/build-install/install/lib" -o build-install/my_app
```

When the bundled MySQL client is installed, retain its matching library,
headers, and `asterorm_mysqlclient.pc` alongside the AsterORM package.
