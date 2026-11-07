set shell := ["zsh", "-cu"]

build_dir := "buildDir"

default:
    @just --list

help:
    @just --list

setup:
    if [[ -d {{build_dir}} ]]; then \
        meson setup {{build_dir}} --reconfigure; \
    else \
        meson setup {{build_dir}}; \
    fi

build: setup
    meson compile -C {{build_dir}}

test: setup
    meson test -C {{build_dir}} --print-errorlogs

# Run SQLite unit/integration tests. Uses :memory:, no external services.
test-sqlite: setup
    meson test -C {{build_dir}} --suite asterorm "sqlite unit tests" "sqlite integration" --print-errorlogs

# Bring up the PostgreSQL test database from compose.yaml and wait for healthy.
db-up:
    docker compose up -d --wait

# Tear the PostgreSQL test database down (and remove its volume).
db-down:
    docker compose down -v

# Run PostgreSQL unit/integration and migration process tests with the database up.
# Leaves the database running for repeated runs.
test-pg: setup db-up
    meson test -C {{build_dir}} --suite asterorm "postgres unit tests" "pg integration" "migration serialization" --print-errorlogs

# End-to-end: bring DB up, run the full test suite, tear DB down on success.
test-all: setup db-up
    meson test -C {{build_dir}} --print-errorlogs && just db-down

format: setup
    meson compile -C {{build_dir}} clang-format

format-check: setup
    meson compile -C {{build_dir}} clang-format-check

lint: setup
    meson compile -C {{build_dir}} clang-tidy

check: format-check test

clean: setup
    meson compile -C {{build_dir}} --clean

wipe:
    rm -rf {{build_dir}}

# Enable the adapter and run against MySQL 8.4 on port 3307.
mysql-up:
    docker compose --profile mysql up -d --wait mysql

test-mysql: setup mysql-up
    meson configure {{build_dir}} -Dmysql=enabled
    meson test -C {{build_dir}} "core unit tests" "mysql unit tests" "mysql integration" --print-errorlogs
