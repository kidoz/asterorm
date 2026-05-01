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

format: setup
    find src include tests examples -type f \( -name "*.cpp" -o -name "*.hpp" \) -print0 | xargs -0 clang-format -i

format-check: setup
    find src include tests examples -type f \( -name "*.cpp" -o -name "*.hpp" \) -print0 | xargs -0 clang-format --dry-run --Werror

lint: setup
    find src tests examples -type f -name "*.cpp" -print0 | xargs -0 clang-tidy -p {{build_dir}} --extra-arg="-isysroot" --extra-arg="$(xcrun --show-sdk-path)"

check: format-check test

clean: setup
    meson compile -C {{build_dir}} --clean

wipe:
    rm -rf {{build_dir}}
