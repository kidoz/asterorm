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
