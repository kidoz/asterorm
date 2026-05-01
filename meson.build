project(
  'asterorm',
  'cpp',
  version : '0.1.0',
  meson_version : '>= 1.1.0',
  default_options : [
    'cpp_std=c++23',
    'warning_level=3',
    'werror=true',
    'default_library=shared',
  ],
)

python = import('python').find_installation('python3')

postgres_opt = get_option('postgres')
libpq_dep = dependency('libpq', required : postgres_opt, fallback: ['postgresql', 'libpq'])

subdir('src')

if get_option('examples')
  subdir('examples')
endif

if get_option('tests')
  catch2_dep = dependency('catch2-with-main', fallback : ['catch2', 'catch2_with_main_dep'], default_options: ['tests=false'])
  subdir('tests')
endif

run_target(
  'clang-format',
  command : [
    python,
    files('tools/run_clang_format.py'),
    '--source-root',
    meson.project_source_root(),
  ],
)

run_target(
  'clang-format-check',
  command : [
    python,
    files('tools/run_clang_format.py'),
    '--source-root',
    meson.project_source_root(),
    '--check',
  ],
)

run_target(
  'clang-tidy',
  command : [
    python,
    files('tools/run_clang_tidy.py'),
    '--source-root',
    meson.project_source_root(),
    '--build-root',
    meson.project_build_root(),
  ],
)
