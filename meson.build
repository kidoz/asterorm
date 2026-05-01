project(
  'asterorm',
  'cpp',
  version : '0.1.0',
  meson_version : '>= 1.1.0',
  default_options : [
    'cpp_std=c++23',
    'warning_level=3',
    'default_library=shared',
  ],
)

add_project_arguments('-Werror', language: 'cpp')

python_program = import('python').find_installation('python3')
pkgconfig = import('pkgconfig')
filesystem_module = import('fs')
cpp_compiler = meson.get_compiler('cpp')

postgres_feature = get_option('postgres')
libpq_dep = dependency('libpq', required : false)
if not libpq_dep.found() and postgres_feature.allowed()
  foreach libpq_prefix : ['/opt/homebrew/opt/libpq', '/usr/local/opt/libpq']
    if not libpq_dep.found() and filesystem_module.exists(libpq_prefix / 'include' / 'libpq-fe.h')
      libpq_library = cpp_compiler.find_library('pq', dirs : libpq_prefix / 'lib', required : false)
      if libpq_library.found()
        libpq_dep = declare_dependency(
          dependencies : libpq_library,
          compile_args : ['-I' + (libpq_prefix / 'include')],
        )
      endif
    endif
  endforeach
endif
if postgres_feature.enabled() and not libpq_dep.found()
  error('libpq dependency not found. Install libpq development headers or set PKG_CONFIG_PATH.')
endif
postgres_enabled = postgres_feature.allowed() and libpq_dep.found()
asterorm_pkg_requires_private = []
if postgres_enabled
  asterorm_pkg_requires_private += ['libpq']
endif

clickhouse_feature = get_option('clickhouse')
clickhouse_dep = dependency('clickhouse-cpp', required : clickhouse_feature, fallback: ['clickhouse-cpp', 'clickhouse_cpp_lib_dep'], default_options: ['werror=false', 'warning_level=0'])

subdir('src')

install_subdir('include/asterorm', install_dir : get_option('includedir'))
install_data(
  'cmake/AsterORMConfig.cmake',
  'cmake/AsterORMConfigVersion.cmake',
  install_dir : get_option('libdir') / 'cmake' / 'AsterORM',
)

pkgconfig.generate(
  name : 'AsterORM',
  filebase : 'asterorm',
  description : 'PostgreSQL-first ORM for C++23',
  version : meson.project_version(),
  libraries : asterorm_libraries,
  requires_private : asterorm_pkg_requires_private,
  subdirs : ['.'],
)

if get_option('examples')
  subdir('examples')
endif

if get_option('tests')
  catch2_dep = dependency('catch2-with-main', fallback : ['catch2', 'catch2_with_main_dep'], default_options: ['tests=false', 'install=false'])
  subdir('tests')
endif

run_target(
  'clang-format',
  command : [
    python_program,
    files('tools/run_clang_format.py'),
    '--source-root',
    meson.project_source_root(),
  ],
)

run_target(
  'clang-format-check',
  command : [
    python_program,
    files('tools/run_clang_format.py'),
    '--source-root',
    meson.project_source_root(),
    '--check',
  ],
)

run_target(
  'clang-tidy',
  command : [
    python_program,
    files('tools/run_clang_tidy.py'),
    '--source-root',
    meson.project_source_root(),
    '--build-root',
    meson.project_build_root(),
  ],
)
