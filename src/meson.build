inc = include_directories('../include')
pool_threads = dependency('threads')

asterorm_core = library(
  'asterorm_core',
  [
    'core/error.cpp',
    'core/result.cpp',
    'core/session.cpp',
    'pool/connection_pool.cpp',
    'sql/ast.cpp',
    'sql/compiler.cpp',
    'sql/query.cpp',
    'sql/window.cpp',
    'crud/repository.cpp',
  ],
  include_directories : inc,
  dependencies : pool_threads,
  install : true,
)

asterorm_core_dep = declare_dependency(
  include_directories : inc,
  dependencies : pool_threads,
  link_with : asterorm_core,
)

asterorm_libraries = [asterorm_core]

subdir('database')
