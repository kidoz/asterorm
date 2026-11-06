inc = include_directories('../include')

asterorm_core = library(
  'asterorm_core',
  [
    'core/error.cpp',
    'core/result.cpp',
    'core/session.cpp',
    'pool/connection_pool.cpp',
    'sql/ast.cpp',
    'sql/compiler.cpp',
    'crud/repository.cpp',
  ],
  include_directories : inc,
  install : true,
)

asterorm_core_dep = declare_dependency(
  include_directories : inc,
  link_with : asterorm_core,
)

asterorm_libraries = [asterorm_core]

subdir('database')
