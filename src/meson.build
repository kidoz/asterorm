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

if postgres_opt.allowed()
  asterorm_pg = library(
    'asterorm_pg',
    [
      'postgres/driver.cpp',
      'postgres/connection.cpp',
      'postgres/result.cpp',
      'postgres/codecs.cpp',
    ],
    include_directories : inc,
    dependencies : [asterorm_core_dep, libpq_dep],
    install : true,
  )

  asterorm_pg_dep = declare_dependency(
    include_directories : inc,
    dependencies : [asterorm_core_dep, libpq_dep],
    link_with : asterorm_pg,
  )

  asterorm_libraries += [asterorm_pg]
endif

if clickhouse_opt.allowed()
  asterorm_ch = library(
    'asterorm_ch',
    [
      'clickhouse/driver.cpp',
      'clickhouse/connection.cpp',
      'clickhouse/result.cpp',
      'clickhouse/codecs.cpp',
    ],
    include_directories : inc,
    dependencies : [asterorm_core_dep, clickhouse_dep],
    install : true,
  )

  asterorm_ch_dep = declare_dependency(
    include_directories : inc,
    dependencies : [asterorm_core_dep, clickhouse_dep],
    link_with : asterorm_ch,
  )

  asterorm_libraries += [asterorm_ch]
endif
