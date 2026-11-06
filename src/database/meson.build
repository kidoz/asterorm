if postgres_enabled
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

if sqlite_enabled
  asterorm_sqlite = library(
    'asterorm_sqlite',
    [
      'sqlite/driver.cpp',
      'sqlite/connection.cpp',
      'sqlite/result.cpp',
    ],
    include_directories : inc,
    dependencies : [asterorm_core_dep, sqlite_dep],
    install : true,
  )

  asterorm_sqlite_dep = declare_dependency(
    include_directories : inc,
    dependencies : [asterorm_core_dep, sqlite_dep],
    link_with : asterorm_sqlite,
  )

  asterorm_libraries += [asterorm_sqlite]
endif

if clickhouse_feature.allowed()
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

if mysql_enabled
  asterorm_mysql = library(
    'asterorm_mysql',
    ['mysql/driver.cpp', 'mysql/connection.cpp', 'mysql/result.cpp'],
    include_directories : inc,
    dependencies : [asterorm_core_dep, mysqlclient_dep],
    install : true,
  )
  asterorm_mysql_dep = declare_dependency(
    include_directories : inc,
    dependencies : [asterorm_core_dep, mysqlclient_dep],
    link_with : asterorm_mysql,
  )
  asterorm_libraries += [asterorm_mysql]
endif
