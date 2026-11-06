if postgres_enabled
  executable(
    'quickstart',
    'quickstart.cpp',
    include_directories : inc,
    dependencies : [asterorm_core_dep, asterorm_pg_dep],
  )
endif

if postgres_enabled or sqlite_enabled or mysql_enabled
  migration_dependencies = [asterorm_core_dep]
  migration_cpp_args = []
  if postgres_enabled
    migration_dependencies += [asterorm_pg_dep]
    migration_cpp_args += ['-DASTERORM_EXAMPLE_POSTGRES']
  endif
  if sqlite_enabled
    migration_dependencies += [asterorm_sqlite_dep]
    migration_cpp_args += ['-DASTERORM_EXAMPLE_SQLITE']
  endif
  if mysql_enabled
    migration_dependencies += [asterorm_mysql_dep]
    migration_cpp_args += ['-DASTERORM_EXAMPLE_MYSQL']
  endif
  typed_query_migration = executable(
    'typed_query_migration',
    'typed_query_migration.cpp',
    dependencies : migration_dependencies,
    cpp_args : migration_cpp_args,
  )
endif
