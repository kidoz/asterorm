core_tests = executable(
  'core_tests',
  [
    'unit/test_core.cpp',
    'unit/test_mysql_dialect.cpp',
    'unit/test_pool.cpp',
    'unit/test_entity_traits.cpp',
    'unit/test_ast.cpp',
    'unit/test_codecs.cpp',
    'unit/test_transactions.cpp',
    'unit/test_typed_dsl.cpp',
    'unit/test_typed_query.cpp',
    'unit/test_select_locking.cpp',
    'unit/test_advisory_lock.cpp',
    'unit/test_typed_query_header.cpp',
    'compile/test_typed_dsl.cpp',
  ],
  dependencies : [asterorm_core_dep, catch2_dep],
)

test('core unit tests', core_tests)

test('typed query guide snippets', python_program, args : files('check_typed_query_guide.py'))

if get_option('examples')
  if sqlite_enabled
    test('typed query guide sqlite', typed_query_migration, args : ['sqlite'])
  endif
  if postgres_enabled
    test('typed query guide postgres', typed_query_migration, args : ['postgres'])
  endif
  if mysql_enabled
    test('typed query guide mysql', typed_query_migration, args : ['mysql'])
  endif
endif

test(
  'typed DSL compile checks',
  python_program,
  args : [files('compile/check_typed_dsl.py'), '--build-root', meson.project_build_root()],
  depends : core_tests,
  timeout : 180,
)

if clickhouse_feature.allowed()
  clickhouse_tests = executable(
    'clickhouse_tests',
    [
      'unit/test_clickhouse.cpp',
    ],
    dependencies : [asterorm_ch_dep, catch2_dep],
  )

  test('clickhouse unit tests', clickhouse_tests)
endif

if sqlite_enabled
  sqlite_tests = executable(
    'sqlite_integration',
    [
      'integration/test_sqlite_crud.cpp',
    ],
    dependencies : [asterorm_sqlite_dep, catch2_dep],
  )

  test('sqlite integration', sqlite_tests)
endif

if postgres_enabled
  pg_tests = executable(
    'pg_integration',
    [
      'integration/test_crud.cpp',
      'integration/test_transactions.cpp',
      'integration/test_advisory_lock.cpp',
      'integration/test_pool.cpp',
      'integration/test_sql.cpp',
      'integration/test_extras.cpp',
      'integration/test_arrays_copy.cpp',
      'integration/test_migrations.cpp',
    ],
    dependencies : [asterorm_pg_dep, catch2_dep],
  )

  test('pg integration', pg_tests)

  migration_worker = executable(
    'migration_worker',
    'support/migration_worker.cpp',
    dependencies : [asterorm_pg_dep],
  )
  test(
    'migration serialization',
    python_program,
    args : [files('check_migration_serialization.py'), migration_worker.full_path()],
    depends : migration_worker,
    is_parallel : false,
    timeout : 60,
  )

  add_test_setup(
    'postgres',
    env : [
      'ASTERORM_TEST_CONNINFO=host=127.0.0.1 port=5432 dbname=orm_test user=orm_test password=orm_test sslmode=disable'
    ],
  )
endif

if mysql_enabled
  mysql_unit_tests = executable(
    'mysql_unit_tests',
    ['unit/test_mysql.cpp'],
    dependencies : [asterorm_mysql_dep, catch2_dep],
  )
  test('mysql unit tests', mysql_unit_tests)
  mysql_tests = executable(
    'mysql_integration',
    ['integration/test_mysql_crud.cpp'],
    dependencies : [asterorm_mysql_dep, catch2_dep],
  )
  test('mysql integration', mysql_tests)
endif
