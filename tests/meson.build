core_tests = executable(
  'core_tests',
  [
    'unit/test_core.cpp',
    'unit/test_mysql_dialect.cpp',
    'unit/test_pool.cpp',
    'unit/test_pool_timeout.cpp',
    'unit/test_entity_traits.cpp',
    'unit/test_ast.cpp',
    'unit/test_query.cpp',
    'unit/test_bulk_predicate.cpp',
    'unit/test_codecs.cpp',
    'unit/test_transactions.cpp',
    'unit/test_transactional_session.cpp',
    'unit/test_typed_dsl.cpp',
    'unit/test_typed_query.cpp',
    'unit/test_select_locking.cpp',
    'unit/test_advisory_lock.cpp',
    'unit/test_timeout_scope.cpp',
    'unit/test_stream_reservation.cpp',
    'unit/test_repository_stream.cpp',
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
  catch2_harness = executable('catch2_harness',
    ['support/catch2_harness_main.cpp', 'integration/test_catch2_harness.cpp'],
    dependencies : [asterorm_sqlite_dep, catch2_runner_dep, dependency('threads')],
  )
  test('Catch2 transactional harness', python_program,
    args : [files('check_catch2_harness.py'), catch2_harness],
    depends : catch2_harness,
    timeout : 60,
  )
  sqlite_unit_tests = executable(
    'sqlite_unit_tests',
    'unit/test_sqlite_result.cpp',
    dependencies : [asterorm_sqlite_dep, catch2_dep],
  )
  test('sqlite unit tests', sqlite_unit_tests)
  test('transactional scope guide', python_program,
    args : [files('check_transactional_guide.py'), '--build-root', meson.project_build_root()],
  )
  test('ordinary CTE guide', python_program,
    args : [files('check_cte_guide.py'), '--build-root', meson.project_build_root()],
  )
  test('bulk mutation guide', python_program,
    args : [files('check_bulk_predicate_guide.py'), '--build-root', meson.project_build_root()],
  )
  sqlite_tests = executable(
    'sqlite_integration',
    [
      'support/sqlite_integration_main.cpp',
      'integration/test_sqlite_crud.cpp',
      'integration/test_sqlite_reference_fixture.cpp',
    ],
    dependencies : [asterorm_sqlite_dep, catch2_runner_dep],
  )

  test('sqlite integration', sqlite_tests)
  sqlite_reference_probe = executable('sqlite_reference_probe',
    ['support/sqlite_integration_main.cpp', 'integration/test_sqlite_reference_probe.cpp'],
    dependencies : [asterorm_sqlite_dep, catch2_runner_dep],
  )
  test('SQLite reference fixtures', python_program,
    args : [files('check_reference_fixtures.py'), '--backend', 'sqlite', '--integration', sqlite_tests,
            '--probe', sqlite_reference_probe],
    depends : [sqlite_tests, sqlite_reference_probe],
    timeout : 60,
  )
endif

if postgres_enabled
  postgres_unit_tests = executable(
    'postgres_unit_tests',
    ['unit/test_postgres_connection.cpp', 'unit/test_postgres_single_row.cpp'],
    dependencies : [asterorm_pg_dep, catch2_dep],
  )
  test('postgres unit tests', postgres_unit_tests)
  test('postgres single-row guide', python_program,
    args : [files('check_single_row_guide.py'), '--build-root', meson.project_build_root()],
  )
  test('repository stream guide', python_program,
    args : [files('check_single_row_guide.py'), '--build-root', meson.project_build_root(),
            '--guide', 'docs/how-to/stream-entities.md'],
  )

  pg_tests = executable(
    'pg_integration',
    [
      'support/pg_integration_main.cpp',
      'integration/test_crud.cpp',
      'integration/test_transactions.cpp',
      'integration/test_transactional_session.cpp',
      'integration/test_advisory_lock.cpp',
      'integration/test_pool.cpp',
      'integration/test_statement_timeout.cpp',
      'integration/test_single_row.cpp',
      'integration/test_cursor.cpp',
      'integration/test_repository_stream.cpp',
      'integration/test_sql.cpp',
      'integration/test_extras.cpp',
      'integration/test_arrays_copy.cpp',
      'integration/test_migrations.cpp',
    ],
    dependencies : [asterorm_pg_dep, catch2_runner_dep],
  )

  test('pg integration', pg_tests)
  pg_reference_probe = executable('pg_reference_probe',
    ['support/pg_integration_main.cpp', 'integration/test_pg_reference_fixture.cpp'],
    dependencies : [asterorm_pg_dep, catch2_runner_dep],
  )
  test('PostgreSQL reference fixtures', python_program,
    args : [files('check_reference_fixtures.py'), '--backend', 'pg', '--integration', pg_tests,
            '--probe', pg_reference_probe],
    depends : [pg_tests, pg_reference_probe],
    timeout : 60,
  )
  test('repository stream memory', python_program,
    args : [files('check_repository_stream_memory.py'), pg_tests],
    depends : pg_tests,
    timeout : 60,
  )
  test('repository cursor memory', python_program,
    args : [files('check_repository_stream_memory.py'), pg_tests, '[.repository-cursor-memory]'],
    depends : pg_tests,
    timeout : 60,
  )
  test('repository stream memory controls', python_program,
    args : [files('check_repository_stream_memory.py'), pg_tests, '--controls'],
    depends : pg_tests,
    timeout : 180,
  )

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
