core_tests = executable(
  'core_tests',
  [
    'unit/test_core.cpp',
    'unit/test_pool.cpp',
    'unit/test_entity_traits.cpp',
    'unit/test_ast.cpp',
    'unit/test_codecs.cpp',
  ],
  dependencies : [asterorm_core_dep, catch2_dep],
)

test('core unit tests', core_tests)

if get_option('postgres').enabled()
  pg_tests = executable(
    'pg_integration',
    [
      'integration/test_crud.cpp',
      'integration/test_transactions.cpp',
      'integration/test_pool.cpp',
      'integration/test_sql.cpp',
      'integration/test_extras.cpp',
      'integration/test_arrays_copy.cpp',
    ],
    dependencies : [asterorm_pg_dep, catch2_dep],
  )

  test('pg integration', pg_tests)

  add_test_setup(
    'postgres',
    env : [
      'ASTERORM_TEST_CONNINFO=host=127.0.0.1 port=5432 dbname=orm_test user=orm_test password=orm_test sslmode=disable'
    ],
  )
endif
