if get_option('optimization') not in ['2', '3']
  error('DSL benchmarks require optimization=2 or 3; use --buildtype=release.')
endif
if get_option('default_library') != 'static' or get_option('b_lto')
  error('DSL benchmarks require --default-library=static and -Db_lto=false for allocation counting and the observation barrier.')
endif

dsl_benchmark = executable(
  'dsl_benchmark',
  ['dsl_benchmark.cpp', 'measurement_support.cpp'],
  dependencies : asterorm_core_dep,
  install : false,
)
dsl_allocations = executable(
  'dsl_allocations',
  ['dsl_benchmark.cpp', 'measurement_support.cpp', 'allocation_probe.cpp'],
  cpp_args : ['-DASTERORM_BENCH_ALLOCATIONS'],
  dependencies : asterorm_core_dep,
  install : false,
)

test('DSL benchmark verification', dsl_benchmark, args : ['--verify-only'])
test('DSL allocation probe verification', dsl_allocations, args : ['--verify-only'])
