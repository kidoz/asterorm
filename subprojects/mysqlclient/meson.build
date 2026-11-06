project('asterorm-mysqlclient', 'c', version : '3.4.11', meson_version : '>= 1.1.0')

# Require TLS support rather than letting the upstream build silently omit it.
dependency('openssl', required : true)
cmake = import('cmake')
client_options = cmake.subproject_options()
client_options.add_cmake_defines({
  'WITH_UNIT_TESTS' : false,
  'WITH_EXTERNAL_ZLIB' : true,
  'WITH_CURL' : false,
  'WITH_SSL' : 'OPENSSL',
  'WITH_MYSQLCOMPAT' : false,
  'INSTALL_LIBDIR' : get_option('libdir'),
  'CLIENT_PLUGIN_MYSQL_CLEAR_PASSWORD' : 'OFF',
  'CLIENT_PLUGIN_ZSTD' : 'OFF',
  # Keep MySQL authentication available without loading runtime plugins.
  'CLIENT_PLUGIN_CACHING_SHA2_PASSWORD' : 'STATIC',
  'CLIENT_PLUGIN_SHA256_PASSWORD' : 'STATIC',
  'CLIENT_PLUGIN_DIALOG' : 'OFF',
  'CLIENT_PLUGIN_CLIENT_ED25519' : 'OFF',
  'CLIENT_PLUGIN_AUTH_GSSAPI_CLIENT' : 'OFF',
  'CLIENT_PLUGIN_PARSEC' : 'OFF',
})
client_options.set_install(false)
client_options.set_install(true, target : 'libmariadb')
client_options.set_override_option('warning_level', '0')
client_options.set_override_option('werror', 'false')
client_project = cmake.subproject('mariadb-connector-c', options : client_options)
mysqlclient_dep = declare_dependency(
  dependencies : client_project.dependency('libmariadb'),
  variables : {'pkgconfig_name' : 'asterorm_mysqlclient'},
)
meson.override_dependency('mysqlclient', mysqlclient_dep)

# Meson's CMake bridge imports build targets; install the matching public
# headers and package metadata explicitly so consumers use the same C API ABI.
client_source_dir = meson.current_source_dir() / '..' / 'mariadb-connector-c-3.4.11'
client_generated_dir = meson.current_build_dir() / '..' / 'mariadb-connector-c-3.4.11' / '__CMake_build' / 'include'
client_include_dir = get_option('includedir') / 'asterorm' / 'mysqlclient'
# The CMake bridge does not export header File objects across the subproject
# boundary. Stage the pinned source/generated headers with installable File objects.
# Plugin-development headers require upstream internal C headers and are not
# part of the AsterORM consumer API, so only the client API is installed.
cmake_program = find_program('cmake', required : true)
foreach header : ['mysql.h', 'mariadb_com.h', 'mariadb_stmt.h', 'ma_pvio.h', 'ma_tls.h',
                  'ma_list.h', 'errmsg.h', 'mariadb_dyncol.h', 'mariadb_ctype.h',
                  'mariadb_rpl.h', 'mysqld_error.h']
  configure_file(
    output : header,
    command : [cmake_program, '-E', 'copy_if_different',
               client_source_dir / 'include' / header, '@OUTPUT@'],
    install : true,
    install_dir : client_include_dir,
  )
endforeach
configure_file(
  output : 'mariadb_version.h',
  command : [cmake_program, '-E', 'copy_if_different',
             client_generated_dir / 'mariadb_version.h', '@OUTPUT@'],
  install : true,
  install_dir : client_include_dir,
)
configure_file(
  output : 'ma_io.h',
  command : [cmake_program, '-E', 'copy_if_different',
             client_source_dir / 'include' / 'mariadb' / 'ma_io.h', '@OUTPUT@'],
  install : true,
  install_dir : client_include_dir / 'mariadb',
)
configure_file(
  output : 'COPYING.LIB',
  command : [cmake_program, '-E', 'copy_if_different',
             client_source_dir / 'COPYING.LIB', '@OUTPUT@'],
  install : true,
  install_dir : get_option('datadir') / 'licenses' / 'asterorm' / 'mysqlclient',
)

import('pkgconfig').generate(
  client_project.target('libmariadb'),
  name : 'AsterORM MySQL client',
  filebase : 'asterorm_mysqlclient',
  description : 'Bundled MariaDB Connector/C for the AsterORM MySQL adapter',
  version : meson.project_version(),
  subdirs : ['asterorm/mysqlclient'],
)
