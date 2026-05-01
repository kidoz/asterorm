include(CMakeFindDependencyMacro)

get_filename_component(_asterorm_prefix "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)

if(NOT TARGET AsterORM::core)
  add_library(AsterORM::core SHARED IMPORTED)
  set_target_properties(AsterORM::core PROPERTIES
    IMPORTED_LOCATION "${_asterorm_prefix}/lib/libasterorm_core${CMAKE_SHARED_LIBRARY_SUFFIX}"
    INTERFACE_INCLUDE_DIRECTORIES "${_asterorm_prefix}/include"
  )
endif()

if(EXISTS "${_asterorm_prefix}/lib/libasterorm_pg${CMAKE_SHARED_LIBRARY_SUFFIX}" AND NOT TARGET AsterORM::postgres)
  find_package(PostgreSQL QUIET)
  add_library(AsterORM::postgres SHARED IMPORTED)
  set_target_properties(AsterORM::postgres PROPERTIES
    IMPORTED_LOCATION "${_asterorm_prefix}/lib/libasterorm_pg${CMAKE_SHARED_LIBRARY_SUFFIX}"
    INTERFACE_INCLUDE_DIRECTORIES "${_asterorm_prefix}/include"
    INTERFACE_LINK_LIBRARIES "AsterORM::core"
  )
  if(TARGET PostgreSQL::PostgreSQL)
    set_property(TARGET AsterORM::postgres APPEND PROPERTY
      INTERFACE_LINK_LIBRARIES PostgreSQL::PostgreSQL)
  endif()
endif()

if(EXISTS "${_asterorm_prefix}/lib/libasterorm_ch${CMAKE_SHARED_LIBRARY_SUFFIX}" AND NOT TARGET AsterORM::clickhouse)
  add_library(AsterORM::clickhouse SHARED IMPORTED)
  set_target_properties(AsterORM::clickhouse PROPERTIES
    IMPORTED_LOCATION "${_asterorm_prefix}/lib/libasterorm_ch${CMAKE_SHARED_LIBRARY_SUFFIX}"
    INTERFACE_INCLUDE_DIRECTORIES "${_asterorm_prefix}/include"
    INTERFACE_LINK_LIBRARIES "AsterORM::core"
  )
endif()

if(NOT TARGET AsterORM::asterorm)
  add_library(AsterORM::asterorm INTERFACE IMPORTED)
  set(_asterorm_components AsterORM::core)
  if(TARGET AsterORM::postgres)
    list(APPEND _asterorm_components AsterORM::postgres)
  endif()
  if(TARGET AsterORM::clickhouse)
    list(APPEND _asterorm_components AsterORM::clickhouse)
  endif()
  set_target_properties(AsterORM::asterorm PROPERTIES
    INTERFACE_LINK_LIBRARIES "${_asterorm_components}"
  )
endif()

