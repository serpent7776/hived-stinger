file(GLOB HEADERS "include/hive/plugins/stinger/*.hpp")

add_library( stinger_plugin
             stinger_plugin.cpp
             ${HEADERS}
           )

target_link_libraries( stinger_plugin chain_plugin )
target_include_directories( stinger_plugin
                            PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" )

if( CLANG_TIDY_EXE )
   set_target_properties(
      stinger_plugin PROPERTIES
      CXX_CLANG_TIDY "${DO_CLANG_TIDY}"
   )
endif( CLANG_TIDY_EXE )

install( TARGETS
   stinger_plugin

   RUNTIME DESTINATION bin
   LIBRARY DESTINATION lib
   ARCHIVE DESTINATION lib
)
