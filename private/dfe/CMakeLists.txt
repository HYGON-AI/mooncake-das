add_library(mooncake_dfe SHARED mooncake_dfe_extension.cpp)

target_include_directories(mooncake_dfe
                           PRIVATE "${MOONCAKE_STORE_INCLUDE_DIR}")

# No glog, no project headers: the provider depends only on libc and pthread so
# the master can load it without pulling in the client stack.
target_link_libraries(mooncake_dfe PRIVATE pthread)

set_target_properties(
    mooncake_dfe
    PROPERTIES OUTPUT_NAME mooncake_dfe
               VERSION 1.0.0
               SOVERSION 1
               CXX_VISIBILITY_PRESET hidden
               VISIBILITY_INLINES_HIDDEN ON)

install(TARGETS mooncake_dfe
        LIBRARY DESTINATION "${MOONCAKE_PRIVATE_EXT_INSTALL_DIR}")
