find_package(hip REQUIRED)
find_program(MOONCAKE_HIPCC_EXECUTABLE NAMES hipcc REQUIRED)

add_library(mooncake_ahe SHARED mooncake_ahe_extension.cpp)

target_include_directories(mooncake_ahe
                           PRIVATE "${MOONCAKE_STORE_INCLUDE_DIR}")

# The scatter kernel stays beside the provider: it is part of the private
# optimization and must not be shipped in the public tree.
set(AHE_KERNEL "${CMAKE_CURRENT_BINARY_DIR}/mc_copy_kernel.co")
add_custom_command(
    OUTPUT "${AHE_KERNEL}"
    COMMAND "${MOONCAKE_HIPCC_EXECUTABLE}" --genco -fPIC -x hip
            "${CMAKE_CURRENT_SOURCE_DIR}/mc_copy_kernel.hip" -o "${AHE_KERNEL}"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/mc_copy_kernel.hip"
    VERBATIM)
add_custom_target(mooncake_ahe_kernel ALL DEPENDS "${AHE_KERNEL}")
add_dependencies(mooncake_ahe mooncake_ahe_kernel)

# No glog, no project headers: the provider talks to the HIP runtime directly,
# so the client can load it without any other private symbol being visible.
target_link_libraries(mooncake_ahe PRIVATE hip::host pthread)

set_target_properties(
    mooncake_ahe
    PROPERTIES OUTPUT_NAME mooncake_ahe
               VERSION 1.0.0
               SOVERSION 1
               CXX_VISIBILITY_PRESET hidden
               VISIBILITY_INLINES_HIDDEN ON
               INSTALL_RPATH "$ORIGIN")

install(TARGETS mooncake_ahe
        LIBRARY DESTINATION "${MOONCAKE_PRIVATE_EXT_INSTALL_DIR}")
install(FILES "${AHE_KERNEL}"
        DESTINATION "${MOONCAKE_PRIVATE_EXT_INSTALL_DIR}")
