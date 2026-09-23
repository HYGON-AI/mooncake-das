file(GLOB HIP_TRANSPORT_SOURCES "*.cpp")

# mc_copy_kernel.co is the default VMM fabric copy (MCCopyKernel).
# MC_HYLINK_USE_KERNEL_COPY=0 selects hipMemcpyAsync at runtime.
if(USE_HYGON)
    set(COPY_KERNEL ${CMAKE_CURRENT_BINARY_DIR}/mc_copy_kernel.co)
    add_custom_command(
        OUTPUT ${COPY_KERNEL}
        COMMAND hipcc --genco -fPIC -x hip
                ${CMAKE_CURRENT_SOURCE_DIR}/mc_copy_kernel.hip
                -o ${COPY_KERNEL}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/mc_copy_kernel.hip
        VERBATIM
    )
    add_library(hip_transport OBJECT ${HIP_TRANSPORT_SOURCES} ${COPY_KERNEL})
else()
    add_library(hip_transport OBJECT ${HIP_TRANSPORT_SOURCES})
endif()

target_link_libraries(hip_transport PRIVATE glog::glog)
target_include_directories(hip_transport PUBLIC ${HIP_INCLUDE_DIRS})
target_compile_options(hip_transport PRIVATE -Werror)
