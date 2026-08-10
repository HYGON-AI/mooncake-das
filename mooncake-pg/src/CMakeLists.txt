add_library(mooncake_pg MODULE connection_poller.cpp mooncake_backend.cpp mooncake_worker.cu mooncake_worker_thread.cpp p2p_proxy.cpp pg_py.cpp)

set_source_files_properties(mooncake_worker.cu PROPERTIES LANGUAGE HIP)
target_include_directories(mooncake_pg PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../include"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-common/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-common/etcd")
set(MOONCAKE_PG_MODULE_NAME "mooncake_pg" CACHE STRING "PG Python module name")
set(MOONCAKE_EXTENSION_SUFFIX ".so" CACHE STRING "Python extension suffix")
set_target_properties(mooncake_pg PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    PREFIX ""
    OUTPUT_NAME "${MOONCAKE_PG_MODULE_NAME}"
    SUFFIX "${MOONCAKE_EXTENSION_SUFFIX}"
    LINKER_LANGUAGE CXX)
target_compile_definitions(mooncake_pg PRIVATE
    TORCH_EXTENSION_NAME=${MOONCAKE_PG_MODULE_NAME}
    TORCH_API_INCLUDE_EXTENSION_H=1
    _GLIBCXX_USE_CXX11_ABI=${TORCH_CXX11_ABI}
    __HIP_PLATFORM_AMD__=1
    USE_ROCM=1
    HIPBLAS_V2=1)
if(MOONCAKE_PG_DISABLE_DEFAULT_STREAM_BRIDGE)
  target_compile_definitions(
      mooncake_pg PRIVATE MOONCAKE_PG_DISABLE_DEFAULT_STREAM_BRIDGE=1)
endif()
target_compile_features(mooncake_pg PRIVATE cxx_std_20)
target_compile_options(mooncake_pg PRIVATE -O3 -g0)
target_link_options(mooncake_pg PRIVATE "-Wl,-Bsymbolic-functions")

if(TARGET transfer_engine)
  target_link_libraries(mooncake_pg PRIVATE transfer_engine)
else()
  find_library(MOONCAKE_ENGINE_LIBRARY NAMES engine.so engine
      PATHS
          "${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-wheel/mooncake"
          "/usr/local/lib/python3.10/dist-packages/mooncake"
      NO_DEFAULT_PATH REQUIRED)
  target_link_options(mooncake_pg PRIVATE
      "-L${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-wheel/mooncake"
      "-L/usr/local/lib/python3.10/dist-packages/mooncake"
      "-Wl,--no-as-needed"
      "-l:engine.so"
      "-Wl,--as-needed")
  set_target_properties(mooncake_pg PROPERTIES BUILD_RPATH "$ORIGIN")
endif()
foreach(_torch_lib c10 torch torch_cpu torch_python c10_hip torch_hip)
  find_library(MOONCAKE_${_torch_lib}_LIBRARY NAMES ${_torch_lib}
      PATHS "${PYTORCH_LIBRARY_DIR}" NO_DEFAULT_PATH REQUIRED)
  target_link_libraries(mooncake_pg PRIVATE "${MOONCAKE_${_torch_lib}_LIBRARY}")
endforeach()
find_library(MOONCAKE_HIP_RUNTIME_LIBRARY NAMES amdhip64
    PATHS "/opt/dtk/lib" "/opt/dtk/hip/lib" REQUIRED)
target_link_libraries(mooncake_pg PRIVATE
    "${MOONCAKE_HIP_RUNTIME_LIBRARY}" ibverbs mlx5)
