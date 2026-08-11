if(MOONCAKE_EP_HCU_BUILD)
  add_library(mooncake_ep MODULE
      ep_py.cpp
      mooncake_ep_buffer.cpp
      mooncake_ep_kernel.cu)
  set_source_files_properties(
      mooncake_ep_kernel.cu PROPERTIES LANGUAGE HIP)
else()
  add_library(mooncake_ep ep_py.cpp mooncake_ep_buffer.cpp mooncake_ep_kernel.cu)
endif()

set_target_properties(mooncake_ep PROPERTIES POSITION_INDEPENDENT_CODE ON)
if(MOONCAKE_EP_HCU_BUILD)
  target_include_directories(mooncake_ep PRIVATE
      "${CMAKE_CURRENT_SOURCE_DIR}/../include"
      "${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include")
  set(MOONCAKE_EP_MODULE_NAME "mooncake_ep" CACHE STRING
      "EP Python module name")
  set(MOONCAKE_EXTENSION_SUFFIX ".so" CACHE STRING
      "Python extension suffix")
  set_target_properties(mooncake_ep PROPERTIES
      PREFIX ""
      OUTPUT_NAME "${MOONCAKE_EP_MODULE_NAME}"
      SUFFIX "${MOONCAKE_EXTENSION_SUFFIX}"
      LINKER_LANGUAGE CXX)
  target_compile_definitions(mooncake_ep PRIVATE
      TORCH_EXTENSION_NAME=${MOONCAKE_EP_MODULE_NAME}
      TORCH_API_INCLUDE_EXTENSION_H=1
      _GLIBCXX_USE_CXX11_ABI=${TORCH_CXX11_ABI}
      __HIP_PLATFORM_AMD__=1
      USE_HCU=1
      USE_ROCM=1
      HIPBLAS_V2=1
      MOONCAKE_EP_USE_HCU=1)
  target_compile_features(mooncake_ep PRIVATE cxx_std_20)
  target_compile_options(mooncake_ep PRIVATE -O3 -g0)
  target_link_options(mooncake_ep PRIVATE "-Wl,-Bsymbolic-functions")

  if(TARGET transfer_engine)
    target_link_libraries(mooncake_ep PRIVATE transfer_engine)
  else()
    # engine.so is a Python extension module and intentionally has no "lib"
    # prefix, so find_file() is required here; find_library() would search for
    # libengine.so instead.
    find_file(MOONCAKE_ENGINE_LIBRARY NAMES engine.so
        PATHS
            "${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-wheel/mooncake"
            "/usr/local/lib/python3.10/dist-packages/mooncake"
        NO_DEFAULT_PATH REQUIRED)
    get_filename_component(
        MOONCAKE_ENGINE_LIBRARY_DIR "${MOONCAKE_ENGINE_LIBRARY}" DIRECTORY)
    target_link_directories(
        mooncake_ep PRIVATE "${MOONCAKE_ENGINE_LIBRARY_DIR}")
    target_link_libraries(mooncake_ep PRIVATE "-Wl,-l:engine.so")
    set_target_properties(mooncake_ep PROPERTIES BUILD_RPATH "$ORIGIN")
  endif()

  foreach(_torch_lib c10 torch torch_cpu torch_python c10_hip torch_hip)
    find_library(MOONCAKE_${_torch_lib}_LIBRARY NAMES ${_torch_lib}
        PATHS "${PYTORCH_LIBRARY_DIR}" NO_DEFAULT_PATH REQUIRED)
    target_link_libraries(
        mooncake_ep PRIVATE "${MOONCAKE_${_torch_lib}_LIBRARY}")
  endforeach()
  find_library(MOONCAKE_HCU_RUNTIME_LIBRARY NAMES amdhip64
      PATHS
          "$ENV{ROCM_PATH}/lib"
          "$ENV{HIP_PATH}/lib"
          "${DTK_ROOT}/lib"
          "${DTK_ROOT}/hip/lib"
      REQUIRED)
  target_link_libraries(mooncake_ep PRIVATE
      "${MOONCAKE_HCU_RUNTIME_LIBRARY}" ibverbs mlx5)
else()
  target_link_libraries(
      mooncake_ep PUBLIC ${TORCH_LIBRARIES} transfer_engine ibverbs mlx5)
endif()
