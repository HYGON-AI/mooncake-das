cmake_minimum_required(VERSION 3.16)
project(mooncake-ep LANGUAGES CXX)

set(MOONCAKE_EP_HCU_BUILD OFF)
if(MOONCAKE_GPU_BACKEND STREQUAL "hcu" OR USE_HCU OR
   "$ENV{MOONCAKE_GPU_BACKEND}" STREQUAL "hcu")
  set(MOONCAKE_EP_HCU_BUILD ON)
  if(NOT DTK_ROOT)
    if(DEFINED ENV{DTK_HOME} AND NOT "$ENV{DTK_HOME}" STREQUAL "")
      set(DTK_ROOT "$ENV{DTK_HOME}")
    else()
      set(DTK_ROOT "/opt/dtk")
    endif()
  endif()
  list(PREPEND CMAKE_PREFIX_PATH
       "${DTK_ROOT}/lib/cmake"
       "${DTK_ROOT}/lib64/cmake"
       "${DTK_ROOT}/hip/lib/cmake")
  if(NOT CMAKE_HIP_ARCHITECTURES)
    set(CMAKE_HIP_ARCHITECTURES gfx936)
  endif()
  enable_language(HIP)
elseif(USE_HIP OR MOONCAKE_GPU_BACKEND STREQUAL "hip" OR
       "$ENV{MOONCAKE_GPU_BACKEND}" STREQUAL "hip")
  message(FATAL_ERROR
          "Mooncake EP does not use the generic AMD USE_HIP backend; select USE_HCU for the Hygon DTK HIP path")
endif()

# Find PyTorch's CMake prefix path
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import torch; print(torch.utils.cmake_prefix_path)"
    OUTPUT_VARIABLE PYTORCH_CMAKE_PATH
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT PYTORCH_CMAKE_PATH)
  message(WARNING "Could not find PyTorch CMake path! Please set Torch_DIR.")
else ()
  message(STATUS "Found PyTorch CMake path: ${PYTORCH_CMAKE_PATH}")
  list(APPEND CMAKE_PREFIX_PATH "${PYTORCH_CMAKE_PATH}/Torch")
endif()

if(MOONCAKE_EP_HCU_BUILD)
  find_package(HIP REQUIRED)
else()
  set(TORCH_CUDA_ARCH_LIST "8.0;9.0")
  find_package(CUDAToolkit REQUIRED)
  # https://discuss.pytorch.org/t/failed-to-find-nvtoolsext/179635/13
  if(NOT TARGET CUDA::nvToolsExt AND TARGET CUDA::nvtx3)
    add_library(CUDA::nvToolsExt INTERFACE IMPORTED)
    target_compile_definitions(
        CUDA::nvToolsExt INTERFACE
        TORCH_CUDA_USE_NVTX3
    )
    target_link_libraries(CUDA::nvToolsExt INTERFACE CUDA::nvtx3)
  endif()
endif()
find_package(Torch REQUIRED)
if(MOONCAKE_EP_HCU_BUILD)
  if(PYTHON_EXECUTABLE)
    set(Python3_EXECUTABLE "${PYTHON_EXECUTABLE}")
  endif()
  find_package(Python3 REQUIRED COMPONENTS Development)
  execute_process(
      COMMAND ${PYTHON_EXECUTABLE} -c
              "import os, torch; print(os.path.join(os.path.dirname(torch.__file__), 'lib'))"
      OUTPUT_VARIABLE PYTORCH_LIBRARY_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE
  )
endif()
include_directories(${TORCH_INCLUDE_DIRS})
if(MOONCAKE_EP_HCU_BUILD)
  include_directories(${Python3_INCLUDE_DIRS})
endif()

include_directories(include)
add_subdirectory(src)
