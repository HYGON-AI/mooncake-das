cmake_minimum_required(VERSION 3.16)

include("${CMAKE_CURRENT_LIST_DIR}/../mooncake-common/DetectGPUArchitectures.cmake")
if(NOT CMAKE_HIP_ARCHITECTURES)
  detect_gpu_architectures(CMAKE_HIP_ARCHITECTURES)
endif()
if(NOT CMAKE_HIP_ARCHITECTURES)
  set(CMAKE_HIP_ARCHITECTURES gfx936)
endif()
set(CMAKE_HIP_ARCHITECTURES "${CMAKE_HIP_ARCHITECTURES}" CACHE STRING
    "HIP architectures to build Mooncake EP for" FORCE)
message(STATUS "Building Mooncake EP for HIP architectures: ${CMAKE_HIP_ARCHITECTURES}")

project(mooncake-ep LANGUAGES CXX HIP)

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

find_package(HIP REQUIRED)
find_package(Torch REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Development)
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import os, torch; print(os.path.join(os.path.dirname(torch.__file__), 'lib'))"
    OUTPUT_VARIABLE PYTORCH_LIBRARY_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
include_directories(${TORCH_INCLUDE_DIRS})
include_directories(${Python3_INCLUDE_DIRS})

include_directories(include)
add_subdirectory(src)
