cmake_minimum_required(VERSION 3.16)

project(mooncake_private_ext LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# `cmake --install` strips the build rpath, but the AHE provider still needs the
# device runtime (e.g. libgalaxyhip.so.5) at load time. Keep the directories of
# the libraries it links against so the installed provider resolves them
# without relying on an external LD_LIBRARY_PATH.
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# Public ABI header location. Override when building out of tree.
set(MOONCAKE_STORE_INCLUDE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-store/include"
    CACHE PATH "Path to mooncake-store/include")

# The provider libraries are installed next to the loader's search path.
set(MOONCAKE_PRIVATE_EXT_INSTALL_DIR
    "/opt/mooncake/plugins"
    CACHE PATH "Install directory for the private providers")

option(MOONCAKE_ENABLE_DFE "Build the DFS data-plane provider" ON)
option(MOONCAKE_ENABLE_AHE "Build the accelerator H2D provider (requires HIP)"
       OFF)

if(MOONCAKE_ENABLE_DFE)
    add_subdirectory(dfe)
endif()

if(MOONCAKE_ENABLE_AHE)
    add_subdirectory(ahe)
endif()
