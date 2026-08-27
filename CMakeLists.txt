cmake_minimum_required(VERSION 3.25)

project(
    mini_ocs_nos
    VERSION 0.1.0
    DESCRIPTION "SONiC-inspired control plane for a simulated optical circuit switch"
    LANGUAGES CXX
)

include(CTest)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(cpp)
