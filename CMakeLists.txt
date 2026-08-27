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

option(OCS_ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
add_library(ocs_sanitizers INTERFACE)
if(OCS_ENABLE_SANITIZERS)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(
            ocs_sanitizers
            INTERFACE
                -fsanitize=address,undefined
                -fno-omit-frame-pointer
        )
        target_link_options(ocs_sanitizers INTERFACE -fsanitize=address,undefined)
    else()
        message(FATAL_ERROR "OCS_ENABLE_SANITIZERS requires GCC or Clang")
    endif()
endif()

add_subdirectory(cpp)
