include(FetchContent)

set(JSON_BuildTests OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    nlohmann_json
    URL https://github.com/nlohmann/json/archive/refs/tags/v3.12.0.tar.gz
    URL_HASH SHA256=4b92eb0c06d10683f7447ce9406cb97cd4b453be18d7279320f7b2f025c10187
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(nlohmann_json)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(DISABLE_TESTS ON CACHE BOOL "" FORCE)
set(ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
set(ENABLE_NUGET OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    hiredis
    URL https://github.com/redis/hiredis/archive/refs/tags/v1.3.0.tar.gz
    URL_HASH SHA256=25cee4500f359cf5cad3b51ed62059aadfc0939b05150c1f19c7e2829123631c
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SOURCE_DIR "${CMAKE_BINARY_DIR}/_deps/hiredis"
    OVERRIDE_FIND_PACKAGE
)
FetchContent_MakeAvailable(hiredis)
get_filename_component(HIREDIS_INCLUDE_PARENT "${hiredis_SOURCE_DIR}" DIRECTORY)
set(hiredis_INCLUDE_DIRS "${HIREDIS_INCLUDE_PARENT}")

set(REDIS_PLUS_PLUS_BUILD_TEST OFF CACHE BOOL "" FORCE)
set(REDIS_PLUS_PLUS_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(REDIS_PLUS_PLUS_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(REDIS_PLUS_PLUS_CXX_STANDARD 20 CACHE STRING "" FORCE)
FetchContent_Declare(
    redis_plus_plus
    URL https://github.com/sewenew/redis-plus-plus/archive/refs/tags/1.3.15.tar.gz
    URL_HASH SHA256=e9288fda74f1501c62dd9ab7d6a8dc67de51aa4d5b7c71770baa51987a945941
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(redis_plus_plus)

add_library(
    ocs_core STATIC
    src/connection_state_machine.cpp
    src/device_command.cpp
    src/errors.cpp
    src/in_process_sim_backend.cpp
    src/orchestrator_service.cpp
    src/reconciler.cpp
    src/redis_repository.cpp
    src/simulated_ocs_device.cpp
    src/syncd_service.cpp
    src/uds_codec.cpp
    src/uds_device_backend.cpp
    src/uds_json.cpp
    src/uds_server.cpp
    src/version.cpp
)
add_library(ocs::core ALIAS ocs_core)

target_include_directories(
    ocs_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_compile_features(ocs_core PUBLIC cxx_std_20)
target_link_libraries(
    ocs_core
    PRIVATE
        nlohmann_json::nlohmann_json
        redis++::redis++_static
)
target_compile_options(
    ocs_core
    PRIVATE
        $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall;-Wextra;-Wpedantic;-Werror>
)
# hiredis is a C library whose public header intentionally uses flexible arrays.
# Keep project warnings strict while suppressing only that C-extension warning in
# the adapter translation unit compiled as C++.
set_source_files_properties(src/redis_repository.cpp PROPERTIES COMPILE_OPTIONS "-Wno-pedantic")

add_executable(ocs-hwsim ocs-hwsim/main.cpp)
target_link_libraries(ocs-hwsim PRIVATE ocs::core)
target_compile_options(
    ocs-hwsim
    PRIVATE
        $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall;-Wextra;-Wpedantic;-Werror>
)

add_executable(ocs-syncd ocs-syncd/main.cpp)
target_link_libraries(ocs-syncd PRIVATE ocs::core)
target_compile_options(
    ocs-syncd
    PRIVATE
        $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall;-Wextra;-Wpedantic;-Werror>
)

add_executable(ocs-orch ocs-orch/main.cpp)
target_link_libraries(ocs-orch PRIVATE ocs::core)
target_compile_options(
    ocs-orch
    PRIVATE
        $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall;-Wextra;-Wpedantic;-Werror>
)

if(BUILD_TESTING)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.17.0.tar.gz
        URL_HASH SHA256=65fab701d9829d38cb77c14acdc431d2108bfdbf8979e40eb8ae567edf10b27c
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    FetchContent_MakeAvailable(googletest)

    add_executable(
        ocs_core_test
        tests/device_test.cpp
        tests/orchestrator_test.cpp
        tests/reconciler_test.cpp
        tests/redis_contract_test.cpp
        tests/syncd_integration_test.cpp
        tests/uds_codec_test.cpp
        tests/uds_transport_test.cpp
        tests/version_test.cpp
    )
    add_dependencies(ocs_core_test ocs-hwsim)
    target_link_libraries(ocs_core_test PRIVATE ocs::core GTest::gtest_main)
    target_compile_definitions(ocs_core_test PRIVATE OCS_HWSIM_PATH="$<TARGET_FILE:ocs-hwsim>")
    target_compile_options(
        ocs_core_test
        PRIVATE
            $<$<CXX_COMPILER_ID:GNU,Clang>:-Wall;-Wextra;-Wpedantic;-Werror>
    )

    include(GoogleTest)
    gtest_discover_tests(ocs_core_test)
endif()
