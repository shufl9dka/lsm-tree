cmake_minimum_required(VERSION 3.0)
project(idx_lsm_tree)

file(GLOB_RECURSE sources   src/*.cpp src/*.h src/.hpp)

add_executable(lsm_bench ${sources})
target_compile_options(lsm_bench PUBLIC -std=c++1y -Wall -Wfloat-conversion)
target_include_directories(lsm_bench PUBLIC src)

install(TARGETS lsm_bench DESTINATION example_destination)

set(CPACK_PACKAGE_NAME "LSM Tree")
set(CPACK_PACKAGE_VERSION "1.0.0")

set(CPACK_MONOLITHIC_INSTALL 1)
include(CPack)
