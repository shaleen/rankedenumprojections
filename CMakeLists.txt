cmake_minimum_required(VERSION 3.10)
project(rankedlimit)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -O3 -Ofast -march=native -mfma -mavx -fomit-frame-pointer -ffp-contract=fast -flto -DARMA_NO_DEBUG")

add_executable(rankedlimit main.cpp utils/config.h algorithm/algorithm.h algorithm/algorithm.cpp)
