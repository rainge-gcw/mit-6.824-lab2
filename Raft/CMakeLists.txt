cmake_minimum_required(VERSION 3.24)
project(Raft)

set(CMAKE_CXX_STANDARD 17)

add_executable(Raft main.cpp Thread.h Worker.h httplib.h json.hpp)
