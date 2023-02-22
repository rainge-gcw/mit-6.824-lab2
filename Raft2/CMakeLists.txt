cmake_minimum_required(VERSION 3.24)
project(Raft2)

set(CMAKE_CXX_STANDARD 17)

add_executable(Raft2 main.cpp Raft.h log.h Thread.h json.hpp massage.h httplib.h)
