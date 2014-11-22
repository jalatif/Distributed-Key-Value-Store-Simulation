cmake_minimum_required(VERSION 2.8.4)
project(DS_MP2)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    testcases/create.conf
    testcases/delete.conf
    testcases/read.conf
    testcases/update.conf
    Application.cpp#practice.cpp
    Application.h
    common.h
    EmulNet.cpp
    EmulNet.h
    Entry.cpp
    Entry.h
    HashTable.cpp
    HashTable.h
    KVStoreGrader.sh
    Log.cpp
    Log.h
    Makefile
    Member.cpp
    Member.h
    Message.cpp
    Message.h
    MP1Node.cpp
    MP1Node.h
    MP2Node.cpp
    MP2Node.h
    Node.cpp
    Node.h
    Params.cpp
    Params.h
    Queue.h
    Readme.txt
    stdincludes.h
    Trace.cpp
    Trace.h)

add_executable(Application ${SOURCE_FILES})