cmake_minimum_required(VERSION 3.5)
project(ycsb C CXX)

set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DHDRMEASUREMENT")
# set(CMAKE_CXX_STANDARD 17)
# set(CMAKE_CXX_STANDARD_REQUIRED ON)

## snappy
#find_path(SNAPPY_INCLUDE_PATH NAMES snappy.h)
#find_library(SNAPPY_LIB NAMES libsnappy.a)
#if ((NOT SNAPPY_INCLUDE_PATH) OR (NOT SNAPPY_LIB))
#    message(FATAL_ERROR "Fail to find snappy")
#endif()
#message( STATUS "snappy libraries : ${SNAPPY_LIB}" )
#include_directories(${SNAPPY_INCLUDE_PATH})
#
## leveldb
#find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
#find_library(LEVELDB_LIB NAMES libleveldb.a)
#if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
#    message(FATAL_ERROR "Fail to find leveldb")
#endif()
#message( STATUS "leveldb libraries : ${LEVELDB_LIB}" )
#include_directories(${LEVELDB_INCLUDE_PATH})

include_directories(${PROJECT_SOURCE_DIR}/)

add_executable(ycsb ${PROJECT_SOURCE_DIR}/leveldb/leveldb_db.cc
                    ${PROJECT_SOURCE_DIR}/azino/azino_db.cc
                    ${PROJECT_SOURCE_DIR}/core/acknowledged_counter_generator.cc
                    ${PROJECT_SOURCE_DIR}/core/basic_db.cc
                    ${PROJECT_SOURCE_DIR}/core/core_workload.cc
                    ${PROJECT_SOURCE_DIR}/core/db_factory.cc
                    ${PROJECT_SOURCE_DIR}/core/db.cc
                    ${PROJECT_SOURCE_DIR}/core/measurements.cc
                    ${PROJECT_SOURCE_DIR}/core/ycsbc.cc)

target_link_libraries(ycsb
#        pthread
#        ${LEVELDB_LIB}
#        ${SNAPPY_LIB})
        azino_sdk::lib
        azino::lib
        ${BRPC_LIB}
        ${COMMON_LIB})