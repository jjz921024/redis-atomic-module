cmake_minimum_required (VERSION 3.16)

project(atomic-module LANGUAGES C)

set(TARGET atomic-module)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -W -Wall -std=c99 -march=native -O3")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-strict-aliasing -Wno-typedef-redefinition -Wno-sign-compare -Wno-unused-parameter -Wno-unused-variable -Wno-stringop-truncation")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -ggdb") #-fstandalone-debug -gdwarf-4

if(SANITIZER_MODE MATCHES "address")
    set(CMAKE_BUILD_TYPE "DEBUG")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -fsanitize=address -fno-omit-frame-pointer")
elseif(SANITIZER_MODE MATCHES "undefined")
    set(CMAKE_BUILD_TYPE "DEBUG")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -fsanitize=undefined -fno-omit-frame-pointer")
elseif(SANITIZER_MODE MATCHES "thread")
    set(CMAKE_BUILD_TYPE "DEBUG")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -fsanitize=thread -fno-omit-frame-pointer")
endif(SANITIZER_MODE MATCHES "address")

set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

add_definitions(-DREDISMODULE_EXPERIMENTAL_API)

FILE(GLOB_RECURSE SOURCES 
    ${PROJECT_SOURCE_DIR}/src/*.c
    ${PROJECT_SOURCE_DIR}/src/*.h
)

include_directories(${PROJECT_SOURCE_DIR}/src)

add_library(${TARGET} SHARED ${SOURCES})
set_target_properties(${TARGET} PROPERTIES PREFIX "")
set_target_properties(${TARGET} PROPERTIES SUFFIX ".so")
