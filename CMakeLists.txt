cmake_minimum_required (VERSION 3.16)

project(atomic-module LANGUAGES C)

option(ENABLE_ASAN "enable address sanitizer" OFF)
option(ENABLE_TSAN "enable thread sanitizer" OFF)

set(TARGET atomic-module)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -W -Wall -std=gnu99 -O2")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-strict-aliasing -Wno-typedef-redefinition -Wno-sign-compare -Wno-unused-parameter -Wno-unused-variable -Wno-stringop-truncation")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -ggdb") #-fstandalone-debug -gdwarf-4

if(ENABLE_ASAN AND ENABLE_TSAN)
    message(FATAL_ERROR "ASan and TSan cannot be used at the same time")
endif()

if(ENABLE_ASAN)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fsanitize=leak")
endif()
if(ENABLE_TSAN)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread")
endif()
# valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes

set(CMAKE_C_VISIBILITY_PRESET hidden) # -fvisibility=hidden
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
