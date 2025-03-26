project(ltpmg)

# required cmake version
cmake_minimum_required(VERSION 2.9)

SET(CMAKE_CXX_STANDARD 17)
SET(CMAKE_CXX_FLAGS "-O3 -lpthread " CACHE INTERNAL "compiler options" FORCE)
# -fsanitize=address

# find_package(Threads)

find_package(gflags REQUIRED)
# include_directories (${GFLAGS_INCLUDE_DIR})

find_package(glog REQUIRED)
# include_directories (${GLOG_INCLUDE_DIR})

add_subdirectory(cuda)
set (EXTRA_LIBS ${EXTRA_LIBS} gpu)

INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/include/)

file(GLOB_RECURSE SRC_FILES src/*.cpp)

ADD_EXECUTABLE(ltpmg ${SRC_FILES})

target_link_libraries (ltpmg ${EXTRA_LIBS} gflags glog::glog)
