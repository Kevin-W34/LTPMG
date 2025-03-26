# CMakeLists.txt for G4CU project

project(gpu)

# required cmake version
cmake_minimum_required(VERSION 2.9)


# packages
find_package(CUDA)

# include_directories (${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/include/)

# nvcc flags -g for debug
#set(CUDA_NVCC_FLAGS -O3;-G;-g)

set(CUDA_NVCC_FLAGS "-gencode arch=compute_86,code=sm_86 -O3 -lpthread")
#set(CUDA_NVCC_FLAGS "-gencode arch=compute_86,code=sm_86 -O3 -G -g -lpthread")
#set(CUDA_NVCC_FLAGS "-gencode arch=compute_75,code=sm_75 -O3 -G -g -lptuda-gdbhread")
#set(CUDA_NVCC_FLAGS -gencode arch=compute_52,code=sm_52;-G;-g)

file(GLOB_RECURSE CURRENT_HEADERS  include/*.h include/*.hpp include/*.cuh)
file(GLOB CURRENT_SOURCES  src/*.cpp src/*.cu)

source_group("Include" FILES ${CURRENT_HEADERS}) 
source_group("Source" FILES ${CURRENT_SOURCES}) 

cuda_add_library(gpu SHARED ${CURRENT_HEADERS} ${CURRENT_SOURCES})
# cuda_add_library(gpu STATIC ${CURRENT_HEADERS} ${CURRENT_SOURCES})

# target_link_libraries (gpu libnccl.so)