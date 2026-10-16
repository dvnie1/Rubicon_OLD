cmake_minimum_required(VERSION 3.20)
project(DisaggSIMDProcessor LANGUAGES CXX)

include(FetchContent)

FetchContent_Declare(
        httplib
        GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
        GIT_TAG        v0.27.0
)
FetchContent_MakeAvailable(httplib)

FetchContent_Declare(
        nlohmann_json
        GIT_REPOSITORY https://github.com/nlohmann/json.git
        GIT_TAG        v3.12.0
)
FetchContent_MakeAvailable(nlohmann_json)

FetchContent_Declare(
        snappy
        GIT_REPOSITORY https://github.com/google/snappy.git
        GIT_TAG 1.2.2
)
FetchContent_MakeAvailable(snappy)

FetchContent_Declare(
        xgboost
        GIT_REPOSITORY https://github.com/dmlc/xgboost.git
        GIT_TAG        v3.4.1
        GIT_SUBMODULES_RECURSE TRUE
)
FetchContent_MakeAvailable(xgboost)

FetchContent_Declare(
        eigen
        GIT_REPOSITORY https://gitlab.com/libeigen/eigen.git
        GIT_TAG        3.4.0
)
FetchContent_MakeAvailable(eigen)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED On)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
#set(warnings "-Wall -Wextra -pedantic -Wcast-align -Wcast-qual -Wctor-dtor-privacy -Wdisabled-optimization -Wformat=2 -Winit-self -Wlogical-op -Wmissing-include-dirs -Wnoexcept -Wold-style-cast -Woverloaded-virtual -Wredundant-decls -Wshadow -Wsign-promo -Wstrict-null-sentinel -Wstrict-overflow=5 -Wundef -Wno-unused -Wno-variadic-macros -Wno-parentheses -fdiagnostics-show-option")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${warnings}")
set(OPTIMIZE_OPTIONS -O3 -g)
set(CMAKE_VERBOSE_MAKEFILE On)

add_compile_options("${OPTIMIZE_OPTIONS}")

find_library(PTHREAD_LIB pthread REQUIRED)
find_library(NUMA_LIB numa REQUIRED)

# ── Protobuf ────────────────────────────────────────────────
find_package(Protobuf REQUIRED)

# Where your .proto files live
set(PROTO_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
# Where generated .pb.h / .pb.cc will be placed
set(PROTO_GEN_DIR ${CMAKE_BINARY_DIR}/proto)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

# Collect all .proto files
file(GLOB PROTO_FILES "${PROTO_SRC_DIR}/*.proto")

# Generate C++ sources from each .proto
set(PROTO_GENERATED_SRCS "")
set(PROTO_GENERATED_HDRS "")
foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    list(APPEND PROTO_GENERATED_SRCS "${PROTO_GEN_DIR}/${PROTO_NAME}.pb.cc")
    list(APPEND PROTO_GENERATED_HDRS "${PROTO_GEN_DIR}/${PROTO_NAME}.pb.h")
endforeach()

add_custom_command(
    OUTPUT ${PROTO_GENERATED_SRCS} ${PROTO_GENERATED_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        --cpp_out=${PROTO_GEN_DIR}
        -I${PROTO_SRC_DIR}
        ${PROTO_FILES}
    DEPENDS ${PROTO_FILES}
    COMMENT "Generating protobuf C++ sources in ${PROTO_GEN_DIR}"
)

# Library target so the generated code is compiled once and linkable
add_library(proto_lib STATIC ${PROTO_GENERATED_SRCS} ${PROTO_GENERATED_HDRS})
target_include_directories(proto_lib PUBLIC ${PROTO_GEN_DIR})
target_link_libraries(proto_lib PUBLIC ${Protobuf_LIBRARIES})
target_include_directories(proto_lib PUBLIC ${Protobuf_INCLUDE_DIRS})

set(TCP_INC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tcp_stuff/include)
file(GLOB_RECURSE TCP_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/tcp_stuff/src/*.cpp)

add_library(tcp_lib STATIC ${TCP_SOURCES})
target_include_directories(tcp_lib PUBLIC ${TCP_INC_DIR})
target_link_libraries(tcp_lib PUBLIC proto_lib httplib nlohmann_json snappy ${PTHREAD_LIB})
# ────────────────────────────────────────────────────────────

set(EXT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/ext)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_subdirectory("ext/SIMDOperators")

# Default log level, overridable via -DLOG_LEVEL=LOG_DEBUG1
set(LOG_LEVEL "LOG_INFO" CACHE STRING "Default log level")

# ── computeUnit ─────────────────────────────────────────────
set(CU_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/computeUnit/src)
set(CU_INC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/computeUnit/include)

file(GLOB_RECURSE CU_SOURCES ${CU_SRC_DIR}/*.cpp)
file(GLOB_RECURSE CU_HEADERS ${CU_INC_DIR}/*.hpp)

add_executable(computeUnit computeUnit/ComputeUnit.cpp ${CU_SOURCES} ${CU_HEADERS})
target_include_directories(computeUnit PRIVATE ${CU_INC_DIR} ${EXT_DIR} ${SIMDOPS_TSL_ROOT} ${SIMDOPS_INCLUDE})
target_link_libraries(computeUnit ${PTHREAD_LIB} ${NUMA_LIB} "tsl" proto_lib tcp_lib)
target_compile_definitions(computeUnit PRIVATE DEFAULT_LOG_LEVEL="${LOG_LEVEL}")

# ── grouper ─────────────────────────────────────────────────
set(GR_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/grouper/src)
set(GR_INC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/grouper/include)

file(GLOB_RECURSE GR_SOURCES ${GR_SRC_DIR}/*.cpp)
file(GLOB_RECURSE GR_HEADERS ${GR_INC_DIR}/*.hpp)

add_executable(grouper grouper/Grouper.cpp ${GR_SOURCES} ${GR_HEADERS}
        grouper/include/MetricsClient.hpp
        grouper/include/QueryVectorizer.hpp)
target_include_directories(grouper PRIVATE ${GR_INC_DIR} ${EXT_DIR})
target_link_libraries(grouper ${PTHREAD_LIB} ${NUMA_LIB} proto_lib httplib nlohmann_json snappy xgboost eigen tcp_lib)
option(GROUPER_VERBOSE "Enable verbose output for grouper" OFF)
if(GROUPER_VERBOSE)
    target_compile_definitions(grouper PRIVATE VERBOSE_OUTPUT)
endif()
option(GROUPER_PROFILING "Enable CSV timing for DagCollection::analyze" OFF)
if(GROUPER_PROFILING)
    target_compile_definitions(grouper PRIVATE PROFILING_ENABLED)
endif()

# ── monitorServer ─────────────────────────────────────────────
add_executable(monitorServer computeUnit/MonitorServerUnit.cpp ${CU_SOURCES} ${CU_HEADERS})
target_include_directories(monitorServer PRIVATE ${CU_INC_DIR} ${GR_INC_DIR} ${EXT_DIR} ${SIMDOPS_TSL_ROOT} ${SIMDOPS_INCLUDE})
target_link_libraries(monitorServer ${PTHREAD_LIB} ${NUMA_LIB} "tsl" proto_lib httplib nlohmann_json snappy tcp_lib)


# ── pg potential bench ────────────────────────────────────────
add_executable(pg_potential_bench "pg_potential_experiment/pg_potential_bench.cpp")

target_compile_options(pg_potential_bench PRIVATE -O3 -march=native -mavx512f)
target_link_libraries(pg_potential_bench ${PTHREAD_LIB} ${NUMA_LIB})
