cmake_minimum_required(VERSION 2.8.4)

project(native)
# Default to release builds
set(CMAKE_BUILD_TYPE, Release)

#set(INCLUDE_DIRECTORIES
#
#    /opt/java/jdk1.7.0_45/include
#    /opt/java/jdk1.7.0_45/include/win32
#    ${GENERATED_JAVAH}
#)

include_directories(
    ${JNI_INCLUDE}
    ${JNI_INCLUDE}/${PF}
    ${GENERATED_JAVAH}
)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")



set(SOURCE_FILES
    src/io/test/NativeTest.c
    src/io/writables/nat/alloc.c
    src/io/writables/nat/item.c
    src/io/writables/nat/NativeCSCSliceHelper.c
    src/io/writables/nat/slice.c
    src/io/writables/nat/vector.c
    src/io/writables/nat/types.h
    src/io/writables/nat/stats.c
    src/io/writables/nat/heap.c
    src/io/writables/nat/blockiterator.c
    )

add_library(mclnative SHARED ${SOURCE_FILES})