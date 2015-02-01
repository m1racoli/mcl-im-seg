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
    src/io/test/NativeTest.c)

add_library(native SHARED ${SOURCE_FILES})