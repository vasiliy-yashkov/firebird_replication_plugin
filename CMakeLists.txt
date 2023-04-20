cmake_minimum_required(VERSION 3.8)

project("replication_plugin" CXX)

if (NOT DEFINED CMAKE_BUILD_TYPE OR "${CMAKE_BUILD_TYPE}" STREQUAL "")
	set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build.")
endif()

if(NOT DEFINED ENV{FB_HOME})
    message(FATAL_ERROR "Variable FB_HOME must be set")
endif()

set (FB_HOME $ENV{FB_HOME})

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(${FB_HOME}/include)

file(GLOB SOURCES "src/*.*")

add_library(replication_plugin SHARED ${SOURCES})

if (MSVC)
	add_definitions(-D_CRT_SECURE_NO_WARNINGS)

	target_compile_options(replication_plugin PRIVATE
		"$<$<CONFIG:Release>:/MT>"
		"$<$<CONFIG:Debug>:/MTd>"
	)
else()
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

	target_compile_options(replication_plugin PRIVATE
		-fvisibility=hidden
	)

	if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.1)
		message(STATUS "CMAKE COMPILER VERSION ${CMAKE_CXX_COMPILER_VERSION}")
		target_link_libraries(replication_plugin
		-static-libgcc
		-lstdc++fs
		-Wl,--exclude-libs,ALL
	)
	else()
		message(STATUS "CMAKE COMPILER VERSION ${CMAKE_CXX_COMPILER_VERSION}")
		target_link_libraries(replication_plugin
		-static-libgcc
		-static-libstdc++
		-Wl,--exclude-libs,ALL
	)
	endif()
endif()

set_property(TARGET replication_plugin PROPERTY CXX_STANDARD 17)
