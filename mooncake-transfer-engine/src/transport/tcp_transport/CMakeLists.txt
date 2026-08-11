file(GLOB TCP_SOURCES "*.cpp")

if (MOONCAKE_USE_HIP_RUNTIME)
  hipify_files(TCP_SOURCES)
endif()

add_library(tcp_transport OBJECT ${TCP_SOURCES})
target_link_libraries(tcp_transport PRIVATE JsonCpp::JsonCpp yalantinglibs::yalantinglibs)
