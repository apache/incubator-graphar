add_library(kuzu_graphar_main
        OBJECT
        graphar_extension.cpp)

set(GRAPHAR_EXTENSION_OBJECT_FILES
        ${GRAPHAR_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_graphar_main>
        PARENT_SCOPE)
