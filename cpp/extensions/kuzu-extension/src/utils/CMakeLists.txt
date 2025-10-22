add_library(kuzu_graphar_utils
        OBJECT
        graphar_utils.cpp)

set(GRAPHAR_EXTENSION_OBJECT_FILES
        ${GRAPHAR_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_graphar_utils>
        PARENT_SCOPE)
