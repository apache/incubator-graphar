add_library(kuzu_graphar_function
        OBJECT
        graphar_scan.cpp
        graphar_bindfunc.cpp)

set(GRAPHAR_EXTENSION_OBJECT_FILES
        ${GRAPHAR_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_graphar_function>
        PARENT_SCOPE)
