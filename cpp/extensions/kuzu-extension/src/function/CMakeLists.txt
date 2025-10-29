add_library(kuzu_graphar_function
        OBJECT
        graphar_scan.cpp
        graphar_scan_bindfunc.cpp
        graphar_export.cpp
        graphar_export_bindfunc.cpp
        graphar_metadata.cpp)

set(GRAPHAR_EXTENSION_OBJECT_FILES
        ${GRAPHAR_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_graphar_function>
        PARENT_SCOPE)
