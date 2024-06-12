#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "graphar" for configuration "Release"
set_property(TARGET graphar APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(graphar PROPERTIES
  IMPORTED_LINK_DEPENDENT_LIBRARIES_RELEASE "Arrow::arrow_shared;Parquet::parquet_shared;ArrowDataset::arrow_dataset_shared;ArrowAcero::arrow_acero_shared"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libgraphar.so"
  IMPORTED_SONAME_RELEASE "libgraphar.so"
  )

list(APPEND _cmake_import_check_targets graphar )
list(APPEND _cmake_import_check_files_for_graphar "${_IMPORT_PREFIX}/lib/libgraphar.so" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
