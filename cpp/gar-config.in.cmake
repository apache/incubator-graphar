# - Config file for the gar package
#
# It defines the following variables
#
#  GAR_INCLUDE_DIR         - include directory for gar
#  GAR_INCLUDE_DIRS        - include directories for gar
#  GAR_LIBRARIES           - libraries to link against

set(GAR_HOME "${CMAKE_CURRENT_LIST_DIR}/../../..")
include("${CMAKE_CURRENT_LIST_DIR}/gar-targets.cmake")

set(GAR_LIBRARIES gar)
set(GAR_INCLUDE_DIR "${GAR_HOME}/include")
set(GAR_INCLUDE_DIRS "${GAR_INCLUDE_DIR}")
