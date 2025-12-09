#include "graphar/fwd.h"
#include "graphar/types.h"
#include "rust/cxx.h"

namespace graphar_rs {
rust::String to_type_name(const graphar::DataType &type);
}