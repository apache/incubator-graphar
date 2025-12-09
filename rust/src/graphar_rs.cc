#include "graphar_rs.h"

namespace graphar_rs {
rust::String to_type_name(const graphar::DataType &type) {
  return rust::String(type.ToTypeName());
}
}