#pragma once

#include "extension/extension.h"

namespace kuzu {
namespace graphar_extension {

class GrapharExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "GRAPHAR";

public:
    static void load(main::ClientContext* context);
};

} // namespace graphar_extension
} // namespace kuzu
