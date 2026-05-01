#pragma once
#include <string>

namespace asterorm {

struct jsonb {
    std::string value;

    bool operator==(const jsonb& other) const {
        return value == other.value;
    }
};

} // namespace asterorm
