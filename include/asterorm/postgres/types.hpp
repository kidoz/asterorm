#pragma once
#include <string>

namespace asterorm::pg {

struct jsonb {
    std::string value;

    bool operator==(const jsonb& other) const {
        return value == other.value;
    }
};

} // namespace asterorm::pg
