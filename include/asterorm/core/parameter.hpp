#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "asterorm/core/error.hpp"

namespace asterorm {

enum class parameter_type : std::uint8_t {
    text,
    boolean,
};

struct query_parameter {
    std::optional<std::string> value;
    parameter_type type{parameter_type::text};
    std::optional<db_error> error;

    query_parameter(std::optional<std::string> encoded_value = std::nullopt,
                    parameter_type value_type = parameter_type::text)
        : value(std::move(encoded_value)), type(value_type) {}

    query_parameter(std::nullopt_t) : value(std::nullopt) {}
    query_parameter(const char* text_value) : value(std::string(text_value)) {}
    query_parameter(std::string text_value) : value(std::move(text_value)) {}

    [[nodiscard]] bool has_value() const {
        return value.has_value();
    }

    explicit operator bool() const {
        return has_value();
    }

    const std::string& operator*() const {
        return *value;
    }

    const std::string* operator->() const {
        return &*value;
    }

    friend bool operator==(const query_parameter& lhs, const query_parameter& rhs) {
        const bool equal_errors = (!lhs.error && !rhs.error) ||
                                  (lhs.error && rhs.error && lhs.error->kind == rhs.error->kind &&
                                   lhs.error->message == rhs.error->message);
        return lhs.value == rhs.value && lhs.type == rhs.type && equal_errors;
    }

    friend bool operator==(const query_parameter& parameter, const char* text_value) {
        return parameter.value && *parameter.value == text_value;
    }
};

using query_parameters = std::vector<query_parameter>;

} // namespace asterorm
