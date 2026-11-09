#include <asterorm/core/codecs.hpp>
#include <chrono>
#include <optional>
#include <string>
#include <vector>

namespace {
struct custom_value {
    int value{};
};
} // namespace

template <> struct asterorm::codec<custom_value> {
    static std::optional<std::string> encode(const custom_value& value) {
        return std::to_string(value.value);
    }
    static asterorm::result<void> decode(const std::optional<std::string>& text,
                                         custom_value& output) {
        return asterorm::decode(text, output.value);
    }
};

int main() {
    const asterorm::interval expected{.months = 14, .days = -3, .microseconds = -1};
    asterorm::interval span;
    if (!asterorm::decode(asterorm::encode(expected), span) || span != expected) {
        return 1;
    }
    asterorm::network host;
    if (!asterorm::decode(std::optional<std::string>{"192.0.2.9/24"}, host) ||
        !asterorm::encode_parameter(asterorm::cidr{.value = host}).error) {
        return 2;
    }
    asterorm::macaddr hardware;
    asterorm::date calendar_date;
    asterorm::numeric amount;
    std::chrono::system_clock::time_point timestamp;
    if (!asterorm::decode(std::optional<std::string>{"08:00:2b:01:02:03"}, hardware) ||
        !asterorm::decode(std::optional<std::string>{"2024-02-29"}, calendar_date) ||
        !asterorm::decode(std::optional<std::string>{"NaN"}, amount) ||
        !asterorm::decode(std::optional<std::string>{"1969-12-31 23:59:59.999999Z"}, timestamp) ||
        !asterorm::encode(timestamp)) {
        return 3;
    }
    const std::vector<std::optional<asterorm::interval>> values{expected, std::nullopt};
    std::vector<std::optional<asterorm::interval>> decoded;
    if (!asterorm::decode(asterorm::encode(values), decoded) || decoded != values) {
        return 4;
    }
    custom_value customized;
    if (!asterorm::decode(asterorm::encode(custom_value{.value = 42}), customized) ||
        customized.value != 42) {
        return 5;
    }
    return 0;
}
