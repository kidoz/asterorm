#include "asterorm/core/detail/text_codecs.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <string_view>

namespace asterorm::detail {
namespace {
db_error malformed(std::string_view domain) {
    return db_error{
        .kind = db_error_kind::parse_failed,
        .message = "Malformed or out-of-range " + std::string{domain},
    };
}

bool digit(char character) {
    return character >= '0' && character <= '9';
}

bool space(char character) {
    return character == ' ' || character == '\t' || character == '\n' || character == '\r' ||
           character == '\f' || character == '\v';
}

bool unsigned_number(std::string_view text, std::uint64_t& output, int base = 10) {
    if (text.empty()) {
        return false;
    }
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), output, base);
    return error == std::errc{} && end == text.data() + text.size();
}

// Small checked scanner shared by date and timestamp grammar; never narrows fields.
struct scanner {
    std::string_view text;
    std::size_t position{};

    bool take(char expected) {
        if (position == text.size() || text[position] != expected) {
            return false;
        }
        ++position;
        return true;
    }

    bool number(unsigned minimum_digits, unsigned maximum_digits, unsigned& output) {
        const auto start = position;
        output = 0;
        while (position < text.size() && digit(text[position]) &&
               position - start < maximum_digits) {
            output = (output * 10) + static_cast<unsigned>(text[position] - '0');
            ++position;
        }
        return position - start >= minimum_digits;
    }

    [[nodiscard]] bool done() const {
        return position == text.size();
    }
};

bool calendar(scanner& input, date& output) {
    unsigned year_value{};
    if (!input.number(4, 5, year_value) || year_value == 0 || year_value > 32767 ||
        !input.take('-') || !input.number(2, 2, output.month) || !input.take('-') ||
        !input.number(2, 2, output.day) || output.month < 1 || output.month > 12 ||
        output.day < 1 || output.day > 31) {
        return false;
    }
    output.year = static_cast<int>(year_value);
    return std::chrono::year_month_day{std::chrono::year{output.year},
                                       std::chrono::month{output.month},
                                       std::chrono::day{output.day}}
        .ok();
}

bool add_checked(std::int64_t& accumulator, std::int64_t component) {
    if ((component > 0 && accumulator > std::numeric_limits<std::int64_t>::max() - component) ||
        (component < 0 && accumulator < std::numeric_limits<std::int64_t>::min() - component)) {
        return false;
    }
    accumulator += component;
    return true;
}

// Parse exact signed decimals with a fixed unit multiplier, including INT64_MIN.
bool scaled_number(std::string_view text, std::uint64_t multiplier, bool fractional, bool invert,
                   std::int64_t& output) {
    bool negative = invert;
    if (!text.empty() && (text.front() == '-' || text.front() == '+')) {
        negative = negative != (text.front() == '-');
        text.remove_prefix(1);
    }
    const auto point = text.find('.');
    std::uint64_t whole{};
    if (!unsigned_number(text.substr(0, point), whole)) {
        return false;
    }
    const std::uint64_t limit =
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) +
        static_cast<unsigned>(negative);
    if (whole > limit / multiplier) {
        return false;
    }
    std::uint64_t magnitude = whole * multiplier;
    if (point != std::string_view::npos) {
        auto fraction = text.substr(point + 1);
        if (!fractional || fraction.empty() || fraction.size() > 6) {
            return false;
        }
        std::uint64_t remainder{};
        if (!unsigned_number(fraction, remainder)) {
            return false;
        }
        for (auto digits = fraction.size(); digits < 6; ++digits) {
            remainder *= 10;
        }
        if (magnitude > limit - remainder) {
            return false;
        }
        magnitude += remainder;
    }
    if (negative && magnitude == limit) {
        output = std::numeric_limits<std::int64_t>::min();
    } else {
        output =
            negative ? -static_cast<std::int64_t>(magnitude) : static_cast<std::int64_t>(magnitude);
    }
    return true;
}

bool clock_time(std::string_view text, bool invert, std::int64_t& output) {
    bool negative = invert;
    if (!text.empty() && (text.front() == '-' || text.front() == '+')) {
        negative = negative != (text.front() == '-');
        text.remove_prefix(1);
    }
    const auto first_colon = text.find(':');
    const auto second_colon =
        text.find(':', first_colon == std::string_view::npos ? 0 : first_colon + 1);
    if (first_colon == std::string_view::npos || second_colon == std::string_view::npos) {
        return false;
    }
    std::uint64_t hours{};
    std::uint64_t minutes{};
    if (!unsigned_number(text.substr(0, first_colon), hours) ||
        !unsigned_number(text.substr(first_colon + 1, second_colon - first_colon - 1), minutes) ||
        second_colon - first_colon != 3 || minutes > 59) {
        return false;
    }
    auto seconds = text.substr(second_colon + 1);
    std::int64_t second_microseconds{};
    if (seconds.size() < 2 || !digit(seconds[0]) || !digit(seconds[1]) ||
        (seconds.size() > 2 && seconds[2] != '.') ||
        !scaled_number(seconds, 1000000, true, false, second_microseconds) ||
        second_microseconds >= 60000000) {
        return false;
    }
    const std::uint64_t limit =
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) +
        static_cast<unsigned>(negative);
    if (hours > limit / 3600000000ULL) {
        return false;
    }
    auto magnitude = hours * 3600000000ULL;
    const auto remainder =
        (minutes * 60000000ULL) + static_cast<std::uint64_t>(second_microseconds);
    if (magnitude > limit - remainder) {
        return false;
    }
    magnitude += remainder;
    if (negative && magnitude == limit) {
        output = std::numeric_limits<std::int64_t>::min();
    } else {
        output =
            negative ? -static_cast<std::int64_t>(magnitude) : static_cast<std::int64_t>(magnitude);
    }
    return true;
}

bool ipv4(std::string_view text, std::array<std::uint8_t, 4>& bytes) {
    for (std::size_t index = 0; index < bytes.size(); ++index) {
        const auto separator = text.find('.');
        auto part = text.substr(0, separator);
        std::uint64_t octet{};
        if (part.size() > 3 || (part.size() > 1 && part.front() == '0') ||
            !unsigned_number(part, octet) || octet > 255) {
            return false;
        }
        bytes[index] = static_cast<std::uint8_t>(octet);
        if (index == 3) {
            return separator == std::string_view::npos;
        }
        if (separator == std::string_view::npos) {
            return false;
        }
        text.remove_prefix(separator + 1);
    }
    return false;
}

bool ipv6_part(std::string_view text, bool allow_ipv4, std::vector<std::uint16_t>& words) {
    if (text.empty()) {
        return true;
    }
    while (!text.empty()) {
        const auto separator = text.find(':');
        auto part = text.substr(0, separator);
        if (part.contains('.')) {
            std::array<std::uint8_t, 4> bytes{};
            if (!allow_ipv4 || separator != std::string_view::npos || !ipv4(part, bytes)) {
                return false;
            }
            words.push_back(static_cast<std::uint16_t>((bytes[0] * 256U) + bytes[1]));
            words.push_back(static_cast<std::uint16_t>((bytes[2] * 256U) + bytes[3]));
        } else {
            std::uint64_t word{};
            if (part.size() > 4 || !unsigned_number(part, word, 16) || word > 65535) {
                return false;
            }
            words.push_back(static_cast<std::uint16_t>(word));
        }
        if (words.size() > 8) {
            return false;
        }
        if (separator == std::string_view::npos) {
            return true;
        }
        text.remove_prefix(separator + 1);
        if (text.empty()) {
            return false;
        }
    }
    return false;
}

std::string ipv4_text(const std::uint8_t* bytes) {
    return std::to_string(bytes[0]) + "." + std::to_string(bytes[1]) + "." +
           std::to_string(bytes[2]) + "." + std::to_string(bytes[3]);
}
} // namespace

result<void> decode_date(std::string_view text, date& output) {
    scanner input{.text = text};
    date decoded;
    if (!calendar(input, decoded) || !input.done()) {
        return std::unexpected(malformed("ISO AD date"));
    }
    output = decoded;
    return {};
}

std::optional<std::string> encode_timestamp(std::chrono::system_clock::time_point timestamp) {
    using namespace std::chrono;
    // floor the whole seconds so pre-epoch fractions never print a negative field.
    const auto elapsed = duration_cast<microseconds>(timestamp.time_since_epoch());
    const auto whole_seconds = floor<seconds>(time_point<system_clock, microseconds>{elapsed});
    auto fraction = elapsed.count() % 1000000;
    if (fraction < 0) {
        fraction += 1000000;
    }
    const auto day_point = floor<days>(whole_seconds);
    if (day_point < sys_days{year{1} / January / 1} ||
        day_point > sys_days{year{32767} / December / 31}) {
        return std::nullopt;
    }
    const year_month_day calendar_date{day_point};
    const int year_value = static_cast<int>(calendar_date.year());
    if (year_value < 1 || year_value > 32767) {
        return std::nullopt;
    }
    const hh_mm_ss clock{whole_seconds - day_point};
    std::array<char, 64> buffer{};
    const int length = std::snprintf(
        buffer.data(), buffer.size(), "%04d-%02u-%02u %02lld:%02lld:%02lld.%06lld+00:00",
        year_value, static_cast<unsigned>(calendar_date.month()),
        static_cast<unsigned>(calendar_date.day()), static_cast<long long>(clock.hours().count()),
        static_cast<long long>(clock.minutes().count()),
        static_cast<long long>(clock.seconds().count()), static_cast<long long>(fraction));
    if (length < 0 || static_cast<std::size_t>(length) >= buffer.size()) {
        return std::nullopt;
    }
    return std::string(buffer.data(), static_cast<std::size_t>(length));
}

result<void> decode_timestamp(std::string_view text,
                              std::chrono::system_clock::time_point& output) {
    using namespace std::chrono;
    scanner input{.text = text};
    date calendar_date;
    unsigned hour{};
    unsigned minute{};
    unsigned second{};
    unsigned fraction{};
    if (!calendar(input, calendar_date) || !(input.take(' ') || input.take('T')) ||
        !input.number(2, 2, hour) || !input.take(':') || !input.number(2, 2, minute) ||
        !input.take(':') || !input.number(2, 2, second) || hour > 23 || minute > 59 ||
        second > 59) {
        return std::unexpected(malformed("timestamp"));
    }
    if (input.take('.')) {
        const auto start = input.position;
        if (!input.number(1, 6, fraction)) {
            return std::unexpected(malformed("timestamp fraction"));
        }
        for (auto digits = input.position - start; digits < 6; ++digits) {
            fraction *= 10;
        }
    }
    int offset{};
    if (!input.done() && (text[input.position] == '+' || text[input.position] == '-')) {
        const bool negative = input.take('-');
        if (!negative) {
            input.take('+');
        }
        unsigned offset_hour{};
        unsigned offset_minute{};
        unsigned offset_second{};
        if (!input.number(2, 2, offset_hour) || offset_hour > 15) {
            return std::unexpected(malformed("timestamp offset"));
        }
        if (input.take(':')) {
            if (!input.number(2, 2, offset_minute) || offset_minute > 59) {
                return std::unexpected(malformed("timestamp offset"));
            }
            if (input.take(':') && (!input.number(2, 2, offset_second) || offset_second > 59)) {
                return std::unexpected(malformed("timestamp offset"));
            }
        }
        offset = static_cast<int>((offset_hour * 3600) + (offset_minute * 60) + offset_second);
        if (negative) {
            offset = -offset;
        }
    } else {
        input.take('Z'); // Offset-less MySQL text is UTC, as before.
    }
    if (!input.done()) {
        return std::unexpected(malformed("timestamp tail"));
    }
    const sys_days day_point{
        year{calendar_date.year} / month{calendar_date.month} / day{calendar_date.day},
    };
    const auto total_seconds = duration_cast<seconds>(day_point.time_since_epoch()).count() +
                               (static_cast<std::int64_t>(hour) * 3600) +
                               (static_cast<std::int64_t>(minute) * 60) + second - offset;
    // AD years up to 32767 fit int64 microseconds. Check before clock conversion.
    const microseconds elapsed{(total_seconds * 1000000) + fraction};
    using clock_duration = system_clock::duration;
    static_assert(clock_duration::period::num == 1 && clock_duration::period::den >= 1000000,
                  "timestamp codec requires a microsecond-or-finer system clock");
    if (elapsed < duration_cast<microseconds>(clock_duration::min()) ||
        elapsed > duration_cast<microseconds>(clock_duration::max())) {
        return std::unexpected(malformed("timestamp representability"));
    }
    output = system_clock::time_point{duration_cast<clock_duration>(elapsed)};
    return {};
}

bool is_valid_numeric(std::string_view text) {
    if (text == "NaN" || text == "Infinity" || text == "-Infinity") {
        return true;
    }
    std::size_t position{};
    if (!text.empty() && (text.front() == '+' || text.front() == '-')) {
        ++position;
    }
    bool saw_digit = false;
    while (position < text.size() && digit(text[position])) {
        saw_digit = true;
        ++position;
    }
    if (position < text.size() && text[position] == '.') {
        ++position;
        while (position < text.size() && digit(text[position])) {
            saw_digit = true;
            ++position;
        }
    }
    if (!saw_digit) {
        return false;
    }
    if (position < text.size() && (text[position] == 'e' || text[position] == 'E')) {
        ++position;
        if (position < text.size() && (text[position] == '+' || text[position] == '-')) {
            ++position;
        }
        const auto start = position;
        while (position < text.size() && digit(text[position])) {
            ++position;
        }
        if (position == start) {
            return false;
        }
    }
    return position == text.size();
}

std::optional<std::string> encode_interval(const interval& value) {
    // Explicit signs on every independent field avoid sql_standard sign inheritance.
    const auto seconds = value.microseconds / 1000000;
    const auto remainder = value.microseconds % 1000000;
    const auto fraction = remainder < 0 ? -remainder : remainder;
    std::array<char, 64> buffer{};
    const int length =
        std::snprintf(buffer.data(), buffer.size(), "%s%lld.%06lld secs",
                      value.microseconds < 0 && seconds == 0 ? "-" : "",
                      static_cast<long long>(seconds), static_cast<long long>(fraction));
    if (length < 0 || static_cast<std::size_t>(length) >= buffer.size()) {
        return std::nullopt;
    }
    std::string text = value.months >= 0 ? "+" : "";
    text += std::to_string(value.months) + " mons ";
    text += value.days >= 0 ? "+" : "";
    text += std::to_string(value.days) + " days ";
    text += value.microseconds >= 0 ? "+" : "";
    text.append(buffer.data(), static_cast<std::size_t>(length));
    return text;
}

result<void> decode_interval(std::string_view text, interval& output) {
    if (text.empty() || text.contains('\0')) {
        return std::unexpected(malformed("interval"));
    }
    std::int64_t months{};
    std::int64_t days{};
    std::int64_t microseconds{};
    unsigned seen{};
    bool invert = false;
    auto component = [&](std::string_view number, unsigned kind) {
        // Order Y M D H M S is fixed for native ISO/verbose output.
        if ((seen & (1U << kind)) != 0 || (seen >> kind) != 0) {
            return false;
        }
        seen |= 1U << kind;
        constexpr std::array<std::uint64_t, 6> multipliers{
            12, 1, 1, 3600000000ULL, 60000000, 1000000,
        };
        std::int64_t parsed{};
        if (!scaled_number(number, multipliers[kind], kind == 5, invert, parsed)) {
            return false;
        }
        if (kind < 2) {
            return add_checked(months, parsed);
        }
        if (kind == 2) {
            return add_checked(days, parsed);
        }
        return add_checked(microseconds, parsed);
    };
    if (text.front() == 'P') {
        text.remove_prefix(1);
        bool time_part = false;
        bool time_value = false;
        while (!text.empty()) {
            if (text.front() == 'T') {
                if (time_part) {
                    return std::unexpected(malformed("ISO interval"));
                }
                time_part = true;
                text.remove_prefix(1);
                if (text.empty()) {
                    return std::unexpected(malformed("ISO interval"));
                }
            }
            std::size_t length{};
            if (!text.empty() && (text.front() == '+' || text.front() == '-')) {
                ++length;
            }
            while (length < text.size() && (digit(text[length]) || text[length] == '.')) {
                ++length;
            }
            if (length == text.size()) {
                return std::unexpected(malformed("ISO interval"));
            }
            unsigned kind = 6;
            switch (text[length]) {
            case 'Y':
                if (!time_part) {
                    kind = 0;
                }
                break;
            case 'M':
                kind = time_part ? 4 : 1;
                break;
            case 'D':
                if (!time_part) {
                    kind = 2;
                }
                break;
            case 'H':
                if (time_part) {
                    kind = 3;
                }
                break;
            case 'S':
                if (time_part) {
                    kind = 5;
                }
                break;
            default:
                break;
            }
            if (kind == 6 || !component(text.substr(0, length), kind)) {
                return std::unexpected(malformed("ISO interval"));
            }
            time_value = time_value || time_part;
            text.remove_prefix(length + 1);
        }
        if (seen == 0 || (time_part && !time_value)) {
            return std::unexpected(malformed("ISO interval"));
        }
    } else {
        const bool verbose = text.starts_with("@ ");
        if (verbose) {
            text.remove_prefix(2);
        }
        if (verbose && text.ends_with(" ago")) {
            invert = true;
            text.remove_suffix(4);
        }
        std::vector<std::string_view> tokens;
        while (!text.empty()) {
            if (space(text.front())) {
                text.remove_prefix(1);
                continue;
            }
            const auto end = text.find_first_of(" \t\r\n");
            tokens.push_back(text.substr(0, end));
            if (tokens.size() > 12) {
                return std::unexpected(malformed("interval arity"));
            }
            if (end == std::string_view::npos) {
                break;
            }
            text.remove_prefix(end);
        }
        if (tokens.empty()) {
            return std::unexpected(malformed("interval"));
        }
        if (verbose && tokens.size() == 1 && tokens.front() == "0") {
            output = interval{};
            return {};
        }
        bool has_units = false;
        // Native SQL output uses bare day and year-month fields, never unit words.
        for (const auto token : tokens) {
            if (token.find_first_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") ==
                std::string_view::npos) {
                continue;
            }
            has_units = true;
        }
        const bool sql_style = !has_units;
        bool inherited_negative = false;
        if (sql_style && tokens.front().starts_with('-')) {
            inherited_negative = true;
            for (std::size_t index = 1; index < tokens.size(); ++index) {
                if (tokens[index].starts_with('+') || tokens[index].starts_with('-')) {
                    inherited_negative = false;
                }
            }
        }
        for (std::size_t index = 0; index < tokens.size(); ++index) {
            auto token = tokens[index];
            if (token.contains(':')) {
                if (verbose || (seen >> 3U) != 0 || index + 1 != tokens.size()) {
                    return std::unexpected(malformed("interval clock"));
                }
                const bool inherit = inherited_negative && index > 0;
                if (!clock_time(token, invert != inherit, microseconds)) {
                    return std::unexpected(malformed("interval clock"));
                }
                seen |= 1U << 5U;
            } else if (sql_style) {
                const auto dash =
                    token.find('-', token.starts_with('-') || token.starts_with('+') ? 1 : 0);
                if (dash != std::string_view::npos) {
                    if (index != 0) {
                        return std::unexpected(malformed("interval year-month"));
                    }
                    const auto month_text = token.substr(dash + 1);
                    std::uint64_t month_value{};
                    if (!unsigned_number(month_text, month_value) || month_value > 11 ||
                        !component(token.substr(0, dash), 0)) {
                        return std::unexpected(malformed("interval year-month"));
                    }
                    std::string signed_month = token.starts_with('-') ? "-" : "+";
                    signed_month += month_text;
                    if (!component(signed_month, 1)) {
                        return std::unexpected(malformed("interval year-month"));
                    }
                } else {
                    const bool saved_invert = invert;
                    invert = invert != (inherited_negative && index > 0);
                    const bool success = component(token, 2);
                    invert = saved_invert;
                    if (!success) {
                        return std::unexpected(malformed("interval days"));
                    }
                }
            } else {
                if (++index == tokens.size()) {
                    return std::unexpected(malformed("interval unit"));
                }
                const auto unit = tokens[index];
                unsigned kind = 6;
                if (unit == "year" || unit == "years") {
                    kind = 0;
                } else if (unit == "mon" || unit == "mons") {
                    kind = 1;
                } else if (unit == "day" || unit == "days") {
                    kind = 2;
                } else if (unit == "hour" || unit == "hours") {
                    kind = 3;
                } else if (unit == "min" || unit == "mins") {
                    kind = 4;
                } else if (unit == "sec" || unit == "secs") {
                    kind = 5;
                }
                if (kind == 6 || !component(token, kind)) {
                    return std::unexpected(malformed("interval component"));
                }
            }
        }
    }
    if (months < std::numeric_limits<std::int32_t>::min() ||
        months > std::numeric_limits<std::int32_t>::max() ||
        days < std::numeric_limits<std::int32_t>::min() ||
        days > std::numeric_limits<std::int32_t>::max()) {
        return std::unexpected(malformed("interval components"));
    }
    output = interval{
        .months = static_cast<std::int32_t>(months),
        .days = static_cast<std::int32_t>(days),
        .microseconds = microseconds,
    };
    return {};
}

result<void> decode_network(std::string_view text, network& output) {
    network decoded;
    const auto slash = text.find('/');
    auto address_text = text.substr(0, slash);
    const bool is_ipv6 = address_text.contains(':');
    decoded.family = is_ipv6 ? address_family::ipv6 : address_family::ipv4;
    const unsigned width = is_ipv6 ? 128 : 32;
    decoded.prefix_length = width;
    if (slash != std::string_view::npos) {
        std::uint64_t prefix{};
        if (!unsigned_number(text.substr(slash + 1), prefix) || prefix > width) {
            return std::unexpected(malformed("network prefix"));
        }
        decoded.prefix_length = static_cast<unsigned>(prefix);
    }
    if (!is_ipv6) {
        std::array<std::uint8_t, 4> bytes{};
        if (!ipv4(address_text, bytes)) {
            return std::unexpected(malformed("IPv4 address"));
        }
        std::ranges::copy(bytes, decoded.address.begin());
    } else {
        const auto compression = address_text.find("::");
        std::vector<std::uint16_t> leading;
        std::vector<std::uint16_t> trailing;
        if (compression == std::string_view::npos) {
            if (!ipv6_part(address_text, true, leading) || leading.size() != 8) {
                return std::unexpected(malformed("IPv6 address"));
            }
        } else {
            if (address_text.find("::", compression + 2) != std::string_view::npos ||
                !ipv6_part(address_text.substr(0, compression), false, leading) ||
                !ipv6_part(address_text.substr(compression + 2), true, trailing) ||
                leading.size() + trailing.size() >= 8) {
                return std::unexpected(malformed("IPv6 compression"));
            }
            leading.resize(8 - trailing.size(), 0);
            leading.insert(leading.end(), trailing.begin(), trailing.end());
        }
        for (std::size_t index = 0; index < 8; ++index) {
            decoded.address[index * 2] = static_cast<std::uint8_t>(leading[index] / 256U);
            decoded.address[(index * 2) + 1] = static_cast<std::uint8_t>(leading[index] % 256U);
        }
    }
    output = decoded;
    return {};
}

bool has_zero_host_bits(const network& value) {
    const unsigned width = value.family == address_family::ipv4 ? 32 : 128;
    if ((value.family != address_family::ipv4 && value.family != address_family::ipv6) ||
        value.prefix_length > width) {
        return false;
    }
    for (unsigned bit = value.prefix_length; bit < width; ++bit) {
        if ((value.address[bit / 8] & (1U << (7 - (bit % 8)))) != 0) {
            return false;
        }
    }
    return true;
}

std::optional<std::string> encode_network(const network& value) {
    const bool is_ipv4 = value.family == address_family::ipv4;
    if ((value.family != address_family::ipv4 && value.family != address_family::ipv6) ||
        value.prefix_length > (is_ipv4 ? 32U : 128U)) {
        return std::nullopt;
    }
    std::string text;
    if (is_ipv4) {
        if (std::any_of(value.address.begin() + 4, value.address.end(),
                        [](auto byte) { return byte != 0; })) {
            return std::nullopt;
        }
        text = ipv4_text(value.address.data());
    } else {
        std::array<unsigned, 8> words{};
        for (std::size_t index = 0; index < words.size(); ++index) {
            words[index] = (value.address[index * 2] * 256U) + value.address[(index * 2) + 1];
        }
        std::size_t best_start = 8;
        std::size_t best_length{};
        for (std::size_t start = 0; start < 8;) {
            if (words[start] != 0) {
                ++start;
                continue;
            }
            auto end = start;
            while (end < 8 && words[end] == 0) {
                ++end;
            }
            if (end - start > best_length && end - start >= 2) {
                best_start = start;
                best_length = end - start;
            }
            start = end;
        }
        for (std::size_t index = 0; index < 8;) {
            if (index == best_start) {
                text += "::";
                index += best_length;
                continue;
            }
            if (!text.empty() && text.back() != ':') {
                text += ':';
            }
            std::array<char, 5> buffer{};
            const auto [end, error] =
                std::to_chars(buffer.data(), buffer.data() + buffer.size(), words[index], 16);
            if (error != std::errc{}) {
                return std::nullopt;
            }
            text.append(buffer.data(), end);
            ++index;
        }
    }
    // Always emit a prefix so CIDR input never depends on classful defaults.
    text += '/' + std::to_string(value.prefix_length);
    return text;
}

std::string encode_macaddr(const macaddr& value) {
    constexpr std::string_view hexadecimal = "0123456789abcdef";
    std::string text;
    for (auto byte : value.bytes) {
        if (!text.empty()) {
            text += ':';
        }
        text += hexadecimal[byte / 16U];
        text += hexadecimal[byte % 16U];
    }
    return text;
}

result<void> decode_macaddr(std::string_view text, macaddr& output) {
    if (text.size() != 17) {
        return std::unexpected(malformed("canonical MAC address"));
    }
    macaddr decoded;
    for (std::size_t index = 0; index < 6; ++index) {
        const auto pair = text.substr(index * 3, 2);
        std::uint64_t byte{};
        if (!unsigned_number(pair, byte, 16) || byte > 255 ||
            (index < 5 && text[(index * 3) + 2] != ':')) {
            return std::unexpected(malformed("canonical MAC address"));
        }
        decoded.bytes[index] = static_cast<std::uint8_t>(byte);
    }
    output = decoded;
    return {};
}

result<std::vector<std::optional<std::string>>> parse_flat_array(std::string_view text) {
    if (text.size() < 2 || text.front() != '{' || text.back() != '}' || text.contains('\0')) {
        return std::unexpected(malformed("one-dimensional array (dimensions unsupported)"));
    }
    text.remove_prefix(1);
    text.remove_suffix(1);
    std::vector<std::optional<std::string>> elements;
    while (!text.empty() && space(text.front())) {
        text.remove_prefix(1);
    }
    if (text.empty()) {
        return elements;
    }
    while (!text.empty()) {
        std::string item;
        const bool quoted = text.front() == '"';
        bool escaped = false;
        if (quoted) {
            text.remove_prefix(1);
        }
        bool closed = !quoted;
        std::size_t trailing_spaces{};
        while (!text.empty()) {
            const char character = text.front();
            if (character == '\\') {
                text.remove_prefix(1);
                if (text.empty()) {
                    return std::unexpected(malformed("array escape"));
                }
                item += text.front();
                text.remove_prefix(1);
                escaped = true;
                trailing_spaces = 0;
            } else if (quoted && character == '"') {
                text.remove_prefix(1);
                closed = true;
                break;
            } else if (!quoted && character == ',') {
                break;
            } else {
                if (!quoted && (character == '{' || character == '}' || character == '"')) {
                    return std::unexpected(malformed("flat array element (nesting unsupported)"));
                }
                item += character;
                text.remove_prefix(1);
                trailing_spaces = space(character) ? trailing_spaces + 1 : 0;
            }
        }
        if (!closed) {
            return std::unexpected(malformed("array quote"));
        }
        if (!quoted) {
            item.resize(item.size() - trailing_spaces);
            if (item.empty()) {
                return std::unexpected(malformed("empty unquoted array element"));
            }
        }
        while (!text.empty() && space(text.front())) {
            text.remove_prefix(1);
        }
        if (!text.empty() && text.front() != ',') {
            return std::unexpected(malformed("array delimiter"));
        }
        const bool null_word = item.size() == 4 && (item[0] == 'N' || item[0] == 'n') &&
                               (item[1] == 'U' || item[1] == 'u') &&
                               (item[2] == 'L' || item[2] == 'l') &&
                               (item[3] == 'L' || item[3] == 'l');
        elements.push_back(!quoted && !escaped && null_word ? std::nullopt
                                                            : std::optional{std::move(item)});
        if (text.empty()) {
            return elements;
        }
        text.remove_prefix(1);
        while (!text.empty() && space(text.front())) {
            text.remove_prefix(1);
        }
        if (text.empty()) {
            return std::unexpected(malformed("trailing array delimiter"));
        }
    }
    return elements;
}
} // namespace asterorm::detail
