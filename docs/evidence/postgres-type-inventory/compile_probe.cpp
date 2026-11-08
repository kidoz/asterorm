#include "asterorm/core/codecs.hpp"
#include "asterorm/core/entity_traits.hpp"
#include "asterorm/sql/typed.hpp"
#include <unordered_map>

#ifndef TYPE_CASE
#define TYPE_CASE 0
#endif
struct inner_value {
    int number{};
};
struct outer_value {
    inner_value nested;
};
template <> struct asterorm::entity_traits<inner_value> {
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&inner_value::number>("number"));
};
template <> struct asterorm::entity_traits<outer_value> {
    static constexpr auto columns =
        std::make_tuple(asterorm::column<&outer_value::nested>("nested"));
};
struct custom_value {
    std::string text;
};
template <> struct asterorm::codec<custom_value> {
    static std::optional<std::string> encode(const custom_value& value) {
        return value.text;
    }
    static asterorm::result<void> decode(const std::optional<std::string>& text,
                                         custom_value& value) {
        value.text = text.value_or("");
        return {};
    }
};
int main() {
    try {
#if TYPE_CASE == 0
        static_assert(asterorm::detail::has_custom_codec<custom_value>::value);
        auto scalar = asterorm::encode_parameter(asterorm::numeric{"1.20"});
        auto json = asterorm::encode_parameter(asterorm::jsonb{"{\"a\":1}"});
        auto array = asterorm::encode_parameter(std::vector<std::optional<int>>{1, std::nullopt});
        auto custom = asterorm::encode_parameter(custom_value{"value"});
        return scalar.error || json.error || array.error || custom.error ? 1 : 0;
#elif TYPE_CASE == 1
        (void)sizeof(asterorm::range<int>);
#elif TYPE_CASE == 2
        (void)sizeof(asterorm::interval);
#elif TYPE_CASE == 3
        (void)sizeof(asterorm::network);
#elif TYPE_CASE == 4
        (void)sizeof(asterorm::macaddr);
#elif TYPE_CASE == 5
        (void)sizeof(asterorm::hstore);
#elif TYPE_CASE == 6
        auto value = asterorm::encode_parameter(
            std::unordered_map<std::string, std::optional<std::string>>{});
#elif TYPE_CASE == 7
        auto value = asterorm::encode_parameter(outer_value{});
#elif TYPE_CASE == 8
        auto value = asterorm::sql::json_get(asterorm::sql::col("document"), "a");
#elif TYPE_CASE == 9
        auto value =
            asterorm::sql::typed::json_get(asterorm::sql::typed::col<&outer_value::nested>, "a");
#elif TYPE_CASE == 10
        auto value = asterorm::sql::jsonb_path_query(asterorm::sql::col("document"), "$.a[*]");
#endif
#if TYPE_CASE >= 6
        (void)value;
#endif
    } catch (...) {
        return 2;
    }
}
