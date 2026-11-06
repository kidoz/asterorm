#include <optional>
#include <string>
#include <tuple>

#include "asterorm/sql/typed.hpp"

#ifndef ASTERORM_COMPILE_CASE
#define ASTERORM_COMPILE_CASE 0
#endif

namespace {
struct compile_user {
    int age;
    double score;
    std::string email;
    bool active;
    bool verified;
    std::optional<bool> enabled;
    int unmapped;
};

struct compile_other {
    int age;
};

struct unknown_entity {};
} // namespace

template <> struct asterorm::entity_traits<compile_user> {
    [[maybe_unused]] static constexpr const char* table = "compile_users";
    [[maybe_unused]] static constexpr auto columns =
        std::make_tuple(asterorm::column<&compile_user::age>("age"),
                        asterorm::column<&compile_user::score>("score"),
                        asterorm::column<&compile_user::email>("email"),
                        asterorm::column<&compile_user::active>("active"),
                        asterorm::column<&compile_user::verified>("verified"),
                        asterorm::column<&compile_user::enabled>("enabled"));
};

template <> struct asterorm::entity_traits<compile_other> {
    [[maybe_unused]] static constexpr const char* table = "compile_others";
    [[maybe_unused]] static constexpr auto columns =
        std::make_tuple(asterorm::column<&compile_other::age>("age"));
};

namespace {
namespace typed = asterorm::sql::typed;

[[maybe_unused]] void check_typed_dsl() {
#if ASTERORM_COMPILE_CASE == 0
    // These controls use the same traits, operators and projection API as the
    // negative cases. The normal core test build also compiles this branch.
    (void)(typed::col<&compile_user::active> == typed::val(true));
    (void)(typed::col<&compile_user::active> != typed::val(false));
    (void)(typed::col<&compile_user::active> < typed::val(true));
    (void)(typed::col<&compile_user::active> <= typed::val(true));
    (void)(typed::col<&compile_user::active> > typed::val(false));
    (void)(typed::col<&compile_user::active> >= typed::val(false));
    (void)(typed::col<&compile_user::enabled> == typed::val(true));
    (void)(typed::col<&compile_user::active> == typed::col<&compile_user::verified>);
    (void)(typed::col<&compile_user::age> > typed::val(18));
    (void)(typed::col<&compile_user::age> > typed::val(18.5));
    (void)(typed::col<&compile_user::score> >= typed::col<&compile_user::age>);
    (void)(typed::col<&compile_user::email> == typed::val("alice@example.com"));
    (void)(typed::col<&compile_user::email> != typed::val(std::string{"bob@example.com"}));
    (void)typed::select<compile_user>().where(typed::col<&compile_user::age> > typed::val(18));
    (void)typed::select_cols<std::tuple<int, std::string>>(typed::col<&compile_user::age>,
                                                           typed::col<&compile_user::email>);
    (void)typed::is_null(typed::col<&compile_user::enabled>);
#elif ASTERORM_COMPILE_CASE == 1
    (void)(typed::col<&compile_user::active> == typed::val("not-a-boolean"));
#elif ASTERORM_COMPILE_CASE == 2
    const char* text_value = "not-a-boolean";
    (void)(typed::col<&compile_user::active> != typed::val(text_value));
#elif ASTERORM_COMPILE_CASE == 3
    (void)(typed::col<&compile_user::enabled> < typed::val("not-a-boolean"));
#elif ASTERORM_COMPILE_CASE == 4
    (void)(typed::col<&compile_user::active> <= typed::val(std::string{"not-a-boolean"}));
#elif ASTERORM_COMPILE_CASE == 5
    (void)(typed::col<&compile_user::active> > typed::val(1));
#elif ASTERORM_COMPILE_CASE == 6
    (void)(typed::col<&compile_user::age> >= typed::val(true));
#elif ASTERORM_COMPILE_CASE == 7
    (void)(typed::col<&compile_user::age> > typed::val(std::string{"x"}));
#elif ASTERORM_COMPILE_CASE == 8
    (void)(typed::col<&compile_user::active> == typed::col<&compile_user::email>);
#elif ASTERORM_COMPILE_CASE == 9
    (void)typed::col<&compile_user::unmapped>;
#elif ASTERORM_COMPILE_CASE == 10
    (void)typed::select<unknown_entity>();
#elif ASTERORM_COMPILE_CASE == 11
    (void)typed::select<compile_user>().where(typed::col<&compile_other::age> > typed::val(18));
#elif ASTERORM_COMPILE_CASE == 12
    (void)typed::select_cols<std::tuple<int>>(typed::col<&compile_user::email>);
#elif ASTERORM_COMPILE_CASE == 13
    (void)(typed::col<&compile_user::age> == typed::null);
#elif ASTERORM_COMPILE_CASE == 14
    (void)typed::select_cols<std::tuple<int, int>>(typed::col<&compile_user::age>,
                                                   typed::col<&compile_other::age>);
#else
#error Unknown ASTERORM_COMPILE_CASE
#endif
}
} // namespace
