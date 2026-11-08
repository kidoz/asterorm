#include <optional>
#include <string>
#include <tuple>

#include "asterorm/sql/typed.hpp"
#include "asterorm/sql/typed_extensions.hpp"
#include "asterorm/sql/typed_window.hpp"

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
    auto window = typed::window().order_by(typed::col<&compile_user::age>);
    (void)typed::select_cols(typed::over(typed::row_number(), window));
    (void)typed::select_cols(
        typed::col<&compile_user::age>,
        typed::over(typed::lag(typed::col<&compile_user::age>, 1, typed::val(0)), window));
    (void)typed::select_cols(
        typed::over(typed::last_value(typed::col<&compile_user::score>),
                    window.rows_between(asterorm::sql::unbounded_preceding(),
                                        asterorm::sql::unbounded_following())));
    (void)typed::select_cols(
        typed::filter(typed::count_all(), typed::col<&compile_user::active> == typed::val(true)));
    (void)typed::select<compile_user>()
        .distinct_on(asterorm::sql::distinct_on_ties::require_tiebreaker,
                     typed::col<&compile_user::active>)
        .order_by(typed::col<&compile_user::active>)
        .order_by(typed::col<&compile_user::age>);
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
    (void)typed::select_cols(typed::col<&compile_user::email>, typed::col<&compile_user::active>,
                             typed::col<&compile_user::enabled>);
    (void)typed::select_cols(typed::count_all(), typed::sum(typed::col<&compile_user::age>));
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
#elif ASTERORM_COMPILE_CASE == 15
    (void)typed::select_cols(typed::col<&compile_user::age>, typed::col<&compile_other::age>);
#elif ASTERORM_COMPILE_CASE == 16
    (void)typed::select_cols(typed::count_all());
#elif ASTERORM_COMPILE_CASE == 17
    (void)typed::select_cols();
#elif ASTERORM_COMPILE_CASE == 18
    (void)typed::window()
        .partition_by(typed::col<&compile_user::age>)
        .order_by(typed::col<&compile_other::age>);
#elif ASTERORM_COMPILE_CASE == 19
    (void)typed::over(typed::row_number(),
                      typed::window().partition_by(typed::col<&compile_user::age>));
#elif ASTERORM_COMPILE_CASE == 20
    (void)typed::over(typed::first_value(typed::col<&compile_user::age>),
                      typed::window().order_by(typed::col<&compile_user::age>));
#elif ASTERORM_COMPILE_CASE == 21
    (void)typed::lag(typed::col<&compile_user::age>, true);
#elif ASTERORM_COMPILE_CASE == 22
    (void)typed::lag(typed::col<&compile_user::age>, 1, typed::val(1.5));
#elif ASTERORM_COMPILE_CASE == 23
    (void)typed::nth_value(typed::col<&compile_user::age>, 1.5);
#elif ASTERORM_COMPILE_CASE == 24
    (void)typed::over(typed::col<&compile_user::age>,
                      typed::window().order_by(typed::col<&compile_user::age>));
#elif ASTERORM_COMPILE_CASE == 25
    (void)typed::over(typed::lag(typed::col<&compile_user::age>),
                      typed::window().order_by(typed::col<&compile_other::age>));
#elif ASTERORM_COMPILE_CASE == 26
    (void)typed::select_cols<std::tuple<int>>(
        typed::over(typed::lag(typed::col<&compile_user::age>),
                    typed::window().order_by(typed::col<&compile_user::age>)));
#elif ASTERORM_COMPILE_CASE == 27
    (void)typed::over(typed::sum(typed::col<&compile_user::active>),
                      typed::window().order_by(typed::col<&compile_user::age>));
#elif ASTERORM_COMPILE_CASE == 28
    (void)asterorm::sql::preceding(1.5);
#elif ASTERORM_COMPILE_CASE == 29
    (void)typed::filter(typed::sum(typed::col<&compile_user::age>),
                        typed::col<&compile_other::age> > typed::val(0));
#elif ASTERORM_COMPILE_CASE == 30
    (void)typed::filter(
        typed::over(typed::rank(), typed::window().order_by(typed::col<&compile_user::age>)),
        typed::col<&compile_user::active> == typed::val(true));
#elif ASTERORM_COMPILE_CASE == 31
    (void)typed::select<compile_user>().distinct_on(asterorm::sql::distinct_on_ties::allow_peers,
                                                    typed::col<&compile_other::age>);
#elif ASTERORM_COMPILE_CASE == 32
    (void)typed::select_cols<std::tuple<int>>(
        typed::filter(typed::sum(typed::col<&compile_user::age>),
                      typed::col<&compile_user::active> == typed::val(true)));
#elif ASTERORM_COMPILE_CASE == 33
    (void)typed::filter(typed::sum(typed::col<&compile_user::active>),
                        typed::col<&compile_user::active> == typed::val(true));
#else
#error Unknown ASTERORM_COMPILE_CASE
#endif
}
} // namespace
