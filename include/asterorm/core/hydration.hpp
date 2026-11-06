#pragma once

#include <cstddef>
#include <tuple>
#include <utility>

#include "asterorm/core/codecs.hpp"
#include "asterorm/core/entity_traits.hpp"
#include "asterorm/core/result.hpp"

namespace asterorm::detail {

template <typename T, typename Result, std::size_t... Is>
asterorm::result<void> hydrate_entity(const Result& res, int row, T& entity,
                                      std::index_sequence<Is...> /*seq*/) {
    using traits = entity_traits<T>;
    asterorm::result<void> decode_err;
    (..., [&]() {
        if (!decode_err)
            return;
        auto col = std::get<Is>(traits::columns);
        auto val_str = res.get_string(row, static_cast<int>(Is));
        auto err = asterorm::decode(val_str, entity.*(col.member_ptr));
        if (!err)
            decode_err = std::move(err);
    }());
    return decode_err;
}

template <typename T, typename Result>
asterorm::result<void> hydrate_entity(const Result& res, int row, T& entity) {
    using traits = entity_traits<T>;
    return hydrate_entity(res, row, entity,
                          std::make_index_sequence<std::tuple_size_v<decltype(traits::columns)>>{});
}

} // namespace asterorm::detail
