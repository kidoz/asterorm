#pragma once
#include "asterorm/sql/compiler.hpp"
#include "asterorm/sql/window.hpp"

namespace asterorm::sql::window_detail {
class parameter_scope {
  public:
    parameter_scope(bool& flag, bool enabled) : flag_(flag), previous_(flag) {
        flag_ = enabled;
    }
    ~parameter_scope() {
        flag_ = previous_;
    }
    parameter_scope(const parameter_scope&) = delete;
    parameter_scope& operator=(const parameter_scope&) = delete;

  private:
    bool& flag_;
    bool previous_;
};
[[nodiscard]] bool is_aggregate(std::string name);
[[nodiscard]] result<void> validate_distinct(const select_ast& ast, dialect target);
[[nodiscard]] result<void> validate_function(const func_expr& function, dialect target);
} // namespace asterorm::sql::window_detail
