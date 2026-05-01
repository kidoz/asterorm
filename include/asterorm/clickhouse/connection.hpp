#pragma once
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "asterorm/clickhouse/result.hpp"
#include "asterorm/core/result.hpp"

namespace clickhouse {
class Client;
}

namespace asterorm::ch {

class connection {
    std::unique_ptr<clickhouse::Client> client_;

  public:
    connection();
    ~connection();

    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;

    connection(connection&&) noexcept;
    connection& operator=(connection&&) noexcept;

    bool is_open() const;
    void close();

    explicit connection(std::unique_ptr<clickhouse::Client> client);

    asterorm::result<ch::result> execute(std::string_view sql);
    asterorm::result<ch::result>
    execute_params(std::string_view sql, const std::vector<std::optional<std::string>>& params);

    asterorm::result<ch::result>
    execute_prepared(std::string_view sql, const std::vector<std::optional<std::string>>& params);
};

} // namespace asterorm::ch