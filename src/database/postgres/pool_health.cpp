#include "asterorm/postgres/connection.hpp"
#include "diagnostics.hpp"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <memory>
#ifdef _WIN32
#include <winsock2.h>
#else
#include <poll.h>
#endif

namespace asterorm::pg {
namespace {
using clock = std::chrono::steady_clock;
db_error health_timeout() {
    db_error error;
    error.kind = db_error_kind::timeout;
    error.message = "PostgreSQL pool pre-ping deadline expired";
    return error;
}
db_error health_error(PGconn* connection, const char* fallback) {
    auto error = diagnostics::make_error_from_pgresult(connection, nullptr,
                                                       db_error_kind::connection_failed);
    if (!connection || !*PQerrorMessage(connection)) {
        error.message = fallback;
    }
    return error;
}
asterorm::result<void> await_socket(PGconn* connection, bool writing, clock::time_point deadline) {
    const int socket = PQsocket(connection);
    if (socket < 0) {
        return std::unexpected(health_error(connection, "Invalid PostgreSQL pool health socket"));
    }
    for (;;) {
        const auto now = clock::now();
        if (now >= deadline) {
            return std::unexpected(health_timeout());
        }
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        const int milliseconds =
            static_cast<int>(std::clamp<std::int64_t>(remaining.count(), 1, INT_MAX));
#ifdef _WIN32
        fd_set ready;
        FD_ZERO(&ready);
        FD_SET(static_cast<SOCKET>(socket), &ready);
        timeval timeout{milliseconds / 1000, (milliseconds % 1000) * 1000};
        const int selected =
            select(0, writing ? nullptr : &ready, writing ? &ready : nullptr, nullptr, &timeout);
        const bool interrupted = selected < 0 && WSAGetLastError() == WSAEINTR;
#else
        pollfd descriptor{
            .fd = socket,
            .events = static_cast<short>(writing ? POLLOUT : POLLIN),
            .revents = 0,
        };
        const int selected = poll(&descriptor, 1, milliseconds);
        const bool interrupted = selected < 0 && errno == EINTR;
#endif
        if (selected > 0) {
            return {};
        }
        if (selected == 0 || interrupted) {
            continue;
        }
        return std::unexpected(
            health_error(connection, "PostgreSQL pool health socket wait failed"));
    }
}
struct ping_guard {
    connection& owner;
    bool completed{};
    ~ping_guard() {
        if (!completed) {
            owner.close();
        }
    }
};
} // namespace

asterorm::result<void> connection::pool_ping(clock::time_point deadline) {
    if (!is_pool_reusable()) {
        db_error error;
        error.kind = db_error_kind::query_failed;
        error.message = "Pool pre-ping requires an idle reader-free PostgreSQL connection";
        return std::unexpected(std::move(error));
    }
    ping_guard guard{.owner = *this};
    if (clock::now() >= deadline) {
        return std::unexpected(health_timeout());
    }
    const int previous_mode = PQisnonblocking(conn_);
    if (PQsetnonblocking(conn_, 1) != 0 ||
        PQsendQueryParams(conn_, "SELECT 1", 0, nullptr, nullptr, nullptr, nullptr, 0) != 1) {
        return std::unexpected(health_error(conn_, "PostgreSQL pool pre-ping dispatch failed"));
    }
    for (;;) {
        if (clock::now() >= deadline) {
            return std::unexpected(health_timeout());
        }
        const int flushed = PQflush(conn_);
        if (flushed == 0) {
            break;
        }
        if (flushed < 0) {
            return std::unexpected(health_error(conn_, "PostgreSQL pool pre-ping flush failed"));
        }
        auto writable = await_socket(conn_, true, deadline);
        if (!writable) {
            return writable;
        }
    }
    bool received{};
    for (;;) {
        if (clock::now() >= deadline) {
            return std::unexpected(health_timeout());
        }
        if (PQconsumeInput(conn_) != 1) {
            return std::unexpected(health_error(conn_, "PostgreSQL pool pre-ping read failed"));
        }
        if (PQisBusy(conn_)) {
            auto readable = await_socket(conn_, false, deadline);
            if (!readable) {
                return readable;
            }
            continue;
        }
        std::unique_ptr<PGresult, decltype(&PQclear)> response{PQgetResult(conn_), PQclear};
        if (!response) {
            break;
        }
        if (PQresultStatus(response.get()) != PGRES_TUPLES_OK) {
            return std::unexpected(diagnostics::make_error_from_pgresult(
                conn_, response.get(), db_error_kind::query_failed));
        }
        if (received || PQntuples(response.get()) != 1 || PQnfields(response.get()) != 1 ||
            PQgetisnull(response.get(), 0, 0) ||
            std::string_view{PQgetvalue(response.get(), 0, 0)} != "1") {
            db_error error;
            error.kind = db_error_kind::query_failed;
            error.message = "Unexpected PostgreSQL pool pre-ping result";
            return std::unexpected(std::move(error));
        }
        received = true;
    }
    if (!received || PQtransactionStatus(conn_) != PQTRANS_IDLE ||
        PQsetnonblocking(conn_, previous_mode) != 0) {
        return std::unexpected(health_error(conn_, "PostgreSQL pool pre-ping did not finish idle"));
    }
    if (clock::now() >= deadline) {
        return std::unexpected(health_timeout());
    }
    guard.completed = true;
    return {};
}
} // namespace asterorm::pg
