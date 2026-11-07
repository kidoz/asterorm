#pragma once
#include <memory>
#include <stdexcept>
#include <utility>

#include "asterorm/core/session.hpp"

namespace asterorm::testing {
// Optional rollback-only test scope; no Catch2 dependency or implicit fixture setup.
template <typename Session> class transactional_session {
  public:
    static result<transactional_session> create(Session& session) {
        auto state = detail::testing_session_access::create(session);
        if (!state) {
            return std::unexpected(std::move(state.error()));
        }
        return transactional_session{session, std::move(*state)};
    }
    transactional_session(const transactional_session&) = delete;
    transactional_session& operator=(const transactional_session&) = delete;
    transactional_session(transactional_session&& other) noexcept
        : owner_(std::exchange(other.owner_, nullptr)), state_(std::move(other.state_)) {}
    transactional_session& operator=(transactional_session&&) = delete;
    ~transactional_session() noexcept {
        if (owner_) {
            detail::testing_session_access::abandon(*owner_, state_);
        }
    }

    Session& session() {
        if (!owner_ || !detail::testing_session_access::active(*owner_, state_)) {
            throw std::logic_error("Transactional test scope is not active");
        }
        return *owner_;
    }
    result<void> finish() {
        auto state = state_;
        auto* owner = owner_; // observer destruction may replace/destroy this public handle
        return owner ? detail::testing_session_access::finish(*owner, state) : result<void>{};
    }

  private:
    transactional_session(Session& owner, std::shared_ptr<detail::testing_scope_state> state)
        : owner_(&owner), state_(std::move(state)) {}
    Session* owner_;
    std::shared_ptr<detail::testing_scope_state> state_;
};
} // namespace asterorm::testing
