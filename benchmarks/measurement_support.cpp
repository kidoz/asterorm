#include "measurement_support.hpp"
#include <atomic>

void asterorm::bench::observe([[maybe_unused]] const void* result) noexcept {
    std::atomic_signal_fence(std::memory_order_seq_cst);
}
