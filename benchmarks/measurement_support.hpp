#pragma once
#include <cstddef>
#include <cstdint>

namespace asterorm::bench {
struct allocation_counts {
    std::uint64_t calls{};
    std::uint64_t bytes{};
};

// Defined in another translation unit, with LTO disabled: the caller must make
// the complete result observable. This adds the same call overhead to both APIs.
void observe(const void* result) noexcept;
void begin_allocations() noexcept;
allocation_counts end_allocations() noexcept;
void verify_allocation_probe();
} // namespace asterorm::bench
