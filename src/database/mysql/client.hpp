#pragma once

#include <mysql.h>

namespace asterorm::mysql::detail {

// mysql_init performs implicit library/thread initialization. Explicit guards
// also cover pooled connections used on a thread other than their creator.
inline bool initialize_client_thread() {
    static const bool initialized = mysql_library_init(0, nullptr, nullptr) == 0;
    if (!initialized)
        return false;
    struct thread_state {
        bool initialized{mysql_thread_init() == 0};
        ~thread_state() {
            if (initialized)
                mysql_thread_end();
        }
    };
    thread_local thread_state state;
    return state.initialized;
}

} // namespace asterorm::mysql::detail
