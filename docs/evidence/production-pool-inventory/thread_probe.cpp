#include <thread>

int main() {
    std::jthread worker{[] {}};
}
