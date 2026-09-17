#include "read_plan.h"
#include "dummy_client.h"
#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>

using namespace mooncake;
using namespace std::chrono_literals;
#define VERIFY(x)                                                \
    do {                                                         \
        if (!(x)) throw std::runtime_error("CHECK failed: " #x); \
    } while (0)

template <class F>
void fails(F f) {
    bool caught = false;
    try {
        f();
    } catch (const std::exception&) {
        caught = true;
    }
    VERIFY(caught);
}
struct Backend : DummyClient {
    std::atomic<int> starts{0}, reads{0}, ends{0};
    bool bad_start = false, short_read = false, bad_end = false;
    int block_call = 0;
    std::promise<void> entered, resume;
    std::shared_future<void> gate = resume.get_future().share();
    std::vector<int> batch_get_session_start(
        const std::vector<std::string>& keys) override {
        ++starts;
        return std::vector<int>(keys.size(), bad_start ? -1 : 0);
    }
    std::vector<int> batch_get_into_multi_buffer_ranges(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& buffers,
        const std::vector<std::vector<size_t>>& sizes,
        const std::vector<std::vector<size_t>>& offsets) override {
        int call = ++reads;
        if (call == block_call) {
            entered.set_value();
            if (gate.wait_for(3s) != std::future_status::ready)
                throw std::runtime_error("test gate timed out");
        }
        std::vector<int> out;
        for (size_t i = 0; i < keys.size(); ++i) {
            size_t total = 0;
            for (size_t j = 0; j < buffers[i].size(); ++j) {
                auto dst = static_cast<unsigned char*>(buffers[i][j]);
                for (size_t k = 0; k < sizes[i][j]; ++k)
                    dst[k] = (offsets[i][j] + k + keys[i][0]) % 251;
                total += sizes[i][j];
            }
            out.push_back(int(total) - (short_read ? 1 : 0));
        }
        return out;
    }
    int batch_get_session_end(const std::vector<std::string>&) override {
        ++ends;
        return bad_end ? -1 : 0;
    }
};
std::vector<ReadLayout> layout(unsigned char* dst, std::string key = "a") {
    return {{{key, "b"},
             {1, 3},
             true,
             {{{reinterpret_cast<size_t>(dst), 32, 8, 2}},
              {{reinterpret_cast<size_t>(dst) + 8, 32, 8, 10}}}}};
}
int main() {
    unsigned char dst[160]{};
    for (bool reuse : {false, true}) {
        auto b = std::make_shared<Backend>();
        ReadPlan plan(b, layout(dst), 2, reuse);
        plan.run();
        plan.wait(1);
        for (auto row : {1, 3})
            for (int j = 0; j < 16; ++j)
                VERIFY(dst[row * 32 + j] ==
                       (2 + j + (row == 1 ? 'a' : 'b')) % 251);
        VERIFY(plan.stats() == std::vector<uint64_t>({2, 4, 32}));
        VERIFY(b->starts == 1 && b->ends == 1);
        fails([&] { plan.run(); });
    }
    {
        auto b = std::make_shared<Backend>();
        b->block_call = 2;
        ReadPlan first(b, layout(dst), 2), overlap(b, layout(dst), 2);
        auto done = std::async(std::launch::async, [&] { first.run(); });
        VERIFY(b->entered.get_future().wait_for(3s) ==
               std::future_status::ready);
        first.wait(0);
        fails([&] { first.stats(); });
        auto last = std::async(std::launch::async, [&] { first.wait(1); });
        VERIFY(last.wait_for(20ms) == std::future_status::timeout);
        fails([&] { overlap.run(); });
        fails([&] { overlap.wait(0); });
        VERIFY(b->starts == 1 && b->ends == 0);
        b->resume.set_value();
        done.get();
        last.get();
        ReadPlan next(b, layout(dst), 2);
        next.run();
        VERIFY(b->ends == 2);
    }
    // Two objects per row, two components, and an empty middle group.
    for (bool reuse : {false, true}) {
        auto b = std::make_shared<Backend>();
        const auto base = reinterpret_cast<size_t>(dst);
        ReadPlan p(b,
                   {{{"c", "d", "e", "f"},
                     {0, 2},
                     false,
                     {{{base, 32, 4, 1}, {base + 4, 32, 4, 5}},
                      {},
                      {{base + 8, 32, 4, 9}, {base + 12, 32, 4, 13}}}}},
                   3, reuse);
        p.run();
        p.wait(2);
        for (int row : {0, 2})
            for (int j = 0; j < 16; ++j) {
                const char key = (row == 0 ? 'c' : 'e') + ((j / 4) % 2);
                VERIFY(dst[row * 32 + j] == (key + j + 1) % 251);
            }
        VERIFY(p.stats() == std::vector<uint64_t>({2, 8, 32}));
    }
    for (int failure = 0; failure < 3; ++failure) {
        auto b = std::make_shared<Backend>();
        b->bad_start = failure == 0;
        b->short_read = failure == 1;
        b->bad_end = failure == 2;
        ReadPlan plan(b, layout(dst), 2);
        fails([&] { plan.run(); });
        fails([&] { plan.wait(1); });
        VERIFY(b->ends == 1 && plan.is_finished());
        if (failure == 2) plan.wait(0);  // Cleanup failure cannot revoke publication.
        if (failure == 0) VERIFY(b->reads == 0);
    }
    {
        auto b = std::make_shared<Backend>();
        fails([&] { ReadPlan p(b, layout(dst), 0); });
        auto bad = layout(dst);
        std::get<1>(bad[0]) = {std::numeric_limits<size_t>::max(), 3};
        fails([&] { ReadPlan p(b, bad, 2); });
        VERIFY(b->starts == 0);
        ReadPlan empty(b, {}, 2);
        empty.run();
        empty.wait(1);
        VERIFY(empty.stats() == std::vector<uint64_t>({0, 0, 0}));
    }
    // Adjacent ranges across layouts satisfy the disjoint-destination contract.
    {
        auto b = std::make_shared<Backend>();
        const auto base = reinterpret_cast<size_t>(dst);
        std::vector<ReadLayout> pools = {
            {{"a"}, {0}, true, {{{base, 0, 8, 0}}, {}}},
            {{"b"}, {0}, true, {{}, {{base + 8, 0, 8, 0}}}}};
        ReadPlan adjacent(b, pools, 2);
        adjacent.run();
        adjacent.wait(0);
        adjacent.wait(1);
    }
    std::cout << "READ_PLAN_TESTS_PASSED\n";
}
