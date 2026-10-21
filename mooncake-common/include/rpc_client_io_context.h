#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_io/io_context_pool.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

namespace mooncake {

std::shared_ptr<coro_io::io_context_pool> CreateRpcClientIoContextPool(
    uint32_t thread_count);

class RpcDrainGuard {
   private:
    struct State {
        std::atomic<bool> stopping{false};
        std::atomic<int> inflight{0};
        std::mutex mutex;
        std::condition_variable cv;
    };

   public:
    class ScopedCall {
       public:
        explicit ScopedCall(const RpcDrainGuard& guard)
            : state_(guard.state_), ok_(try_enter(*state_)) {}
        ~ScopedCall() {
            if (ok_) {
                leave(*state_);
            }
        }
        ScopedCall(const ScopedCall&) = delete;
        ScopedCall& operator=(const ScopedCall&) = delete;
        bool ok() const { return ok_; }

       private:
        static bool try_enter(State& state) {
            if (state.stopping.load(std::memory_order_acquire)) {
                return false;
            }
            state.inflight.fetch_add(1, std::memory_order_acq_rel);
            if (state.stopping.load(std::memory_order_acquire)) {
                leave(state);
                return false;
            }
            return true;
        }

        static void leave(State& state) {
            if (state.inflight.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                std::lock_guard<std::mutex> lock(state.mutex);
                state.cv.notify_all();
            }
        }

        std::shared_ptr<State> state_;
        bool ok_;
    };

    bool drain_for(std::chrono::milliseconds timeout) {
        state_->stopping.store(true, std::memory_order_release);
        std::unique_lock<std::mutex> lock(state_->mutex);
        return state_->cv.wait_for(lock, timeout, [&] {
            return state_->inflight.load(std::memory_order_acquire) == 0;
        });
    }

   private:
    std::shared_ptr<State> state_ = std::make_shared<State>();
};

template <typename PoolTag>
coro_io::io_context_pool& GetRpcClientIoContextPool(uint32_t thread_count) {
    static const auto io_pool = CreateRpcClientIoContextPool(thread_count);
    return *io_pool;
}

namespace detail {

using MasterClientPool =
    coro_io::client_pool<coro_rpc::coro_rpc_client>;

inline std::shared_ptr<MasterClientPool> SharedPoolRegistryImpl(
    std::string_view address, MasterClientPool::pool_config config,
    coro_io::io_context_pool& io_context_pool) {
    static std::mutex registry_mutex;
    static std::unordered_map<std::string, std::shared_ptr<MasterClientPool>>
        registry;
    std::lock_guard<std::mutex> lock(registry_mutex);

    std::string key(address);
    auto append = [&key](auto value) {
        key.push_back('|');
        key.append(std::to_string(value));
    };
    append(config.max_connection);
    append(config.connect_retry_count);
    append(config.reconnect_wait_time.count());
    append(config.client_config.connect_timeout_duration.count());
    append(config.client_config.request_timeout_duration.count());
    append(static_cast<size_t>(config.client_config.socket_config.index()));

    auto& pool = registry[key];
    if (!pool) {
        pool = MasterClientPool::create(std::string(address), std::move(config),
                                        io_context_pool);
    }
    return pool;
}

}  // namespace detail

/**
 * A replaceable client pool. Pools are retained process-wide because ylt's
 * background reconnect coroutines may outlive the active holder.
 */
class RpcClientPool {
   public:
    using ClientPool = coro_io::client_pool<coro_rpc::coro_rpc_client>;
    using PoolConfig = ClientPool::pool_config;

    explicit RpcClientPool(coro_io::io_context_pool& io_context_pool,
                           PoolConfig config = {})
        : io_context_pool_(io_context_pool), config_(std::move(config)) {
        config_.host_alive_detect_duration = std::chrono::seconds(0);
    }

    std::shared_ptr<ClientPool> GetOrCreateClientPool(
        std::string_view address) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        if (!client_pool_ || address_ != address) {
            client_pool_ = detail::SharedPoolRegistryImpl(
                address, config_, io_context_pool_);
            address_ = address;
        }
        return client_pool_;
    }

    std::shared_ptr<ClientPool> GetClientPool() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return client_pool_;
    }

   private:
    mutable std::shared_mutex mutex_;
    coro_io::io_context_pool& io_context_pool_;
    PoolConfig config_;
    std::string address_;
    std::shared_ptr<ClientPool> client_pool_;
};

}  // namespace mooncake
