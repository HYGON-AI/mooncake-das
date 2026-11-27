#pragma once

#include "real_client.h"
#include "device/accelerator_registry.h"
#include "ext/ahe_loader.h"
#include "ext/mooncake_private_ext_abi.h"

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>

namespace mooncake {

// Public façade over the accelerator host-to-device extension.
//
// The persistent streams, their synchronization workers and the batch copy
// engine live in the private provider. This class only keeps one provider
// handle per device, serializes submissions through a per-device lease and
// drains every handle on shutdown. When no provider is loaded the leases stay
// usable: the caller falls back to the public per-range copy on the implicit
// stream.
class RealClient::DfsH2dStreamPool {
   private:
    struct DeviceKey {
        const device::AcceleratorDevice *device = nullptr;
        int32_t device_id = -1;

        bool operator<(const DeviceKey &other) const {
            if (device != other.device) {
                return std::less<const device::AcceleratorDevice *>{}(
                    device, other.device);
            }
            return device_id < other.device_id;
        }
    };

    struct DeviceState {
        DeviceState(const device::AcceleratorDevice *device, int32_t device_id)
            : device(device), device_id(device_id) {}

        const device::AcceleratorDevice *device;
        int32_t device_id;
        std::mutex mutex;
        // Opaque provider handle; nullptr when the extension is unavailable.
        void *handle = nullptr;
        uint32_t stream_count = 0;
        bool initialized = false;
    };

   public:
    class Lease {
       public:
        Lease(std::shared_ptr<DeviceState> state,
              std::unique_lock<std::mutex> lock)
            : state_(std::move(state)), lock_(std::move(lock)) {}

        const device::AcceleratorDevice *device() const {
            return state_->device;
        }
        int32_t device_id() const { return state_->device_id; }
        bool accelerated() const { return state_->handle != nullptr; }
        uint32_t stream_count() const {
            return state_->handle != nullptr ? state_->stream_count : 0;
        }

        bool Submit(const mooncake_ahe_copy_range *ranges, size_t range_count,
                    uint32_t stream_key) {
            if (state_->handle == nullptr) return false;
            return ext::AheProvider::Instance().Submit(
                state_->handle, ranges, range_count, stream_key);
        }

        // With a provider this waits for every stream of the device handle;
        // without one it drains the implicit stream used by the fallback.
        bool Synchronize() {
            if (state_->handle == nullptr) {
                // The implicit stream belongs to the current device; switch
                // back before draining so a batch that spans several devices
                // waits on the right one instead of whatever was last set.
                state_->device->SetContext(state_->device_id);
                return state_->device->SynchronizeStream(nullptr);
            }
            return ext::AheProvider::Instance().Sync(state_->handle);
        }

       private:
        std::shared_ptr<DeviceState> state_;
        std::unique_lock<std::mutex> lock_;
    };

    explicit DfsH2dStreamPool(size_t streams_per_device)
        : streams_per_device_(streams_per_device) {}

    ~DfsH2dStreamPool() {
        if (!Shutdown()) {
            LOG(ERROR)
                << "Failed to synchronize a DFS H2D stream during shutdown";
        }
    }

    std::unique_ptr<Lease> Acquire(
        const device::AcceleratorDevice *accelerator, int32_t device_id) {
        std::shared_ptr<DeviceState> state;
        std::unique_lock<std::mutex> lock(mutex_);
        if (shutting_down_) return nullptr;
        DeviceKey key{accelerator, device_id};
        auto [it, inserted] = states_.try_emplace(key);
        if (inserted) {
            it->second = std::make_shared<DeviceState>(accelerator, device_id);
        }
        state = it->second;

        // Keep the map lock until this lease owns the device state. Shutdown
        // can then swap and destroy states only after every lease is returned.
        std::unique_lock<std::mutex> state_lock(state->mutex);
        lock.unlock();
        state->device->SetContext(state->device_id);
        if (!state->initialized) {
            InitializeDevice(*state);
            state->initialized = true;
        }
        return std::make_unique<Lease>(state, std::move(state_lock));
    }

    bool Shutdown() {
        std::map<DeviceKey, std::shared_ptr<DeviceState>> states;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (shutting_down_) {
                shutdown_cv_.wait(lock,
                                  [this] { return shutdown_complete_; });
                return shutdown_succeeded_;
            }
            shutting_down_ = true;
            states.swap(states_);
        }

        bool succeeded = true;
        for (auto &[_, state] : states) {
            std::lock_guard<std::mutex> state_lock(state->mutex);
            state->device->SetContext(state->device_id);
            if (state->handle != nullptr) {
                if (!ext::AheProvider::Instance().Sync(state->handle)) {
                    succeeded = false;
                }
                ext::AheProvider::Instance().Destroy(state->handle);
                state->handle = nullptr;
            }
            state->stream_count = 0;
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shutdown_succeeded_ = succeeded;
            shutdown_complete_ = true;
        }
        shutdown_cv_.notify_all();
        return succeeded;
    }

   private:
    void InitializeDevice(DeviceState &state) {
        ext::AheProvider &provider = ext::AheProvider::Instance();
        if (!provider.available()) return;
        const uint32_t count = static_cast<uint32_t>(streams_per_device_);
        if (count == 0) return;
        void *handle = provider.Create(state.device_id, count);
        if (handle == nullptr) return;
        state.handle = handle;
        state.stream_count = count;
    }

    const size_t streams_per_device_;
    std::mutex mutex_;
    std::condition_variable shutdown_cv_;
    std::map<DeviceKey, std::shared_ptr<DeviceState>> states_;
    bool shutting_down_ = false;
    bool shutdown_complete_ = false;
    bool shutdown_succeeded_ = true;
};

}  // namespace mooncake
