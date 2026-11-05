#pragma once

#include "real_client.h"
#include "device/accelerator_registry.h"
#include <functional>

namespace mooncake {

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
        struct SyncWorker {
            std::mutex mutex;
            std::condition_variable task_cv;
            std::condition_variable completion_cv;
            std::thread thread;
            bool has_task = false;
            bool stopping = false;
            bool ready = false;
            bool result = false;
        };

        DeviceState(const device::AcceleratorDevice *device, int32_t device_id)
            : device(device), device_id(device_id) {}

        ~DeviceState() { StopSyncWorkers(); }

        bool StartSyncWorkers() {
            try {
                sync_workers.reserve(streams.size());
                for (void *stream : streams) {
                    auto worker = std::make_unique<SyncWorker>();
                    SyncWorker *worker_ptr = worker.get();
                    worker->thread = std::thread([this, worker_ptr, stream] {
                        device->SetContext(device_id);
                        {
                            std::lock_guard<std::mutex> lock(
                                worker_ptr->mutex);
                            worker_ptr->ready = true;
                        }
                        worker_ptr->completion_cv.notify_one();
                        while (true) {
                            std::unique_lock<std::mutex> lock(
                                worker_ptr->mutex);
                            worker_ptr->task_cv.wait(lock, [worker_ptr] {
                                return worker_ptr->has_task ||
                                       worker_ptr->stopping;
                            });
                            if (worker_ptr->stopping &&
                                !worker_ptr->has_task) {
                                break;
                            }
                            lock.unlock();
                            bool result = false;
                            try {
                                result = device->SynchronizeStream(stream);
                            } catch (const std::exception &error) {
                                LOG(ERROR)
                                    << "Persistent DFS H2D sync worker failed: "
                                    << error.what();
                            } catch (...) {
                                LOG(ERROR) << "Persistent DFS H2D sync worker "
                                              "failed";
                            }
                            lock.lock();
                            worker_ptr->result = result;
                            worker_ptr->has_task = false;
                            lock.unlock();
                            worker_ptr->completion_cv.notify_one();
                        }
                    });
                    sync_workers.push_back(std::move(worker));
                    std::unique_lock<std::mutex> lock(worker_ptr->mutex);
                    worker_ptr->completion_cv.wait(
                        lock, [worker_ptr] { return worker_ptr->ready; });
                }
            } catch (const std::exception &error) {
                LOG(ERROR) << "Failed to create persistent DFS H2D sync "
                              "worker: "
                           << error.what();
                StopSyncWorkers();
                return false;
            } catch (...) {
                LOG(ERROR) << "Failed to create persistent DFS H2D sync "
                              "worker";
                StopSyncWorkers();
                return false;
            }
            return true;
        }

        bool SynchronizeStreams(const std::vector<uint8_t> &used_streams,
                                std::vector<uint8_t> *results) {
            if (!results || used_streams.size() != sync_workers.size()) {
                return false;
            }
            results->assign(sync_workers.size(), 1);
            for (size_t i = 0; i < sync_workers.size(); ++i) {
                if (!used_streams[i]) continue;
                auto &worker = *sync_workers[i];
                std::lock_guard<std::mutex> lock(worker.mutex);
                if (worker.stopping || worker.has_task) return false;
            }
            for (size_t i = 0; i < sync_workers.size(); ++i) {
                if (!used_streams[i]) continue;
                auto &worker = *sync_workers[i];
                std::lock_guard<std::mutex> lock(worker.mutex);
                worker.has_task = true;
            }
            for (size_t i = 0; i < sync_workers.size(); ++i) {
                if (used_streams[i]) sync_workers[i]->task_cv.notify_one();
            }
            for (size_t i = 0; i < sync_workers.size(); ++i) {
                if (!used_streams[i]) continue;
                auto &worker = *sync_workers[i];
                std::unique_lock<std::mutex> lock(worker.mutex);
                worker.completion_cv.wait(
                    lock, [&worker] { return !worker.has_task; });
                (*results)[i] = worker.result;
            }
            return true;
        }

        void StopSyncWorkers() {
            for (auto &worker : sync_workers) {
                {
                    std::lock_guard<std::mutex> lock(worker->mutex);
                    worker->stopping = true;
                }
                worker->task_cv.notify_one();
            }
            for (auto &worker : sync_workers) {
                if (worker->thread.joinable()) worker->thread.join();
            }
            sync_workers.clear();
        }

        const device::AcceleratorDevice *device;
        int32_t device_id;
        std::mutex mutex;
        std::vector<void *> streams;
        std::vector<std::unique_ptr<SyncWorker>> sync_workers;
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
        const std::vector<void *> &streams() const { return state_->streams; }
        bool SynchronizeStreams(const std::vector<uint8_t> &used_streams,
                                std::vector<uint8_t> *results) {
            return state_->SynchronizeStreams(used_streams, results);
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
            it->second =
                std::make_shared<DeviceState>(accelerator, device_id);
        }
        state = it->second;

        // Keep the map lock until this lease owns the device state. Shutdown
        // can then swap and destroy states only after every lease is returned.
        std::unique_lock<std::mutex> state_lock(state->mutex);
        lock.unlock();
        state->device->SetContext(state->device_id);
        if (!state->initialized) {
            state->streams.reserve(streams_per_device_);
            for (size_t i = 0; i < streams_per_device_; ++i) {
                void *stream = nullptr;
                if (!state->device->CreateStream(&stream)) break;
                state->streams.push_back(stream);
            }
            if (!state->StartSyncWorkers()) {
                for (void *stream : state->streams) {
                    state->device->DestroyStream(stream);
                }
                state->streams.clear();
                return nullptr;
            }
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
            std::vector<uint8_t> results;
            const std::vector<uint8_t> used_streams(state->streams.size(), 1);
            if (!state->SynchronizeStreams(used_streams, &results)) {
                succeeded = false;
            } else {
                for (uint8_t result : results) succeeded &= result != 0;
            }
            state->StopSyncWorkers();
            state->device->SetContext(state->device_id);
            for (void *stream : state->streams) {
                state->device->DestroyStream(stream);
            }
            state->streams.clear();
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
    const size_t streams_per_device_;
    std::mutex mutex_;
    std::condition_variable shutdown_cv_;
    std::map<DeviceKey, std::shared_ptr<DeviceState>> states_;
    bool shutting_down_ = false;
    bool shutdown_complete_ = false;
    bool shutdown_succeeded_ = true;
};

}  // namespace mooncake
