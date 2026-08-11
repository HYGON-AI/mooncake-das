#pragma once

#ifdef MOONCAKE_EP_USE_HCU
#include <ATen/hip/HIPContext.h>
#include <ATen/hip/impl/HIPStreamMasqueradingAsCUDA.h>
#include <c10/hip/HIPStream.h>
#else
#include <ATen/cuda/CUDAContext.h>
#endif
#include <memory>
#include <mooncake_ep_exception.cuh>
#include <torch/torch.h>

namespace mooncake {

#ifdef MOONCAKE_EP_USE_HCU
using EpEventStream =
    decltype(c10::hip::getCurrentHIPStreamMasqueradingAsCUDA());

inline EpEventStream ep_current_stream() {
    return c10::hip::getCurrentHIPStreamMasqueradingAsCUDA();
}
#else
using EpEventStream = at::cuda::CUDAStream;

inline EpEventStream ep_current_stream() {
    return at::cuda::getCurrentCUDAStream();
}
#endif

struct EventHandle {
    std::shared_ptr<torch::Event> event;

    EventHandle() {
        event = std::make_shared<torch::Event>(torch::kCUDA);
        event->record(ep_current_stream());
    }

    explicit EventHandle(const EpEventStream& stream) {
        event = std::make_shared<torch::Event>(torch::kCUDA);
        event->record(stream);
    }

    EventHandle(const EventHandle& other) = default;

    void current_stream_wait() const {
        ep_current_stream().unwrap().wait(*event);
    }

    void synchronize() const { event->synchronize(); }
};

inline torch::Event create_event(const EpEventStream& s) {
    auto event = torch::Event(torch::kCUDA);
    event.record(s);
    return event;
}

inline void stream_wait(const EpEventStream& s_0,
                        const EpEventStream& s_1) {
    EP_HOST_ASSERT(s_0.id() != s_1.id());
    s_0.unwrap().wait(create_event(s_1));
}

inline void stream_wait(const EpEventStream& s,
                        const EventHandle& event) {
    s.unwrap().wait(*event.event);
}

}  // namespace mooncake
