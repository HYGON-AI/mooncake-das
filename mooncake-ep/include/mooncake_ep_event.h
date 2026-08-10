#pragma once

#include <ATen/hip/HIPContext.h>
#include <ATen/hip/impl/HIPStreamMasqueradingAsCUDA.h>
#include <c10/hip/HIPStream.h>
#include <memory>
#include <mooncake_ep_exception.cuh>
#include <torch/torch.h>

namespace mooncake {

struct EventHandle {
    std::shared_ptr<torch::Event> event;

    EventHandle() {
        event = std::make_shared<torch::Event>(torch::kCUDA);
        event->record(c10::hip::getCurrentHIPStreamMasqueradingAsCUDA());
    }

    explicit EventHandle(
        const c10::hip::HIPStreamMasqueradingAsCUDA& stream) {
        event = std::make_shared<torch::Event>(torch::kCUDA);
        event->record(stream);
    }

    EventHandle(const EventHandle& other) = default;

    void current_stream_wait() const {
        c10::hip::getCurrentHIPStreamMasqueradingAsCUDA().unwrap().wait(*event);
    }
};

inline torch::Event create_event(
    const c10::hip::HIPStreamMasqueradingAsCUDA& s) {
    auto event = torch::Event(torch::kCUDA);
    event.record(s);
    return event;
}

inline void stream_wait(const c10::hip::HIPStreamMasqueradingAsCUDA& s_0,
                        const c10::hip::HIPStreamMasqueradingAsCUDA& s_1) {
    EP_HOST_ASSERT(s_0.id() != s_1.id());
    s_0.unwrap().wait(create_event(s_1));
}

inline void stream_wait(const c10::hip::HIPStreamMasqueradingAsCUDA& s,
                        const EventHandle& event) {
    s.unwrap().wait(*event.event);
}

}  // namespace mooncake
