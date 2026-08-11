#include "transport/hip_transport/hip_transport.h"

#include <gtest/gtest.h>

namespace mooncake {

class HipTransportTestPeer {
   public:
    static void setStreamSynchronize(
        HipTransport::StreamSynchronizeFn function) {
        HipTransport::stream_synchronize_fn_ = function;
    }
    static hipError_t drain(hipStream_t stream) {
        return HipTransport::synchronizeAfterEventRecordFailure(stream);
    }
};

namespace {
int synchronize_calls = 0;
hipStream_t synchronized_stream = nullptr;

hipError_t fakeStreamSynchronize(hipStream_t stream) {
    ++synchronize_calls;
    synchronized_stream = stream;
    return hipSuccess;
}
}  // namespace

TEST(HipTransportFailureTest, EventRecordFailureDrainUsesOwningStream) {
    synchronize_calls = 0;
    synchronized_stream = nullptr;
    HipTransportTestPeer::setStreamSynchronize(fakeStreamSynchronize);
    auto stream = reinterpret_cast<hipStream_t>(0x1234);
    EXPECT_EQ(HipTransportTestPeer::drain(stream), hipSuccess);
    EXPECT_EQ(synchronize_calls, 1);
    EXPECT_EQ(synchronized_stream, stream);
    HipTransportTestPeer::setStreamSynchronize(hipStreamSynchronize);
}

}  // namespace mooncake
