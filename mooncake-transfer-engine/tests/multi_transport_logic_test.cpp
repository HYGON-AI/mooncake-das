#include "multi_transport.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "transfer_metadata.h"

namespace mooncake {

class MultiTransportTestPeer {
   public:
    static void addTransport(MultiTransport& multi, const std::string& name,
                             std::shared_ptr<Transport> transport) {
        multi.transport_map_[name] = std::move(transport);
    }

    static Status select(MultiTransport& multi,
                         const MultiTransport::TransferRequest& request,
                         Transport*& transport) {
        return multi.selectTransport(request, transport);
    }

    static Status selectPreferred(MultiTransport& multi,
                                  const MultiTransport::TransferRequest& request,
                                  Transport*& transport,
                                  std::string protocol) {
        return multi.mp_selectTransport(request, transport, protocol);
    }

    static Status selectPreferredRef(
        MultiTransport& multi,
        const MultiTransport::TransferRequest& request, Transport*& transport,
        std::string& protocol) {
        return multi.mp_selectTransport(request, transport, protocol);
    }

    static size_t taskCount(MultiTransport::BatchID batch_id) {
        return reinterpret_cast<Transport::BatchDesc*>(batch_id)
            ->task_list.size();
    }

    static Transport* installInstance(MultiTransport& multi,
                                      const std::string& name,
                                      std::unique_ptr<Transport> transport) {
        return multi.installTransportInstance(name, nullptr,
                                              std::move(transport));
    }

    static void failTask(MultiTransport::BatchID batch_id, size_t task_id) {
        auto& task = reinterpret_cast<Transport::BatchDesc*>(batch_id)
                         ->task_list[task_id];
        task.failed_slice_count = task.slice_count;
        task.is_finished = true;
    }
};

class FakeTransport final : public Transport {
   public:
    FakeTransport(std::string name, bool fail_submit, bool fail_install = false,
                  int* destructor_calls = nullptr,
                  bool create_slice_before_failure = false)
        : name_(std::move(name)),
          fail_submit_(fail_submit),
          fail_install_(fail_install),
          destructor_calls_(destructor_calls),
          create_slice_before_failure_(create_slice_before_failure) {}
    ~FakeTransport() override {
        if (destructor_calls_) ++*destructor_calls_;
    }

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::NotImplemented("unused");
    }
    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        ++submit_task_calls;
        submitted_task_count += tasks.size();
        if (fail_submit_) {
            if (create_slice_before_failure_ && !tasks.empty()) {
                tasks.front()->slice_count = 1;
            }
            return Status::InvalidArgument("injected failure");
        }
        for (auto* task : tasks) {
            task->slice_count = 1;
            task->success_slice_count = 1;
            task->is_finished = true;
        }
        return Status::OK();
    }
    Status getTransferStatus(BatchID, size_t, TransferStatus&) override {
        return Status::NotImplemented("unused");
    }
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return 0;
    }
    int unregisterLocalMemory(void*, bool) override { return 0; }
    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        return 0;
    }
    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        return 0;
    }
    const char* getName() const override { return name_.c_str(); }

    size_t submit_task_calls = 0;
    size_t submitted_task_count = 0;

   private:
    std::string name_;
    bool fail_submit_;
    bool fail_install_;
    int* destructor_calls_;
    bool create_slice_before_failure_;

   protected:
    int install(std::string& local_server_name,
                std::shared_ptr<TransferMetadata> metadata,
                std::shared_ptr<Topology> topology) override {
        if (fail_install_) return -1;
        return Transport::install(local_server_name, std::move(metadata),
                                  std::move(topology));
    }
};

static TransferMetadata::BufferDesc makeBuffer(uint64_t addr,
                                                const char* protocol,
                                                bool shareable = false) {
    TransferMetadata::BufferDesc buffer;
    buffer.addr = addr;
    buffer.length = 0x100;
    buffer.protocol = protocol;
    if (shareable) buffer.shm_name = "ipc-handle";
    return buffer;
}

TEST(MultiTransportInstallTest, FailedInstallDestroysCandidateAndKeepsMapClean) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::string local_name = "node-a:1000";
    MultiTransport multi(metadata, local_name);
    int destructor_calls = 0;
    auto candidate = std::make_unique<FakeTransport>(
        "fake", false, true, &destructor_calls);
    EXPECT_EQ(MultiTransportTestPeer::installInstance(
                  multi, "fake", std::move(candidate)),
              nullptr);
    EXPECT_EQ(destructor_calls, 1);
    EXPECT_EQ(multi.getTransport("fake"), nullptr);
}

TEST(MultiTransportSelectionTest, RoutesSameHostHipAndRemoteRdma) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::string local_name = "node-a:1000";
    MultiTransport multi(metadata, local_name);
    MultiTransportTestPeer::addTransport(
        multi, "hip", std::make_shared<FakeTransport>("hip", false));
    MultiTransportTestPeer::addTransport(
        multi, "rdma", std::make_shared<FakeTransport>("rdma", false));
    MultiTransportTestPeer::addTransport(
        multi, "tcp", std::make_shared<FakeTransport>("tcp", false));

    auto same_host = std::make_shared<TransferMetadata::SegmentDesc>();
    same_host->name = "node-a:2000";
    same_host->protocol = "rdma,tcp,hip";
    same_host->buffers = {makeBuffer(0x1000, "rdma"),
                          makeBuffer(0x1000, "tcp"),
                          makeBuffer(0x1000, "hip", true),
                          makeBuffer(0x2000, "rdma"),
                          makeBuffer(0x2000, "tcp")};
    metadata->addLocalSegment(7, same_host->name, std::move(same_host));

    auto remote = std::make_shared<TransferMetadata::SegmentDesc>();
    remote->name = "node-b:2000";
    remote->protocol = "rdma,tcp,hip";
    remote->buffers = {makeBuffer(0x1000, "rdma"),
                       makeBuffer(0x1000, "tcp"),
                       makeBuffer(0x1000, "hip", true)};
    metadata->addLocalSegment(8, remote->name, std::move(remote));

    MultiTransport::TransferRequest request{};
    request.target_offset = 0x1010;
    request.length = 16;
    Transport* selected = nullptr;
    request.target_id = 7;
    ASSERT_TRUE(MultiTransportTestPeer::select(multi, request, selected).ok());
    EXPECT_STREQ(selected->getName(), "hip");
    request.target_offset = 0x2000;
    ASSERT_TRUE(MultiTransportTestPeer::select(multi, request, selected).ok());
    EXPECT_STREQ(selected->getName(), "rdma");
    request.target_id = 8;
    request.target_offset = 0x1010;
    ASSERT_TRUE(MultiTransportTestPeer::select(multi, request, selected).ok());
    EXPECT_STREQ(selected->getName(), "rdma");

    auto hip_status = MultiTransportTestPeer::selectPreferred(
        multi, request, selected, "hip");
    EXPECT_TRUE(hip_status.ok());
    EXPECT_STREQ(selected->getName(), "rdma");

    std::string tcp_only_name = "node-a:3000";
    MultiTransport tcp_only(metadata, tcp_only_name);
    MultiTransportTestPeer::addTransport(
        tcp_only, "tcp", std::make_shared<FakeTransport>("tcp", false));
    EXPECT_FALSE(
        MultiTransportTestPeer::select(tcp_only, request, selected).ok());
}

TEST(MultiTransportSelectionTest,
     ChoosesByFirstAddressThenChecksInstalledTransport) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::string local_name = "node-a:1000";
    MultiTransport multi(metadata, local_name);
    MultiTransportTestPeer::addTransport(
        multi, "rdma", std::make_shared<FakeTransport>("rdma", false));
    MultiTransportTestPeer::addTransport(
        multi, "tcp", std::make_shared<FakeTransport>("tcp", false));

    auto local = std::make_shared<TransferMetadata::SegmentDesc>();
    local->name = "node-a:2000";
    local->protocol = "hip,rdma,tcp";
    auto partial_hip = makeBuffer(0x1000, "hip", true);
    partial_hip.length = 8;
    local->buffers = {partial_hip, makeBuffer(0x1000, "rdma"),
                      makeBuffer(0x1000, "tcp")};
    metadata->addLocalSegment(11, local->name, std::move(local));

    MultiTransport::TransferRequest request{};
    request.target_id = 11;
    request.target_offset = 0x1000;
    request.length = 16;
    Transport* selected = nullptr;
    // Upstream selection checks only the first target address. HIP therefore
    // wins on priority even though this request extends past the HIP buffer,
    // and the call fails only because HIP is not installed.
    EXPECT_FALSE(MultiTransportTestPeer::select(multi, request, selected).ok());
    MultiTransportTestPeer::addTransport(
        multi, "hip", std::make_shared<FakeTransport>("hip", false));
    ASSERT_TRUE(MultiTransportTestPeer::select(multi, request, selected).ok());
    EXPECT_STREQ(selected->getName(), "hip");

    std::string preferred = "hip";
    auto remote = std::make_shared<TransferMetadata::SegmentDesc>();
    remote->name = "node-b:2000";
    remote->protocol = "hip,rdma,tcp";
    remote->buffers = {makeBuffer(0x2000, "hip", true),
                       makeBuffer(0x2000, "rdma"),
                       makeBuffer(0x2000, "tcp")};
    metadata->addLocalSegment(12, remote->name, std::move(remote));
    request.target_id = 12;
    request.target_offset = 0x2000;
    ASSERT_TRUE(MultiTransportTestPeer::selectPreferredRef(
                    multi, request, selected, preferred)
                    .ok());
    EXPECT_EQ(preferred, "rdma");
    EXPECT_STREQ(selected->getName(), "rdma");

    std::string tcp_local_name = "node-a:3000";
    MultiTransport tcp_only(metadata, tcp_local_name);
    MultiTransportTestPeer::addTransport(
        tcp_only, "tcp", std::make_shared<FakeTransport>("tcp", false));
    preferred = "hip";
    EXPECT_FALSE(MultiTransportTestPeer::selectPreferredRef(
                     tcp_only, request, selected, preferred)
                     .ok());
    EXPECT_EQ(preferred, "rdma");
}

TEST(MultiTransportSubmitTest, ExplicitPreferenceMutatesAcrossEntries) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::string local_name = "node-a:1000";
    MultiTransport multi(metadata, local_name);
    auto hip = std::make_shared<FakeTransport>("hip", false);
    auto rdma = std::make_shared<FakeTransport>("rdma", false);
    MultiTransportTestPeer::addTransport(multi, "hip", hip);
    MultiTransportTestPeer::addTransport(multi, "rdma", rdma);

    auto remote = std::make_shared<TransferMetadata::SegmentDesc>();
    remote->name = "node-b:2000";
    remote->protocol = "hip,rdma";
    remote->buffers = {makeBuffer(0x1000, "hip", true),
                       makeBuffer(0x1000, "rdma")};
    metadata->addLocalSegment(13, remote->name, std::move(remote));
    auto local = std::make_shared<TransferMetadata::SegmentDesc>();
    local->name = "node-a:2000";
    local->protocol = "hip,rdma";
    local->buffers = {makeBuffer(0x1000, "hip", true),
                      makeBuffer(0x1000, "rdma")};
    metadata->addLocalSegment(14, local->name, std::move(local));

    auto batch = multi.allocateBatchID(2);
    std::vector<MultiTransport::TransferRequest> requests(2);
    for (auto& request : requests) {
        request.target_offset = 0x1000;
        request.length = 16;
    }
    requests[0].target_id = 13;
    requests[1].target_id = 14;
    std::string preferred = "hip";
    ASSERT_TRUE(multi.mp_submitTransfer(batch, requests, preferred).ok());
    EXPECT_EQ(preferred, "rdma");
    EXPECT_EQ(rdma->submitted_task_count, 2U);
    EXPECT_EQ(hip->submitted_task_count, 0U);
    EXPECT_TRUE(multi.freeBatchID(batch).ok());

    auto remote_batch = multi.allocateBatchID(1);
    requests.resize(1);
    preferred = "hip";
    ASSERT_TRUE(
        multi.mp_submitTransfer(remote_batch, requests, preferred).ok());
    EXPECT_EQ(preferred, "rdma");
    EXPECT_TRUE(multi.freeBatchID(remote_batch).ok());
}

TEST(MultiTransportSubmitTest, PreselectsAndTerminatesUnsubmittedGroups) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::string local_name = "node-a:1000";
    MultiTransport multi(metadata, local_name);
    MultiTransportTestPeer::addTransport(
        multi, "rdma", std::make_shared<FakeTransport>("rdma", false));
    MultiTransportTestPeer::addTransport(
        multi, "tcp", std::make_shared<FakeTransport>("tcp", true));

    auto remote = std::make_shared<TransferMetadata::SegmentDesc>();
    remote->name = "node-b:2000";
    remote->protocol = "rdma,tcp";
    remote->buffers = {makeBuffer(0x1000, "rdma"),
                       makeBuffer(0x2000, "tcp")};
    metadata->addLocalSegment(9, remote->name, std::move(remote));

    auto batch = multi.allocateBatchID(2);
    std::vector<MultiTransport::TransferRequest> requests(2);
    requests[0].target_id = 9;
    requests[0].target_offset = 0x1000;
    requests[0].length = 16;
    requests[1].target_id = 9;
    requests[1].target_offset = 0x2000;
    requests[1].length = 16;
    EXPECT_FALSE(multi.submitTransfer(batch, requests).ok());

    MultiTransport::TransferStatus status{};
    ASSERT_TRUE(multi.getBatchTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::FAILED);
    EXPECT_TRUE(multi.freeBatchID(batch).ok());

    auto untouched_batch = multi.allocateBatchID(1);
    requests.resize(1);
    requests[0].target_offset = 0x3000;
    EXPECT_FALSE(multi.submitTransfer(untouched_batch, requests).ok());
    EXPECT_EQ(MultiTransportTestPeer::taskCount(untouched_batch), 0U);
    EXPECT_TRUE(multi.freeBatchID(untouched_batch).ok());
}

TEST(MultiTransportSubmitTest, FailedGroupWithCreatedSliceMustDrain) {
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::string local_name = "node-a:1000";
    MultiTransport multi(metadata, local_name);
    MultiTransportTestPeer::addTransport(
        multi, "tcp",
        std::make_shared<FakeTransport>("tcp", true, false, nullptr, true));
    auto remote = std::make_shared<TransferMetadata::SegmentDesc>();
    remote->name = "node-b:2000";
    remote->protocol = "tcp";
    remote->buffers = {makeBuffer(0x2000, "tcp")};
    metadata->addLocalSegment(10, remote->name, std::move(remote));

    auto batch = multi.allocateBatchID(1);
    MultiTransport::TransferRequest request{};
    request.target_id = 10;
    request.target_offset = 0x2000;
    request.length = 16;
    EXPECT_FALSE(multi.submitTransfer(batch, {request}).ok());
    MultiTransport::TransferStatus status{};
    ASSERT_TRUE(multi.getBatchTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::WAITING);
    EXPECT_TRUE(multi.freeBatchID(batch).IsBatchBusy());
    MultiTransportTestPeer::failTask(batch, 0);
    ASSERT_TRUE(multi.getBatchTransferStatus(batch, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::FAILED);
    EXPECT_TRUE(multi.freeBatchID(batch).ok());
}

}  // namespace mooncake
