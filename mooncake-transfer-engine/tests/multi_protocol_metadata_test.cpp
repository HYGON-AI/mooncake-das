#include "transfer_metadata.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "transfer_metadata_plugin.h"

namespace mooncake {

class TransferMetadataTestPeer {
   public:
    static int encode(TransferMetadata& metadata,
                      const TransferMetadata::SegmentDesc& desc,
                      Json::Value& json) {
        return metadata.encodeSegmentDesc(desc, json);
    }
    static std::shared_ptr<TransferMetadata::SegmentDesc> decode(
        TransferMetadata& metadata, Json::Value& json,
        const std::string& name) {
        return metadata.decodeSegmentDesc(json, name);
    }
    static void setStorage(TransferMetadata& metadata,
                           std::shared_ptr<MetadataStoragePlugin> storage) {
        metadata.p2p_handshake_mode_ = false;
        metadata.storage_plugin_ = std::move(storage);
    }
    static void setHandshake(TransferMetadata& metadata,
                             std::shared_ptr<HandShakePlugin> handshake) {
        metadata.handshake_plugin_ = std::move(handshake);
    }
    static std::unique_lock<std::recursive_mutex> lockLocalTransaction(
        TransferMetadata& metadata) {
        return std::unique_lock<std::recursive_mutex>(
            metadata.local_segment_transaction_mutex_);
    }
};

class FakeStorage final : public MetadataStoragePlugin {
   public:
    bool get(const std::string&, Json::Value&) override { return false; }
    bool set(const std::string&, const Json::Value& value) override {
        std::lock_guard<std::mutex> lock(mutex);
        ++set_calls;
        last_value = value;
        set_observed = true;
        cv.notify_all();
        return allow_set;
    }
    bool remove(const std::string&) override { return true; }

    bool allow_set = true;
    int set_calls = 0;
    bool set_observed = false;
    Json::Value last_value;
    std::mutex mutex;
    std::condition_variable cv;
};

class FakeHandshake final : public HandShakePlugin {
   public:
    int startDaemon(uint16_t, int) override { return 0; }
    int send(std::string, uint16_t, const Json::Value&,
             Json::Value&) override {
        return 0;
    }
    int sendNotify(std::string, uint16_t, const Json::Value&,
                   Json::Value&) override {
        return 0;
    }
    int sendProbe(std::string, uint16_t, const Json::Value&,
                  Json::Value&) override {
        return 0;
    }
    int exchangeMetadata(std::string, uint16_t, const Json::Value&,
                         Json::Value&) override {
        return 0;
    }
    void registerOnConnectionCallBack(OnReceiveCallBack callback) override {
        connection = std::move(callback);
    }
    void registerOnMetadataCallBack(OnReceiveCallBack callback) override {
        metadata = std::move(callback);
    }
    void registerOnNotifyCallBack(OnReceiveCallBack callback) override {
        notify = std::move(callback);
    }
    void registerOnProbeCallBack(OnReceiveCallBack callback) override {
        probe = std::move(callback);
    }

    OnReceiveCallBack connection;
    OnReceiveCallBack metadata;
    OnReceiveCallBack notify;
    OnReceiveCallBack probe;
};

static TransferMetadata::SegmentDesc multiProtocolSegment(
    const std::string& protocols) {
    TransferMetadata::SegmentDesc desc;
    desc.name = "segment-a";
    desc.protocol = protocols;
    desc.cxl_name = "cxl0";
    desc.cxl_base_addr = 0x8000;
    bool rdma_device_added = false;
    std::stringstream stream(protocols);
    std::string protocol;
    while (std::getline(stream, protocol, ',')) {
        if (protocol == "rdma" && !rdma_device_added) {
            TransferMetadata::DeviceDesc device;
            device.name = "rdma0";
            device.gid = "gid0";
            desc.devices.push_back(std::move(device));
            rdma_device_added = true;
        }
        if (protocol != "rdma" && protocol != "tcp" && protocol != "hip" &&
            protocol != "maca" && protocol != "cxl") {
            continue;
        }
        TransferMetadata::BufferDesc buffer;
        buffer.addr = 0x1000;
        buffer.length = 0x100;
        buffer.name = "memory0";
        buffer.protocol = protocol;
        if (buffer.protocol == "rdma") {
            buffer.rkey.push_back(1);
            buffer.lkey.push_back(2);
        }
        if (buffer.protocol == "hip" || buffer.protocol == "maca")
            buffer.shm_name = "ipc-handle";
        if (buffer.protocol == "cxl") buffer.offset = 0x20;
        desc.buffers.push_back(std::move(buffer));
    }
    return desc;
}

TEST(MultiProtocolMetadataTest, AcceptsAllNetworkAndHipPairPermutations) {
    TransferMetadata metadata(P2PHANDSHAKE);
    for (const char* protocols : {"rdma,tcp", "tcp,rdma", "rdma,hip",
                                  "hip,rdma", "tcp,hip", "hip,tcp"}) {
        auto desc = multiProtocolSegment(protocols);
        Json::Value json;
        ASSERT_EQ(TransferMetadataTestPeer::encode(metadata, desc, json), 0)
            << protocols;
        auto decoded =
            TransferMetadataTestPeer::decode(metadata, json, desc.name);
        ASSERT_NE(decoded, nullptr) << protocols;
        EXPECT_EQ(decoded->protocol, protocols);
        EXPECT_EQ(decoded->buffers.size(), 2U);
    }
}

TEST(MultiProtocolMetadataTest, PreservesSupportedCxlPairPermutations) {
    TransferMetadata metadata(P2PHANDSHAKE);
    for (const char* protocols : {"cxl,rdma", "rdma,cxl", "cxl,tcp",
                                  "tcp,cxl"}) {
        auto desc = multiProtocolSegment(protocols);
        Json::Value json;
        ASSERT_EQ(TransferMetadataTestPeer::encode(metadata, desc, json), 0)
            << protocols;
        auto decoded =
            TransferMetadataTestPeer::decode(metadata, json, desc.name);
        ASSERT_NE(decoded, nullptr) << protocols;
        EXPECT_EQ(decoded->protocol, protocols);
        EXPECT_EQ(decoded->buffers.size(), 2U);
    }
}

TEST(MultiProtocolMetadataTest, AcceptsAllTriplePermutationsAndRoundTrips) {
    TransferMetadata metadata(P2PHANDSHAKE);
    std::vector<std::string> protocols = {"rdma", "tcp", "hip"};
    std::sort(protocols.begin(), protocols.end());
    do {
        std::string joined = protocols[0] + "," + protocols[1] + "," +
                             protocols[2];
        auto desc = multiProtocolSegment(joined);
        Json::Value json;
        ASSERT_EQ(TransferMetadataTestPeer::encode(metadata, desc, json), 0)
            << joined;
        auto decoded =
            TransferMetadataTestPeer::decode(metadata, json, desc.name);
        ASSERT_NE(decoded, nullptr) << joined;
        EXPECT_EQ(decoded->protocol, joined);
        EXPECT_EQ(decoded->buffers.size(), 3U);
    } while (std::next_permutation(protocols.begin(), protocols.end()));
}

TEST(MultiProtocolMetadataTest, AcceptsUpstreamAllowlistCombinations) {
    TransferMetadata metadata(P2PHANDSHAKE);
    for (const char* protocols : {"cxl,hip", "hip,cxl", "cxl,maca",
                                  "rdma,tcp,hip,maca", "rdma,rdma"}) {
        auto desc = multiProtocolSegment(protocols);
        Json::Value json;
        ASSERT_EQ(TransferMetadataTestPeer::encode(metadata, desc, json), 0)
            << protocols;
        auto decoded =
            TransferMetadataTestPeer::decode(metadata, json, desc.name);
        ASSERT_NE(decoded, nullptr) << protocols;
        EXPECT_EQ(decoded->protocol, protocols);
    }
}

TEST(MultiProtocolMetadataTest, NormalizesEmptyTokensAndRejectsUnknown) {
    TransferMetadata metadata(P2PHANDSHAKE);
    auto desc = multiProtocolSegment("rdma,,hip,");
    Json::Value json;
    ASSERT_EQ(TransferMetadataTestPeer::encode(metadata, desc, json), 0);
    ASSERT_TRUE(json["protocol"].isArray());
    ASSERT_EQ(json["protocol"].size(), 2U);
    auto decoded = TransferMetadataTestPeer::decode(metadata, json, desc.name);
    ASSERT_NE(decoded, nullptr);
    EXPECT_EQ(decoded->protocol, "rdma,hip");

    auto unsupported = multiProtocolSegment("tcp,unknown");
    EXPECT_EQ(TransferMetadataTestPeer::encode(metadata, unsupported, json),
              ERR_INVALID_ARGUMENT);
}

TEST(MultiProtocolMetadataTest, AddrOnlyRemovalDeletesFirstMatchingTwin) {
    TransferMetadata metadata(P2PHANDSHAKE);
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>(
        multiProtocolSegment("rdma,tcp,hip"));
    metadata.addLocalSegment(LOCAL_SEGMENT_ID, desc->name, std::move(desc));
    ASSERT_EQ(metadata.removeLocalMemoryBuffer(reinterpret_cast<void*>(0x1000),
                                               false),
              0);
    auto current = metadata.getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(current, nullptr);
    ASSERT_EQ(current->buffers.size(), 2U);
    EXPECT_EQ(current->buffers[0].protocol, "tcp");
    EXPECT_EQ(current->buffers[1].protocol, "hip");
}

TEST(MultiProtocolMetadataTest, FailedPublishDoesNotMutateLocalDescriptor) {
    TransferMetadata metadata(P2PHANDSHAKE);
    auto storage = std::make_shared<FakeStorage>();
    storage->allow_set = false;
    TransferMetadataTestPeer::setStorage(metadata, storage);

    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "segment-a";
    desc->protocol = "rdma,tcp";
    metadata.addLocalSegment(LOCAL_SEGMENT_ID, desc->name, std::move(desc));

    TransferMetadata::BufferDesc buffer;
    buffer.addr = 0x1000;
    buffer.length = 0x100;
    buffer.protocol = "tcp";
    EXPECT_EQ(metadata.addLocalMemoryBuffer(buffer, true), ERR_METADATA);
    auto current = metadata.getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(current, nullptr);
    EXPECT_TRUE(current->buffers.empty());
    EXPECT_EQ(storage->set_calls, 1);
}

TEST(MultiProtocolMetadataTest,
     BatchRollbackCannotLeakThroughConcurrentPublisher) {
    TransferMetadata metadata(P2PHANDSHAKE);
    auto storage = std::make_shared<FakeStorage>();
    TransferMetadataTestPeer::setStorage(metadata, storage);
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "segment-a";
    desc->protocol = "rdma,tcp";
    metadata.addLocalSegment(LOCAL_SEGMENT_ID, desc->name, std::move(desc));

    TransferMetadata::BufferDesc uncommitted;
    uncommitted.addr = 0x1000;
    uncommitted.length = 0x100;
    uncommitted.name = "uncommitted";
    uncommitted.protocol = "tcp";
    auto committed = uncommitted;
    committed.addr = 0x2000;
    committed.name = "committed";

    std::mutex phase_mutex;
    std::condition_variable phase_cv;
    bool uncommitted_added = false;
    bool allow_rollback = false;
    bool publisher_started = false;
    std::thread batch_thread([&] {
        auto transaction =
            TransferMetadataTestPeer::lockLocalTransaction(metadata);
        EXPECT_EQ(metadata.addLocalMemoryBuffer(uncommitted, false), 0);
        {
            std::lock_guard<std::mutex> lock(phase_mutex);
            uncommitted_added = true;
        }
        phase_cv.notify_all();
        {
            std::unique_lock<std::mutex> lock(phase_mutex);
            phase_cv.wait(lock, [&] { return allow_rollback; });
        }
        EXPECT_EQ(metadata.removeLocalMemoryBuffer(
                      reinterpret_cast<void*>(uncommitted.addr), false),
                  0);
    });

    {
        std::unique_lock<std::mutex> lock(phase_mutex);
        phase_cv.wait(lock, [&] { return uncommitted_added; });
    }
    std::thread publisher_thread([&] {
        {
            std::lock_guard<std::mutex> lock(phase_mutex);
            publisher_started = true;
        }
        phase_cv.notify_all();
        EXPECT_EQ(metadata.addLocalMemoryBuffer(committed, true), 0);
    });
    {
        std::unique_lock<std::mutex> lock(phase_mutex);
        phase_cv.wait(lock, [&] { return publisher_started; });
    }
    {
        std::unique_lock<std::mutex> lock(storage->mutex);
        EXPECT_FALSE(storage->cv.wait_for(
            lock, std::chrono::milliseconds(20),
            [&] { return storage->set_observed; }));
    }
    {
        std::lock_guard<std::mutex> lock(phase_mutex);
        allow_rollback = true;
    }
    phase_cv.notify_all();
    batch_thread.join();
    publisher_thread.join();

    std::lock_guard<std::mutex> lock(storage->mutex);
    ASSERT_EQ(storage->last_value["buffers"].size(), 1U);
    EXPECT_EQ(storage->last_value["buffers"][0]["addr"].asUInt64(),
              committed.addr);
}

TEST(MultiProtocolMetadataTest, NullCallbackDoesNotReplaceRdmaHandshake) {
    TransferMetadata metadata(P2PHANDSHAKE);
    auto handshake = std::make_shared<FakeHandshake>();
    TransferMetadataTestPeer::setHandshake(metadata, handshake);
    int calls = 0;
    ASSERT_EQ(metadata.startHandshakeDaemon(
                  [&calls](const TransferMetadata::HandShakeDesc&,
                           TransferMetadata::HandShakeDesc&) {
                      ++calls;
                      return 0;
                  },
                  0, -1),
              0);
    ASSERT_TRUE(static_cast<bool>(handshake->connection));
    ASSERT_EQ(metadata.startHandshakeDaemon(nullptr, 0, -1), 0);
    ASSERT_TRUE(static_cast<bool>(handshake->connection));
    Json::Value peer, local;
    EXPECT_EQ(handshake->connection(peer, local), 0);
    EXPECT_EQ(calls, 1);
}

}  // namespace mooncake
