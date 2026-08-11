#include "transfer_engine_impl.h"

#include <gtest/gtest.h>

#include <string>
#include <memory>
#include <vector>

#include "multi_transport.h"
#include "transfer_metadata.h"

namespace mooncake {

class TransferEngineImplTestPeer {
   public:
    static void configure(TransferEngineImpl& engine,
                          std::shared_ptr<TransferMetadata> metadata,
                          std::shared_ptr<MultiTransport> multi) {
        engine.metadata_ = std::move(metadata);
        engine.multi_transports_ = std::move(multi);
        engine.local_server_name_ = "node-a:1000";
    }
    static size_t regionCount(const TransferEngineImpl& engine) {
        return engine.local_memory_regions_.size();
    }
};

class MultiTransportTestPeer {
   public:
    static void addTransport(MultiTransport& multi, const std::string& name,
                             std::shared_ptr<Transport> transport) {
        multi.transport_map_[name] = std::move(transport);
    }
};

class RegistrationFakeTransport final : public Transport {
   public:
    RegistrationFakeTransport(std::string name, std::vector<std::string>* log,
                              bool fail_single, bool fail_batch)
        : name_(std::move(name)),
          log_(log),
          fail_single_(fail_single),
          fail_batch_(fail_batch) {}

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::NotImplemented("unused");
    }
    Status getTransferStatus(BatchID, size_t, TransferStatus&) override {
        return Status::NotImplemented("unused");
    }
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        log_->push_back("register:" + name_);
        return fail_single_ ? -77 : 0;
    }
    int unregisterLocalMemory(void*, bool) override {
        log_->push_back("unregister:" + name_);
        return 0;
    }
    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        log_->push_back("batch-register:" + name_);
        return fail_batch_ ? -88 : 0;
    }
    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        log_->push_back("batch-unregister:" + name_);
        return 0;
    }
    const char* getName() const override { return name_.c_str(); }

   private:
    std::string name_;
    std::vector<std::string>* log_;
    bool fail_single_;
    bool fail_batch_;
};

TEST(TransferEngineImplAlignmentTest, RollsBackEarlierTransportOnRegistrationFailure) {
    TransferEngineImpl engine;
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::string local_name = "node-a:1000";
    auto multi = std::make_shared<MultiTransport>(metadata, local_name);
    std::vector<std::string> log;
    MultiTransportTestPeer::addTransport(
        *multi, "a", std::make_shared<RegistrationFakeTransport>(
                         "a", &log, false, false));
    MultiTransportTestPeer::addTransport(
        *multi, "b", std::make_shared<RegistrationFakeTransport>(
                         "b", &log, true, false));
    TransferEngineImplTestPeer::configure(engine, metadata, multi);

    EXPECT_EQ(engine.registerLocalMemory(reinterpret_cast<void*>(0x1000), 0x100,
                                         "cpu:0", true, true),
              -77);
    EXPECT_EQ(log, std::vector<std::string>(
                       {"register:a", "register:b", "unregister:a"}));
    EXPECT_EQ(TransferEngineImplTestPeer::regionCount(engine), 0U);
}

TEST(TransferEngineImplAlignmentTest, RollsBackEarlierTransportOnBatchFailure) {
    TransferEngineImpl engine;
    auto metadata = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
    std::string local_name = "node-a:1000";
    auto multi = std::make_shared<MultiTransport>(metadata, local_name);
    std::vector<std::string> log;
    MultiTransportTestPeer::addTransport(
        *multi, "a", std::make_shared<RegistrationFakeTransport>(
                         "a", &log, false, false));
    MultiTransportTestPeer::addTransport(
        *multi, "b", std::make_shared<RegistrationFakeTransport>(
                         "b", &log, false, true));
    TransferEngineImplTestPeer::configure(engine, metadata, multi);

    std::vector<Transport::BufferEntry> buffers = {
        {reinterpret_cast<void*>(0x2000), 0x100}};
    EXPECT_EQ(engine.registerLocalMemoryBatch(buffers, "cpu:0"), -88);
    EXPECT_EQ(log, std::vector<std::string>({"batch-register:a",
                                             "batch-register:b",
                                             "batch-unregister:a"}));
    EXPECT_EQ(TransferEngineImplTestPeer::regionCount(engine), 0U);
}

}  // namespace mooncake
