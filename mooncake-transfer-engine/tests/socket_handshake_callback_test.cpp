#include "transfer_metadata_plugin.h"

#include <gtest/gtest.h>

#include <atomic>
#include <functional>
#include <thread>

namespace mooncake {

TEST(SocketHandshakeCallbackTest, DispatchIsReentrantAndRaceFreeWithRegistration) {
    auto plugin = HandShakePlugin::Create(P2PHANDSHAKE);
    ASSERT_NE(plugin, nullptr);
    int sockfd = -1;
    uint16_t port = findAvailableTcpPort(sockfd);
    ASSERT_NE(port, 0);

    std::atomic<int> callback_calls{0};
    HandShakePlugin::OnReceiveCallBack reentrant_callback;
    reentrant_callback = [&](const Json::Value&, Json::Value& local) {
        callback_calls.fetch_add(1, std::memory_order_relaxed);
        plugin->registerOnConnectionCallBack(reentrant_callback);
        local["ok"] = true;
        return 0;
    };
    plugin->registerOnConnectionCallBack(reentrant_callback);
    ASSERT_EQ(plugin->startDaemon(port, sockfd), 0);

    Json::Value local, peer;
    ASSERT_EQ(plugin->send("127.0.0.1", port, local, peer), 0);
    EXPECT_TRUE(peer["ok"].asBool());

    std::thread registrar([&] {
        for (int i = 0; i < 100; ++i) {
            plugin->registerOnConnectionCallBack(reentrant_callback);
        }
    });
    for (int i = 0; i < 20; ++i) {
        Json::Value response;
        EXPECT_EQ(plugin->send("127.0.0.1", port, local, response), 0);
    }
    registrar.join();
    EXPECT_GE(callback_calls.load(std::memory_order_relaxed), 21);
    // Stop and join the listener while every callback capture is still alive.
    plugin.reset();
}

}  // namespace mooncake
