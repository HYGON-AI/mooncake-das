// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <async_simple/coro/SyncAwait.h>
#include <gtest/gtest.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include <string>
#include <vector>

#ifdef YLT_ENABLE_SSL
// Supply the SSL implementation for Asio's separate compilation mode.
#include <asio/ssl/impl/src.hpp>
#endif

namespace mooncake {
namespace {

using async_simple::coro::syncAwait;
using coro_rpc::coro_rpc_client;
using coro_rpc::coro_rpc_server;

TEST(RpcTcpNoDelayTest, ConfiguredPolicySurvivesConnectAndReconnect) {
    coro_rpc::g_action = {};
    for (bool enabled : {false, true}) {
        for (bool bound : {false, true}) {
            SCOPED_TRACE(::testing::Message()
                         << "enabled=" << enabled << ", bound=" << bound);
            coro_rpc_server server(1, 0, "127.0.0.1");
            ASSERT_FALSE(server.async_start().hasResult());
            coro_rpc_client client;
            coro_rpc_client::config config;
            config.socket_config =
                coro_rpc_client::tcp_config{.enable_tcp_no_delay = enabled};
            if (bound) config.local_ip = "127.0.0.1";
            ASSERT_TRUE(client.init_config(config));
            for (int attempt = 0; attempt < 2; ++attempt) {
                SCOPED_TRACE(attempt);
                ASSERT_FALSE(syncAwait(
                    client.connect(attempt == 0 ? "127.0.0.1" : "localhost",
                                   std::to_string(server.port()))));
                EXPECT_EQ(client.tcp_no_delay(), enabled);
            }
        }
    }
}

TEST(RpcTcpNoDelayTest, ConfiguredPolicySurvivesEndpointFailover) {
    coro_rpc::g_action = {};
    for (bool enabled : {false, true}) {
        SCOPED_TRACE(enabled);
        coro_rpc_server server(1, 0, "127.0.0.1");
        ASSERT_FALSE(server.async_start().hasResult());
        coro_rpc_client client;
        coro_rpc_client::config config;
        config.socket_config =
            coro_rpc_client::tcp_config{.enable_tcp_no_delay = enabled};
        ASSERT_TRUE(client.init_config(config));
        std::vector<asio::ip::tcp::endpoint> endpoints{
            {asio::ip::address_v4::loopback(), 0},
            {asio::ip::address_v4::loopback(), server.port()}};
        ASSERT_FALSE(syncAwait(client.connect(
            "127.0.0.1", std::to_string(server.port()), &endpoints)));
        EXPECT_EQ(client.tcp_no_delay(), enabled);
    }
}

TEST(RpcTcpNoDelayTest, ReturnsConnectionFailure) {
    coro_rpc::g_action = {};
    coro_rpc_client client;
    EXPECT_TRUE(syncAwait(client.connect("127.0.0.1", "0")));
}

#ifdef YLT_ENABLE_SSL
TEST(RpcTcpNoDelayTest, SslPolicySurvivesConnectAndReconnect) {
    coro_rpc::g_action = {};
    for (bool enabled : {false, true}) {
        SCOPED_TRACE(enabled);
        coro_rpc_server server(1, 0, "127.0.0.1");
        server.init_ssl(coro_rpc::ssl_configure{YLT_TEST_CERT_DIR, "server.crt",
                                                "server.key"});
        ASSERT_FALSE(server.async_start().hasResult());
        coro_rpc_client client;
        coro_rpc_client::config config;
        coro_rpc_client::tcp_with_ssl_config ssl;
        ssl.ssl_cert_path =
            std::filesystem::path(YLT_TEST_CERT_DIR) / "server.crt";
        ssl.ssl_domain = "localhost";
        ssl.enable_tcp_no_delay = enabled;
        config.socket_config = ssl;
        ASSERT_TRUE(client.init_config(config));
        for (int attempt = 0; attempt < 2; ++attempt) {
            SCOPED_TRACE(attempt);
            ASSERT_FALSE(syncAwait(
                client.connect("127.0.0.1", std::to_string(server.port()))));
            EXPECT_EQ(client.tcp_no_delay(), enabled);
        }
    }
}
#endif

}  // namespace
}  // namespace mooncake
