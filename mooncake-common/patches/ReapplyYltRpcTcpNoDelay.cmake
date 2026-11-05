# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

# Backport https://github.com/alibaba/yalantinglibs/pull/1230 to the pinned YLT.
# Reapply the RPC configuration after connect recreates the underlying socket.
set(_ylt_rpc_client
    "${yalantinglibs_SOURCE_DIR}/include/ylt/coro_rpc/impl/coro_rpc_client.hpp")
file(READ "${_ylt_rpc_client}" _ylt_rpc_contents)
set(_ylt_notice_before [=[#pragma once
]=])
set(_ylt_notice_after
    [=[// Copyright (c) 2026 Hygon Information Technology Co., Ltd.
// SPDX-License-Identifier: Apache-2.0
// Modified by Hygon Information Technology Co., Ltd., 2026.

#pragma once
]=])
set(_ylt_connect_before [=[    ELOG_INFO << "connect successful, remote addr: "
]=])
set(_ylt_connect_after
    [=[    // Endpoint-list connect may reopen the socket, losing pre-connect options.
    // Reapply the RPC client's TCP policy to the actual connected socket.
    std::visit(
        [&](const auto &socket_config) {
          if constexpr (requires { socket_config.enable_tcp_no_delay; }) {
            control_->socket_wrapper_.socket()->set_option(
                asio::ip::tcp::no_delay(socket_config.enable_tcp_no_delay), ec);
          }
        },
        config_.socket_config);
    if (ec) {
      ELOG_WARN << "client_id " << config_.client_id
                << " failed to configure TCP_NODELAY: " << ec.message();
      close_socket_async(control_);
      co_return errc::not_connected;
    }
    ELOG_INFO << "connect successful, remote addr: "
]=])
set(_ylt_accessor_before
    [=[#ifdef UNIT_TEST_INJECT
 public:
  coro_rpc::err_code sync_connect(const std::string &host,
]=])
set(_ylt_accessor_after
    [=[#ifdef UNIT_TEST_INJECT
 public:
  bool tcp_no_delay() {
    asio::ip::tcp::no_delay option;
    control_->socket_wrapper_.socket()->get_option(option);
    return option.value();
  }

  coro_rpc::err_code sync_connect(const std::string &host,
]=])
set(_ylt_rpc_updated FALSE)
foreach(_ylt_part IN ITEMS notice connect accessor)
  string(FIND "${_ylt_rpc_contents}" "${_ylt_${_ylt_part}_after}" _ylt_patched)
  if(_ylt_patched EQUAL -1)
    string(FIND "${_ylt_rpc_contents}" "${_ylt_${_ylt_part}_before}"
                _ylt_position)
    if(_ylt_position EQUAL -1)
      message(
        FATAL_ERROR
          "Pinned YLT RPC client changed; review TCP_NODELAY ${_ylt_part} patch"
      )
    endif()
    string(REPLACE "${_ylt_${_ylt_part}_before}" "${_ylt_${_ylt_part}_after}"
                   _ylt_rpc_contents "${_ylt_rpc_contents}")
    set(_ylt_rpc_updated TRUE)
  endif()
endforeach()
if(_ylt_rpc_updated)
  file(WRITE "${_ylt_rpc_client}" "${_ylt_rpc_contents}")
endif()
