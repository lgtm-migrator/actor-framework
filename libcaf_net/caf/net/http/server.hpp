// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/byte_span.hpp"
#include "caf/detail/append_hex.hpp"
#include "caf/error.hpp"
#include "caf/logger.hpp"
#include "caf/net/connection_acceptor.hpp"
#include "caf/net/fwd.hpp"
#include "caf/net/http/context.hpp"
#include "caf/net/http/header.hpp"
#include "caf/net/http/header_fields_map.hpp"
#include "caf/net/http/lower_layer.hpp"
#include "caf/net/http/status.hpp"
#include "caf/net/http/upper_layer.hpp"
#include "caf/net/http/v1.hpp"
#include "caf/net/message_flow_bridge.hpp"
#include "caf/net/multiplexer.hpp"
#include "caf/net/receive_policy.hpp"
#include "caf/net/socket_manager.hpp"
#include "caf/net/stream_oriented.hpp"
#include "caf/pec.hpp"
#include "caf/settings.hpp"
#include "caf/unordered_flat_map.hpp"

#include <algorithm>

namespace caf::net::http {

/// Implements the server part for the HTTP Protocol as defined in RFC 7231.
class server : public stream_oriented::upper_layer, public http::lower_layer {
public:
  // -- member types -----------------------------------------------------------

  using header_fields_type = header_fields_map;

  using status_code_type = status;

  using context_type = context;

  using header_type = header;

  enum class mode {
    read_header,
    read_payload,
    read_chunks,
  };

  using upper_layer_ptr = std::unique_ptr<http::upper_layer>;

  // -- constants --------------------------------------------------------------

  /// Default maximum size for incoming HTTP requests: 64KiB.
  static constexpr uint32_t default_max_request_size = 65'536;

  // -- constructors, destructors, and assignment operators --------------------

  explicit server(upper_layer_ptr up) : up_(std::move(up)) {
    // nop
  }

  // -- factories --------------------------------------------------------------

  static std::unique_ptr<server> make(upper_layer_ptr up);

  // -- properties -------------------------------------------------------------

  auto& upper_layer() noexcept {
    return *up_;
  }

  const auto& upper_layer() const noexcept {
    return *up_;
  }

  // -- http::lower_layer implementation ---------------------------------------

  bool can_send_more() const noexcept override;

  bool is_reading() const noexcept override;

  void close() override;

  void request_messages() override;

  void suspend_reading() override;

  bool send_header(context, status code,
                   const header_fields_map& fields) override;

  bool send_payload(context, const_byte_span bytes) override;

  bool send_chunk(context, const_byte_span bytes) override;

  bool send_end_of_chunks() override;

  void fin(context) override;

  // -- stream_oriented::upper_layer implementation ----------------------------

  error init(socket_manager* owner, stream_oriented::lower_layer* down,
             const settings& config) override;

  void abort(const error& reason) override;

  bool prepare_send() override;

  bool done_sending() override;

  ptrdiff_t consume(byte_span input, byte_span) override;

private:
  // -- utility functions ------------------------------------------------------

  void write_response(status code, std::string_view content);

  bool invoke_upper_layer(const_byte_span payload);

  bool handle_header(std::string_view http);

  stream_oriented::lower_layer* down_;

  upper_layer_ptr up_;

  /// Buffer for re-using memory.
  header hdr_;

  /// Stores whether we are currently waiting for the payload.
  mode mode_ = mode::read_header;

  /// Stores the expected payload size when in read_payload mode.
  size_t payload_len_ = 0;

  /// Maximum size for incoming HTTP requests.
  uint32_t max_request_size_ = default_max_request_size;
};

} // namespace caf::net::http
