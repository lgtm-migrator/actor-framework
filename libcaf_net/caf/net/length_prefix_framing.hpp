// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/byte_span.hpp"
#include "caf/detail/net_export.hpp"
#include "caf/net/binary/lower_layer.hpp"
#include "caf/net/binary/upper_layer.hpp"
#include "caf/net/stream_oriented.hpp"

#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>

namespace caf::net {

/// Length-prefixed message framing for discretizing a Byte stream into messages
/// of varying size. The framing uses 4 Bytes for the length prefix, but
/// messages (including the 4 Bytes for the length prefix) are limited to a
/// maximum size of INT32_MAX. This limitation comes from the POSIX API (recv)
/// on 32-bit platforms.
class CAF_NET_EXPORT length_prefix_framing
  : public stream_oriented::upper_layer,
    public binary::lower_layer {
public:
  // -- member types -----------------------------------------------------------

  using upper_layer_ptr = std::unique_ptr<binary::upper_layer>;

  // -- constants --------------------------------------------------------------

  static constexpr size_t hdr_size = sizeof(uint32_t);

  static constexpr size_t max_message_length = INT32_MAX - sizeof(uint32_t);

  // -- constructors, destructors, and assignment operators --------------------

  explicit length_prefix_framing(upper_layer_ptr up);

  // -- factories --------------------------------------------------------------

  static std::unique_ptr<length_prefix_framing> make(upper_layer_ptr up);

  // -- high-level factory functions -------------------------------------------

  // disposable accept(actor_system& sys, Socket fd,
  //                   acceptor_resource_t<Ts...> out, OnRequest on_request,
  //                   size_t limit = 0);

  // -- implementation of stream_oriented::upper_layer -------------------------

  error init(stream_oriented::lower_layer* down,
             const settings& config) override;

  void abort(const error& reason) override;

  ptrdiff_t consume(byte_span buffer, byte_span delta) override;

  void prepare_send() override;

  bool done_sending() override;

  // -- implementation of binary::lower_layer ----------------------------------

  bool can_send_more() const noexcept override;

  void request_messages() override;

  void suspend_reading() override;

  bool is_reading() const noexcept override;

  void write_later() override;

  void begin_message() override;

  byte_buffer& message_buffer() override;

  bool end_message() override;

  void shutdown() override;

  // -- utility functions ------------------------------------------------------

  static std::pair<size_t, byte_span> split(byte_span buffer) noexcept;

private:
  // -- member variables -------------------------------------------------------

  stream_oriented::lower_layer* down_;
  upper_layer_ptr up_;
  size_t message_offset_ = 0;
};

} // namespace caf::net
