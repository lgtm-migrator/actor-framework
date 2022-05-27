// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#pragma once

#include "caf/actor_system.hpp"
#include "caf/async/blocking_producer.hpp"
#include "caf/detail/atomic_ref_counted.hpp"
#include "caf/detail/net_export.hpp"
#include "caf/net/http/request.hpp"
#include "caf/net/http/server.hpp"
#include "caf/net/middleman.hpp"

#include <memory>
#include <type_traits>

namespace caf::detail {

class CAF_NET_EXPORT http_request_producer : public atomic_ref_counted,
                                             public async::producer {
public:
  using buffer_ptr = async::spsc_buffer_ptr<net::http::request>;

  http_request_producer(buffer_ptr buf) : buf_(std::move(buf)) {
    // nop
  }

  static auto make(buffer_ptr buf) {
    auto ptr = make_counted<http_request_producer>(buf);
    buf->set_producer(ptr);
    return ptr;
  }

  void on_consumer_ready() override;

  void on_consumer_cancel() override;

  void on_consumer_demand(size_t) override;

  void ref_producer() const noexcept override;

  void deref_producer() const noexcept override;

  bool push(const net::http::request& item);

private:
  buffer_ptr buf_;
};

using http_request_producer_ptr = intrusive_ptr<http_request_producer>;

class CAF_NET_EXPORT http_flow_adapter : public net::http::upper_layer {
public:
  explicit http_flow_adapter(http_request_producer_ptr ptr)
    : producer_(std::move(ptr)) {
    // nop
  }

  void prepare_send() override;

  bool done_sending() override;

  void abort(const error& reason) override;

  error init(net::socket_manager* owner, net::http::lower_layer* down,
             const settings& config) override;

  ptrdiff_t consume(const net::http::header& hdr,
                    const_byte_span payload) override;

  static auto make(http_request_producer_ptr ptr) {
    return std::make_unique<http_flow_adapter>(ptr);
  }

private:
  async::execution_context* parent_ = nullptr;
  net::http::lower_layer* down_ = nullptr;
  std::vector<disposable> pending_;
  http_request_producer_ptr producer_;
};

template <class Transport>
class http_acceptor_factory {
public:
  explicit http_acceptor_factory(http_request_producer_ptr producer)
    : producer_(std::move(producer)) {
    // nop
  }

  error init(net::socket_manager*, const settings&) {
    return none;
  }

  template <class Socket>
  net::socket_manager_ptr make(net::multiplexer* mpx, Socket fd) {
    auto app = http_flow_adapter::make(producer_);
    auto serv = net::http::server::make(std::move(app));
    auto transport = Transport::make(fd, std::move(serv));
    auto res = net::socket_manager::make(mpx, fd, std::move(transport));
    mpx->watch(res->as_disposable());
    return res;
  }

  void abort(const error&) {
    // nop
  }

private:
  http_request_producer_ptr producer_;
};

} // namespace caf::detail

namespace caf::net::http {

/// Convenience function for creating async resources for connecting the HTTP
/// server to a worker.
inline auto make_request_resource() {
  return async::make_spsc_buffer_resource<request>();
}

/// Listens for incoming HTTP requests on @p fd.
/// @param sys The host system.
/// @param fd An accept socket in listening mode. For a TCP socket, this socket
///           must already listen to a port.
/// @param out A buffer resource that connects the server to a listener that
///            processes the requests.
/// @param limit The maximum amount of connections before closing @p fd. Passing
///              0 means "no limit".
template <class Transport = stream_transport, class Socket>
disposable serve(actor_system& sys, Socket fd,
                 async::producer_resource<request> out, size_t limit = 0) {
  using factory_t = detail::http_acceptor_factory<Transport>;
  using impl_t = connection_acceptor<Socket, factory_t>;
  if (auto buf = out.try_open()) {
    auto& mpx = sys.network_manager().mpx();
    auto producer = detail::http_request_producer::make(std::move(buf));
    auto factory = factory_t{std::move(producer)};
    auto impl = impl_t::make(fd, limit, std::move(factory));
    auto ptr = socket_manager::make(&mpx, fd, std::move(impl));
    mpx.init(ptr);
    return ptr->as_disposable();
  } else {
    return disposable{};
  }
}

} // namespace caf::net::http
