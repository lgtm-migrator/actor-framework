// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/master/LICENSE.

#define CAF_SUITE io.network.ip_endpoint

#include "caf/io/network/ip_endpoint.hpp"

#include "io-test.hpp"

#include <vector>

#include "caf/actor_system.hpp"
#include "caf/actor_system_config.hpp"
#include "caf/binary_deserializer.hpp"
#include "caf/binary_serializer.hpp"
#include "caf/io/middleman.hpp"
#include "caf/io/network/interfaces.hpp"

using namespace caf;
using namespace caf::io;

namespace {

class config : public actor_system_config {
public:
  config() {
    // this will call WSAStartup for network initialization on Windows
    load<io::middleman>();
  }
};

struct fixture : test_coordinator_fixture<> {
  template <class... Ts>
  auto serialize(Ts&... xs) {
    byte_buffer buf;
    binary_serializer sink{sys, buf};
    if (!(sink.apply(xs) && ...))
      CAF_FAIL("serialization failed: " << sink.get_error());
    return buf;
  }

  template <class Buffer, class... Ts>
  void deserialize(const Buffer& buf, Ts&... xs) {
    binary_deserializer source{sys, buf};
    if (!(source.apply(xs) && ...))
      CAF_FAIL("serialization failed: " << source.get_error());
  }
};

} // namespace

BEGIN_FIXTURE_SCOPE(fixture)

CAF_TEST_DISABLED(ip_endpoint) {
  // create an empty endpoint
  network::ip_endpoint ep;
  ep.clear();
  CHECK_EQ("", network::host(ep));
  CHECK_EQ(uint16_t{0}, network::port(ep));
  CHECK_EQ(size_t{0}, *ep.length());
  // fill it with data from a local endpoint
  network::interfaces::get_endpoint("localhost", 12345, ep);
  // save the data
  auto h = network::host(ep);
  auto p = network::port(ep);
  auto l = *ep.length();
  CHECK("localhost" == h || "127.0.0.1" == h || "::1" == h);
  CHECK_EQ(12345, p);
  CHECK(0 < l);
  // serialize the endpoint and clear it
  auto buf = serialize(ep);
  auto save = ep;
  ep.clear();
  CHECK_EQ("", network::host(ep));
  CHECK_EQ(uint16_t{0}, network::port(ep));
  CHECK_EQ(size_t{0}, *ep.length());
  // deserialize the data and check if it was load successfully
  deserialize(buf, ep);
  CHECK_EQ(h, network::host(ep));
  CHECK_EQ(uint16_t{p}, network::port(ep));
  CHECK_EQ(size_t{l}, *ep.length());
  CHECK_EQ(save, ep);
}

END_FIXTURE_SCOPE()
