#ifdef IS_TEST_NET
#include <boost/test/unit_test.hpp>

#include <hive/plugins/stinger/socket_sink.hpp>

#include <fc/exception/exception.hpp>
#include <fc/filesystem.hpp>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

using hive::plugins::stinger::socket_sink;

namespace {

/// a blocking client connection to a sink's socket
class client
{
public:
  explicit client( const fc::path& path )
  {
    _fd = ::socket( AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0 );
    BOOST_REQUIRE( _fd >= 0 );
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    const std::string p = path.generic_string();
    BOOST_REQUIRE( p.size() < sizeof( addr.sun_path ) );
    std::memcpy( addr.sun_path, p.c_str(), p.size() + 1 );
    BOOST_REQUIRE_MESSAGE( ::connect( _fd, reinterpret_cast< const sockaddr* >( &addr ), sizeof( addr ) ) == 0,
      "connect: " << std::strerror( errno ) );
  }
  ~client() { close(); }

  void close()
  {
    if( _fd >= 0 )
      ::close( _fd );
    _fd = -1;
  }

  /// read until `n` bytes arrived or `timeout_ms` passed without data
  std::string read( size_t n, int timeout_ms = 2000 )
  {
    std::string out;
    while( out.size() < n )
    {
      pollfd p{ _fd, POLLIN, 0 };
      if( ::poll( &p, 1, timeout_ms ) <= 0 )
        break;
      char buf[ 65536 ];
      const ssize_t got = ::recv( _fd, buf, std::min( sizeof( buf ), n - out.size() ), 0 );
      if( got <= 0 )
        break;
      out.append( buf, size_t( got ) );
    }
    return out;
  }

  /// true when the peer closed: a read returns 0 within `timeout_ms`
  bool at_eof( int timeout_ms = 2000 )
  {
    pollfd p{ _fd, POLLIN, 0 };
    if( ::poll( &p, 1, timeout_ms ) <= 0 )
      return false;
    char buf[ 16 ];
    return ::recv( _fd, buf, sizeof( buf ), 0 ) == 0;
  }

private:
  int _fd = -1;
};

const std::string HELLO = "{\"kind\":\"hello\"}\n";

} // anonymous namespace

BOOST_AUTO_TEST_SUITE( stinger_socket )

BOOST_AUTO_TEST_CASE( creates_an_owner_only_socket_and_removes_it )
{
  fc::temp_directory dir;
  const fc::path path = dir.path() / "stinger.sock";
  {
    socket_sink sink( path );
    BOOST_CHECK_EQUAL( sink.path().generic_string(), path.generic_string() );
    struct stat st;
    BOOST_REQUIRE_EQUAL( ::stat( path.generic_string().c_str(), &st ), 0 );
    BOOST_CHECK( S_ISSOCK( st.st_mode ) );
    BOOST_CHECK_EQUAL( st.st_mode & 0777, 0600u );
    BOOST_CHECK_EQUAL( sink.connections(), 0u );
  }
  BOOST_CHECK( !fc::exists( path ) );
}

BOOST_AUTO_TEST_CASE( greets_and_serves_lines )
{
  fc::temp_directory dir;
  socket_sink sink( dir.path() / "stinger.sock" );

  // nothing waiting: a poll is a no-op
  sink.poll( HELLO );
  BOOST_CHECK_EQUAL( sink.connections(), 0u );

  client a( sink.path() );
  client b( sink.path() );
  sink.poll( HELLO );
  BOOST_CHECK_EQUAL( sink.connections(), 2u );
  BOOST_CHECK_EQUAL( sink.stats().accepted, 2u );
  BOOST_CHECK_EQUAL( a.read( HELLO.size() ), HELLO );
  BOOST_CHECK_EQUAL( b.read( HELLO.size() ), HELLO );

  sink.send( "one\n" );
  sink.send( "two\n" );
  BOOST_CHECK_EQUAL( a.read( 8 ), "one\ntwo\n" );
  BOOST_CHECK_EQUAL( b.read( 8 ), "one\ntwo\n" );
  BOOST_CHECK_EQUAL( sink.stats().lines, 4u );
  BOOST_CHECK_EQUAL( sink.stats().bytes, 16u );

  // a line sent with nobody connected is nobody's loss
  a.close();
  b.close();
  sink.poll( HELLO );
  BOOST_CHECK_EQUAL( sink.connections(), 0u );
  sink.send( "three\n" );
  BOOST_CHECK_EQUAL( sink.stats().lines, 4u );
}

BOOST_AUTO_TEST_CASE( notices_a_consumer_leaving )
{
  fc::temp_directory dir;
  socket_sink sink( dir.path() / "stinger.sock" );

  auto c = std::make_unique< client >( sink.path() );
  sink.poll( HELLO );
  BOOST_REQUIRE_EQUAL( sink.connections(), 1u );
  c->close();
  // found by the next poll, without anything to send
  sink.poll( HELLO );
  BOOST_CHECK_EQUAL( sink.connections(), 0u );
  BOOST_CHECK_EQUAL( sink.stats().closed_by_peer, 1u );

  // or by the next send, when the poll did not run in between
  c = std::make_unique< client >( sink.path() );
  sink.poll( HELLO );
  BOOST_REQUIRE_EQUAL( sink.connections(), 1u );
  c->close();
  // the first send may still land in the dead socket's buffer; the second cannot
  sink.send( "a\n" );
  sink.send( "b\n" );
  BOOST_CHECK_EQUAL( sink.connections(), 0u );
  BOOST_CHECK_EQUAL( sink.stats().closed_by_peer, 2u );
  BOOST_CHECK_EQUAL( sink.stats().failed, 0u );
}

BOOST_AUTO_TEST_CASE( drops_a_consumer_that_stops_reading )
{
  fc::temp_directory dir;
  // a small send buffer keeps the test quick; the kernel rounds it up a little
  socket_sink sink( dir.path() / "stinger.sock", 64 * 1024 );

  client slow( sink.path() );
  client fast( sink.path() );
  sink.poll( HELLO );
  BOOST_REQUIRE_EQUAL( sink.connections(), 2u );
  BOOST_CHECK_EQUAL( fast.read( HELLO.size() ), HELLO );
  BOOST_CHECK_EQUAL( slow.read( HELLO.size() ), HELLO );

  // the fast one keeps up, the slow one never reads again
  const std::string line( 4096, 'x' );
  size_t sent = 0;
  for( ; sent < 4096 && sink.connections() == 2; ++sent )
  {
    sink.send( line );
    BOOST_REQUIRE_EQUAL( fast.read( line.size() ).size(), line.size() );
  }
  BOOST_CHECK_EQUAL( sink.connections(), 1u );
  BOOST_CHECK_EQUAL( sink.stats().dropped_behind, 1u );
  BOOST_CHECK_EQUAL( sink.stats().closed_by_peer, 0u );
  BOOST_CHECK_GT( sent, 1u );
  BOOST_TEST_MESSAGE( "slow consumer dropped after " << sent << " lines of " << line.size() << " bytes" );

  // the slow one sees what the kernel had, then EOF; the fast one is unaffected
  BOOST_CHECK( !slow.read( 1 ).empty() );
  sink.send( "still here\n" );
  BOOST_CHECK_EQUAL( fast.read( 11 ), "still here\n" );
}

BOOST_AUTO_TEST_CASE( consumer_input_is_ignored )
{
  fc::temp_directory dir;
  socket_sink sink( dir.path() / "stinger.sock" );
  client c( sink.path() );
  sink.poll( HELLO );
  BOOST_REQUIRE_EQUAL( sink.connections(), 1u );
  // a consumer that talks is drained, not disconnected
  BOOST_CHECK_EQUAL( c.read( HELLO.size() ), HELLO );
  {
    client talker( sink.path() );
    sink.poll( HELLO );
    BOOST_REQUIRE_EQUAL( sink.connections(), 2u );
  }
  sink.poll( HELLO );
  BOOST_CHECK_EQUAL( sink.connections(), 1u );
  sink.send( "line\n" );
  BOOST_CHECK_EQUAL( c.read( 5 ), "line\n" );
}

BOOST_AUTO_TEST_CASE( replaces_a_stale_socket )
{
  fc::temp_directory dir;
  const fc::path path = dir.path() / "stinger.sock";
  // a socket file nobody listens on, as a crashed node leaves behind
  {
    const int fd = ::socket( AF_UNIX, SOCK_STREAM, 0 );
    BOOST_REQUIRE( fd >= 0 );
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strcpy( addr.sun_path, path.generic_string().c_str() );
    BOOST_REQUIRE_EQUAL( ::bind( fd, reinterpret_cast< const sockaddr* >( &addr ), sizeof( addr ) ), 0 );
    ::close( fd );
  }
  BOOST_REQUIRE( fc::exists( path ) );

  socket_sink sink( path );
  client c( path );
  sink.poll( HELLO );
  BOOST_CHECK_EQUAL( sink.connections(), 1u );
}

BOOST_AUTO_TEST_CASE( refuses_a_socket_in_use )
{
  fc::temp_directory dir;
  const fc::path path = dir.path() / "stinger.sock";
  socket_sink first( path );
  BOOST_CHECK_THROW( socket_sink second( path ), fc::exception );
  // and did not take the first one's socket away
  client c( path );
  first.poll( HELLO );
  BOOST_CHECK_EQUAL( first.connections(), 1u );
  BOOST_CHECK( fc::exists( path ) );
}

BOOST_AUTO_TEST_CASE( refuses_a_path_that_is_not_a_socket )
{
  fc::temp_directory dir;
  const fc::path path = dir.path() / "stinger.sock";
  std::ofstream( path.generic_string() ) << "not a socket";
  BOOST_CHECK_THROW( socket_sink sink( path ), fc::exception );
  // left alone
  BOOST_CHECK( fc::exists( path ) );
  // a missing directory
  BOOST_CHECK_THROW( socket_sink sink( dir.path() / "nowhere" / "stinger.sock" ), fc::exception );
  // a path too long for a unix socket address
  BOOST_CHECK_THROW( socket_sink sink( dir.path() / std::string( 120, 'p' ) ), fc::exception );
}

BOOST_AUTO_TEST_SUITE_END()
#endif
