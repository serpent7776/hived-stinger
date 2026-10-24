#include <hive/plugins/stinger/socket_sink.hpp>

#include <fc/exception/exception.hpp>
#include <fc/log/logger.hpp>

#include <cerrno>
#include <cstring>
#include <string>

#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

namespace hive { namespace plugins { namespace stinger {

namespace {

/// a descriptor closed when the scope ends unless released
class scoped_fd
{
public:
  explicit scoped_fd( int fd ) : _fd( fd ) {}
  ~scoped_fd() { if( _fd >= 0 ) ::close( _fd ); }
  scoped_fd( const scoped_fd& ) = delete;
  scoped_fd& operator=( const scoped_fd& ) = delete;
  int get() const { return _fd; }
  int release() { int fd = _fd; _fd = -1; return fd; }
private:
  int _fd;
};

std::string error_text( int err )
{
  return std::string( std::strerror( err ) ) + " (errno " + std::to_string( err ) + ")";
}

sockaddr_un address_of( const fc::path& path )
{
  const std::string p = path.generic_string();
  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;
  FC_ASSERT( p.size() < sizeof( addr.sun_path ),
    "socket path ${p} is longer than the ${n} bytes a unix socket address holds", ("p", p)("n", sizeof( addr.sun_path ) - 1) );
  std::memcpy( addr.sun_path, p.c_str(), p.size() + 1 );
  return addr;
}

/// true when something is listening at `addr`: a connect succeeds
bool in_use( const sockaddr_un& addr )
{
  scoped_fd probe( ::socket( AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0 ) );
  FC_ASSERT( probe.get() >= 0, "socket() failed: ${e}", ("e", error_text( errno )) );
  return ::connect( probe.get(), reinterpret_cast< const sockaddr* >( &addr ), sizeof( addr ) ) == 0;
}

} // anonymous namespace

socket_sink::socket_sink( const fc::path& path, int send_buffer )
  : _path( path ), _send_buffer( send_buffer )
{
  try
  {
    const sockaddr_un addr = address_of( _path );

    // whatever is at the path: a stale socket of a node that did not clean
    // up is replaced, a live one is somebody else's, anything else is a mistake
    struct stat st;
    if( ::lstat( addr.sun_path, &st ) == 0 )
    {
      FC_ASSERT( S_ISSOCK( st.st_mode ), "${p} exists and is not a socket", ("p", _path.generic_string()) );
      FC_ASSERT( !in_use( addr ), "another process is listening on ${p}", ("p", _path.generic_string()) );
      FC_ASSERT( ::unlink( addr.sun_path ) == 0 || errno == ENOENT,
        "cannot remove stale socket ${p}: ${e}", ("p", _path.generic_string())("e", error_text( errno )) );
    }
    else
      FC_ASSERT( errno == ENOENT, "cannot stat ${p}: ${e}", ("p", _path.generic_string())("e", error_text( errno )) );

    scoped_fd fd( ::socket( AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0 ) );
    FC_ASSERT( fd.get() >= 0, "socket() failed: ${e}", ("e", error_text( errno )) );
    FC_ASSERT( ::bind( fd.get(), reinterpret_cast< const sockaddr* >( &addr ), sizeof( addr ) ) == 0,
      "cannot bind ${p}: ${e}", ("p", _path.generic_string())("e", error_text( errno )) );
    // from here the path is ours to remove
    try
    {
      // a consumer connects by writing to the path: owner only, whatever the umask
      FC_ASSERT( ::chmod( addr.sun_path, S_IRUSR | S_IWUSR ) == 0,
        "cannot set permissions on ${p}: ${e}", ("p", _path.generic_string())("e", error_text( errno )) );
      FC_ASSERT( ::listen( fd.get(), 16 ) == 0,
        "cannot listen on ${p}: ${e}", ("p", _path.generic_string())("e", error_text( errno )) );
    }
    catch( ... )
    {
      ::unlink( addr.sun_path );
      throw;
    }
    _listen_fd = fd.release();
  } FC_CAPTURE_AND_RETHROW( (path.generic_string()) )
}

socket_sink::~socket_sink()
{
  for( int fd : _connections )
    ::close( fd );
  _connections.clear();
  if( _listen_fd >= 0 )
  {
    ::close( _listen_fd );
    ::unlink( _path.generic_string().c_str() );
  }
}

void socket_sink::poll( std::string_view hello ) noexcept
{
  // one poll over the listener and every connection: readable on the
  // listener is a connection waiting; readable or hung up on a connection
  // is the consumer closing (or talking, which is ignored and drained)
  std::vector< pollfd > fds;
  fds.reserve( _connections.size() + 1 );
  fds.push_back( pollfd{ _listen_fd, POLLIN, 0 } );
  for( int fd : _connections )
    fds.push_back( pollfd{ fd, POLLIN | POLLRDHUP, 0 } );

  const int ready = ::poll( fds.data(), fds.size(), 0 );
  if( ready < 0 )
  {
    if( errno != EINTR )
      wlog( "stinger: poll on ${p} failed: ${e}", ("p", _path.generic_string())("e", error_text( errno )) );
    return;
  }
  if( ready == 0 )
    return;

  // connections first, from the back, so closing does not shift the ones
  // still to look at; the listener's index does not move
  for( size_t i = _connections.size(); i-- > 0; )
  {
    const short revents = fds[ i + 1 ].revents;
    if( revents == 0 )
      continue;
    char buf[ 256 ];
    const ssize_t n = ::recv( _connections[ i ], buf, sizeof( buf ), MSG_DONTWAIT );
    if( n > 0 )
      continue;   // consumers have nothing to say; whatever this was is dropped
    if( n < 0 && ( errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR ) )
      continue;   // POLLRDHUP without data, or a spurious wakeup; the next send finds out
    // EOF, or ECONNRESET: a consumer that quit with lines it never read
    const bool left = n == 0 || errno == ECONNRESET;
    close_connection( i, left ? "closed by the consumer" : "failed" );
    if( left )
      ++_stats.closed_by_peer;
    else
      ++_stats.failed;
  }

  if( fds[ 0 ].revents != 0 )
    accept_pending( hello );
}

void socket_sink::accept_pending( std::string_view hello ) noexcept
{
  for( ;; )
  {
    const int fd = ::accept4( _listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC );
    if( fd < 0 )
    {
      if( errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR )
        return;
      wlog( "stinger: accept on ${p} failed: ${e}", ("p", _path.generic_string())("e", error_text( errno )) );
      return;
    }
    // the kernel's buffer is the only slack a consumer gets; best effort,
    // the kernel caps the request silently
    if( ::setsockopt( fd, SOL_SOCKET, SO_SNDBUF, &_send_buffer, sizeof( _send_buffer ) ) != 0 )
      wlog( "stinger: cannot set the send buffer on a connection to ${p}: ${e}",
        ("p", _path.generic_string())("e", error_text( errno )) );
    ++_stats.accepted;
    _connections.push_back( fd );
    ilog( "stinger: consumer connected to ${p}, ${n} connected", ("p", _path.generic_string())("n", _connections.size()) );
    if( !send_to( fd, hello ) )
      close_connection( _connections.size() - 1, "could not take the greeting" );
  }
}

void socket_sink::send( std::string_view line ) noexcept
{
  for( size_t i = _connections.size(); i-- > 0; )
  {
    if( send_to( _connections[ i ], line ) )
    {
      ++_stats.lines;
      _stats.bytes += line.size();
    }
    else
      close_connection( i, nullptr );
  }
}

bool socket_sink::send_to( int fd, std::string_view line ) noexcept
{
  for( ;; )
  {
    const ssize_t n = ::send( fd, line.data(), line.size(), MSG_NOSIGNAL | MSG_DONTWAIT );
    if( n == ssize_t( line.size() ) )
      return true;
    if( n < 0 && errno == EINTR )
      continue;
    if( n >= 0 || errno == EAGAIN || errno == EWOULDBLOCK )
    {
      // the kernel took some or none of it: the consumer is not reading.
      // A partial line is in flight, so this connection cannot be kept
      ++_stats.dropped_behind;
      wlog( "stinger: consumer on ${p} is behind by more than the send buffer - disconnecting it", ("p", _path.generic_string()) );
    }
    else if( errno == EPIPE || errno == ECONNRESET )
    {
      ++_stats.closed_by_peer;
      ilog( "stinger: consumer on ${p} went away", ("p", _path.generic_string()) );
    }
    else
    {
      ++_stats.failed;
      wlog( "stinger: send to a consumer on ${p} failed: ${e}", ("p", _path.generic_string())("e", error_text( errno )) );
    }
    return false;
  }
}

void socket_sink::close_connection( size_t index, const char* why ) noexcept
{
  ::close( _connections[ index ] );
  _connections.erase( _connections.begin() + index );
  if( why )
    ilog( "stinger: consumer on ${p} ${w}, ${n} connected", ("p", _path.generic_string())("w", why)("n", _connections.size()) );
}

} } } // hive::plugins::stinger
