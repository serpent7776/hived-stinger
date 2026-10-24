#pragma once

#include <fc/filesystem.hpp>

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace hive { namespace plugins { namespace stinger {

/**
 * A unix stream socket the match stream is served on, as the lines
 * records.hpp renders. Local consumers connect, read lines, and never
 * write. Everything here runs on the sink thread, which is also the only
 * thread that calls it, so there is no locking.
 *
 * The node never waits for a consumer: a line is handed to each connection
 * with one non-blocking send, and a connection that cannot take all of it
 * at once - its kernel send buffer, a few MB, is full because the consumer
 * stopped reading - is closed. The consumer then knows it missed events: a
 * line without a trailing newline before EOF is torn and must be discarded,
 * and the `seq` of the first record after reconnecting says how many went
 * by. Nothing is buffered in userspace and nothing is retried.
 *
 * The socket file is created at construction (a stale one left by a dead
 * node is replaced; one another process is listening on is refused) with
 * owner-only permissions, and removed at destruction.
 */
class socket_sink
{
public:
  struct stats_type
  {
    uint64_t accepted        = 0;  ///< connections accepted
    uint64_t lines           = 0;  ///< lines handed to connections (one per connection per line)
    uint64_t bytes           = 0;  ///< bytes of those lines
    uint64_t closed_by_peer  = 0;  ///< connections the consumer closed
    uint64_t dropped_behind  = 0;  ///< connections closed because a line did not fit at once
    uint64_t failed          = 0;  ///< connections closed on any other error
  };

  /// kernel send buffer asked for per connection; the kernel doubles it,
  /// and caps the request at net.core.wmem_max (4 MB on a stock kernel)
  static constexpr int DEFAULT_SEND_BUFFER = 4 * 1024 * 1024;

  /// bind and listen on `path`; throws fc::exception when that fails
  explicit socket_sink( const fc::path& path, int send_buffer = DEFAULT_SEND_BUFFER );
  ~socket_sink();

  socket_sink( const socket_sink& ) = delete;
  socket_sink& operator=( const socket_sink& ) = delete;

  const fc::path& path() const { return _path; }
  size_t connections() const { return _connections.size(); }
  const stats_type& stats() const { return _stats; }

  /// Accept the connections waiting, greeting each with `hello`, and close
  /// the ones whose consumer has gone away. Never blocks; never throws.
  void poll( std::string_view hello ) noexcept;

  /// Hand one complete line, newline included, to every connection; a
  /// connection that cannot take all of it at once is closed. Never blocks;
  /// never throws.
  void send( std::string_view line ) noexcept;

private:
  /// send `line` on one connection; false when the connection was closed
  bool send_to( int fd, std::string_view line ) noexcept;
  void close_connection( size_t index, const char* why ) noexcept;
  void accept_pending( std::string_view hello ) noexcept;

  fc::path           _path;
  int                _send_buffer;
  int                _listen_fd = -1;
  std::vector< int > _connections;
  stats_type         _stats;
};

} } } // hive::plugins::stinger
