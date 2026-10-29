#pragma once

#include <hive/plugins/stinger/frame.hpp>
#include <hive/plugins/stinger/rules.hpp>
#include <hive/plugins/stinger/streams.hpp>

#include <hive/protocol/types.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace hive { namespace plugins { namespace stinger {

/**
 * The match stream as consumers see it on the socket (socket_sink.hpp):
 * newline-delimited JSON, one record per line, every record an object with
 * a `kind` and a `seq`. `seq` counts the records the node rendered since it
 * started, across all connections: each record's is one above the one
 * before it, so a consumer that reconnects can tell how many it missed
 * from its hello's.
 *
 *   {"kind":"hello","seq":N,"version":1,
 *    "delivered":{"head":{"block":B,"block_id":"..."},"irreversible":I}}
 *     first line of every connection. Its seq is a position, like the rest
 *     of the greeting: the seq of the last record the node rendered, 0 for
 *     none, so the first record after it carries N+1. A greeting consumes
 *     no seq - a consumer never sees a gap because another one connected.
 *     `version` is this format's; a consumer should refuse one it does not
 *     know.
 *     `delivered` says where each stream stands, since the socket keeps no
 *     history: what this connection will get, and what a consumer has to
 *     fetch elsewhere. `head` is the head stream's position: the height of
 *     the last block_applied or switch_fork it delivered, with the id of
 *     the block there. It is a cut: nothing at or below it follows, every
 *     block above it does, so a consumer holding blocks above B, or a
 *     different block at B, drops them as on a switch_fork. null until the
 *     stream delivered a block - no head rules, or the node not yet live;
 *     the first block_applied then says where the stream starts.
 *     `irreversible` is the irreversible stream's position: every matched
 *     block at or below I has been released to the socket, and every block
 *     above I that the chain makes irreversible is delivered. A consumer
 *     waits until the node's last irreversible block is at or above I,
 *     backfills up to I from the node's APIs, and takes what follows from
 *     the socket. It is a bound, not a cut: matches at or below I released
 *     just before the connection may still be queued and follow the
 *     greeting; the backfill covers them. It is the highest block the chain
 *     has made irreversible, as the writer saw it, or one below the lowest
 *     block the node has matched, whichever is higher
 *     (irreversible_position): a node that synced with matching off
 *     (stinger_plugin.cpp) never looked at the blocks below that one, so
 *     neither stream carries them, and the irreversible stream has no
 *     marker that would show the gap. That is why the consumer waits: while
 *     I is above the chain's last irreversible block, the blocks between
 *     them are reversible, and the stream cannot correct a backfill a fork
 *     replaces - a replacement block with no matching operation sends
 *     nothing. Once the chain is irreversible up to I, what the consumer
 *     fetches is final, and matches at or below I that arrive later are
 *     duplicates of it. null while the stream has not started - nothing
 *     matched yet, so where it will start is not known; 0 once it has and
 *     nothing is irreversible yet.
 *     Both positions assume the sink kept up; see dropped for what a
 *     consumer sees when it did not.
 *
 *   {"kind":"match","seq":N,"delivery":"irreversible"|"head","rules":["..."],
 *    "block":B,"timestamp":"...","trx_id":"...","trx_in_block":T,"op_in_trx":O,
 *    "virtual_op":false,"op":{"type":"...","value":{...}}}
 *     one matched operation, for the rules of one delivery class that
 *     matched it and are delivered on the socket. An operation matched by
 *     rules of both classes appears once per class, in each class's order.
 *     `op` is the operation in database_api form; it is null, and
 *     `op_omitted` says why, when the body was left out of the frame or
 *     the rendered line would exceed the line limit.
 *
 *   {"kind":"block_applied","seq":N,"block":B,"block_id":"..."}
 *   {"kind":"irreversible","seq":N,"block":B}
 *   {"kind":"switch_fork","seq":N,"block":B,"block_id":"..."}
 *     the head stream's markers (frame.hpp): a block's head matches are
 *     closed by its block_applied; switch_fork retracts every block above
 *     B and names the block now at B; irreversible says everything up to B
 *     is final. They exist only when a rule is delivered at head.
 *     block_applied heights are contiguous, except when the sink fell
 *     behind and dropped a block's batch (see dropped), or failed to
 *     deliver it (hived logs it): then the height is skipped, its head
 *     matches are lost, and a consumer that sees the gap fetches the block
 *     from the node.
 *
 *   {"kind":"dropped","seq":N,"delivery":"irreversible"|"head",
 *    "from_block":A,"to_block":B,"blocks":K}
 *     the node fell behind and threw away the matches of K blocks of one
 *     delivery class, all between A and B inclusive (blocks of the range
 *     not counted had no matches), before they reached the socket. The
 *     record stands where those blocks' records would have: after the
 *     records of what preceded them, a switch_fork included, and before
 *     those of what followed, so the records of this class that follow
 *     are from blocks above B until the next switch_fork. Matches are held for
 *     the sink under a memory budget (stinger-ready-queue-mb), and when a
 *     burst of matches - a block with tens of thousands of virtual
 *     operations, a sync faster than the sink renders - takes it over,
 *     the oldest waiting blocks go. A consumer fetches the blocks of the
 *     range from the node: for the irreversible class they are final; for
 *     the head class they were applied, and a later switch_fork may still
 *     retract them like any other. Every consumer connected at the time
 *     gets the record; a consumer that connects later sees nothing, which
 *     the positions in its hello allow for (they are where the streams
 *     stand, not what reached the socket). `seq` is consumed like any
 *     other record's: this is about the node's queue, not the socket -
 *     a consumer that fell behind the socket is disconnected instead
 *     (socket_sink.hpp).
 */

/// What the hello record says about where each stream stands; see there.
struct stream_position
{
  /// head stream: the height of the last block_applied or switch_fork
  /// delivered, and the id of the block at that height; 0 is none yet,
  /// rendered null
  uint32_t                  head_block = 0;
  protocol::block_id_type   head_id;
  /// irreversible stream: the block a consumer backfills up to; none,
  /// rendered null, while the stream has not started
  std::optional< uint32_t > irreversible;
};

/// The irreversible stream's position for the hello, from the highest block
/// the chain has made irreversible and the lowest block matched so far, as
/// the writer stored them (0 for none): none until a block was matched,
/// else the irreversible height or one below the lowest matched block,
/// whichever is higher
inline std::optional< uint32_t > irreversible_position( uint32_t last_irreversible, uint32_t matched_from )
{
  if( matched_from == 0 )
    return std::nullopt;
  return std::max( last_irreversible, matched_from - 1 );
}

class record_writer
{
public:
  static constexpr uint32_t FORMAT_VERSION = 1;

  /// `line_limit` bounds a rendered line, newline included; a match whose
  /// line would be longer is rendered again without its operation body
  explicit record_writer( size_t line_limit );

  /// the seq the next record will carry
  uint64_t next_seq() const { return _next_seq; }
  /// the seq of the last record rendered, 0 for none: the hello's seq
  uint64_t last_seq() const { return _next_seq - 1; }
  size_t line_limit() const { return _line_limit; }

  /// Each returns the record as one line, newline included, in a buffer
  /// that the next call overwrites.
  const std::string& hello( const stream_position& position );
  const std::string& match( delivery_class cls, const std::vector< std::string_view >& rules, const match_frame& m );
  const std::string& block_applied( uint32_t block_num, const protocol::block_id_type& block_id );
  const std::string& irreversible( uint32_t block_num );
  const std::string& switch_fork( uint32_t block_num, const protocol::block_id_type& block_id );
  const std::string& dropped( delivery_class cls, const dropped_range& range );

private:
  const std::string& marker( const char* kind, uint32_t block_num, const protocol::block_id_type* block_id );

  size_t      _line_limit;
  uint64_t    _next_seq = 1;
  std::string _line;
};

} } } // hive::plugins::stinger
