#pragma once

#include <hive/plugins/stinger/frame.hpp>
#include <hive/plugins/stinger/rules.hpp>

#include <hive/protocol/types.hpp>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace hive { namespace plugins { namespace stinger {

/**
 * The match stream as consumers see it on the socket (socket_sink.hpp):
 * newline-delimited JSON, one record per line, every record an object with
 * a `kind` and a `seq`. `seq` counts the records the node rendered since it
 * started, across all connections, so a consumer that reconnects can tell
 * how many it missed from the first `seq` it sees.
 *
 *   {"kind":"hello","version":1,"next_seq":N}
 *     first line of every connection; carries no seq of its own. `version`
 *     is this format's; a consumer should refuse one it does not know.
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
 *   {"kind":"switch_fork","seq":N,"block":B}
 *     the head stream's markers (frame.hpp): a block's head matches are
 *     closed by its block_applied; switch_fork retracts every block above
 *     B; irreversible says everything up to B is final. They exist only
 *     when a rule is delivered at head.
 */
class record_writer
{
public:
  static constexpr uint32_t FORMAT_VERSION = 1;

  /// `line_limit` bounds a rendered line, newline included; a match whose
  /// line would be longer is rendered again without its operation body
  explicit record_writer( size_t line_limit );

  /// the seq the next record will carry
  uint64_t next_seq() const { return _next_seq; }
  size_t line_limit() const { return _line_limit; }

  /// Each returns the record as one line, newline included, in a buffer
  /// that the next call overwrites.
  const std::string& hello();
  const std::string& match( delivery_class cls, const std::vector< std::string_view >& rules, const match_frame& m );
  const std::string& block_applied( uint32_t block_num, const protocol::block_id_type& block_id );
  const std::string& irreversible( uint32_t block_num );
  const std::string& switch_fork( uint32_t block_num );

private:
  const std::string& marker( const char* kind, uint32_t block_num, const protocol::block_id_type* block_id );

  size_t      _line_limit;
  uint64_t    _next_seq = 1;
  std::string _line;
};

} } } // hive::plugins::stinger
