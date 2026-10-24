#pragma once

#include <hive/protocol/operations.hpp>
#include <hive/protocol/types.hpp>

#include <fc/exception/exception.hpp>
#include <fc/io/datastream.hpp>
#include <fc/io/raw.hpp>
#include <fc/time.hpp>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>
#include <vector>

namespace hive { namespace plugins { namespace stinger {

/// tag selecting the bounded fc::datastream specialization below
struct bounded_window;

} } } // hive::plugins::stinger

namespace fc {

/**
 * fc::datastream over a fixed window that records overflow instead of
 * throwing: the primary template's overflow path is assert(false) in debug
 * builds, so it cannot be the trigger for leaving an operation body out of
 * a frame. Once over, writes are discarded and the flag sticks; a
 * fc::raw::pack traversal runs to the end of the operation (bounded by the
 * operation itself) and the body is then left out as a whole.
 *
 * It is a specialization on a tag type rather than a class of its own so
 * that the operator<< overloads fc and hive::protocol declare for
 * datastream<ST> and for generic streams - found through ADL in namespace
 * fc - apply to it; pack only ever calls write() and tellp() on it. A plain
 * class in the stinger namespace does not compile: the generic
 * operator<<(Stream&, const json_string&) is declared in namespace fc, which
 * is an associated namespace of the stream only when the stream is an
 * fc type, so pack of any operation carrying a json_string fails to find it.
 */
template<>
class datastream< hive::plugins::stinger::bounded_window >
{
public:
  datastream( char* start, size_t size ) : _start( start ), _pos( start ), _end( start + size ) {}

  bool write( const char* d, size_t s )
  {
    if( _overflow || s > size_t( _end - _pos ) )
    {
      _overflow = true;
      return false;
    }
    std::memcpy( _pos, d, s );
    _pos += s;
    return true;
  }
  bool put( char c ) { return write( &c, 1 ); }
  /// bytes written so far
  size_t tellp() const { return size_t( _pos - _start ); }
  /// true once a write did not fit
  bool overflowed() const { return _overflow; }

private:
  char* _start;
  char* _pos;
  char* _end;
  bool  _overflow = false;
};

} // fc

namespace hive { namespace plugins { namespace stinger {

using bounded_stream = fc::datastream< bounded_window >;

/**
 * The match stream's frame format: what the writer thread appends to a
 * block's match buffer and the sink thread decodes. Byte-oriented with no
 * alignment requirement (headers are memcpy'd into stack structs on both sides),
 * native endian, in-process only - it is not an on-the-wire format, so it
 * carries no version until a sink writes it to a file or a socket.
 *
 *   header (8 bytes)
 *     uint32 len             payload bytes following this header
 *     uint8  kind            frame_kind
 *     uint8  flags           bit 0: body_omitted
 *     uint16 rule_count      match frames only
 *
 *   match payload (37 + 8 * rule_count + packed_op bytes)
 *     uint32 block_num
 *     int32  trx_in_block
 *     int32  op_in_trx
 *     uint32 timestamp       time_point_sec
 *     uint8  virtual_op
 *     uint8  trx_id[20]      transaction_id_type (ripemd160)
 *     uint64 rule_ids[rule_count]
 *     uint8  packed_op[]     fc::raw::pack of the operation; empty when body_omitted
 *
 *   block_applied payload (24 bytes)
 *     uint32 block_num
 *     uint8  block_id[20]    block_id_type (ripemd160)
 *
 *   irreversible / switch_fork payload (4 bytes)
 *     uint32 block_num
 *
 * One frame per matched operation however many rules matched it: the rule
 * ids are an array, so the body is copied once and the sink fans out.
 * trx_in_block and op_in_trx are positions within a block, stored as int32
 * (they are int64 in operation_notification); the signed type is kept
 * because virtual ops outside a transaction may carry a negative marker.
 */
enum class frame_kind : uint8_t
{
  match         = 0,
  block_applied = 1,
  irreversible  = 2,
  switch_fork   = 3
};

struct frame_header
{
  uint32_t len;
  uint8_t  kind;
  uint8_t  flags;
  uint16_t rule_count;

  /// the packed operation body was too large for the frame budget and was left out.
  /// Not expected in practice: an operation in a block is bounded by the block
  /// size ceiling and the budget is above it. Safety valve, not a path real
  /// traffic takes.
  static constexpr uint8_t FLAG_BODY_OMITTED = 1;
};
static_assert( sizeof( frame_header ) == 8, "frame header must be 8 bytes" );

/// Where in the chain a matched operation sits; the fixed-size part of a
/// match payload. The in-memory struct is padded; the frame holds its fields
/// back to back, see match_coords_size.
struct match_coords
{
  uint32_t            block_num    = 0;
  int32_t             trx_in_block = 0;
  int32_t             op_in_trx    = 0;
  fc::time_point_sec  timestamp;
  bool                virtual_op   = false;
  protocol::transaction_id_type trx_id;
};
/// bytes the coordinates take in a frame
constexpr size_t match_coords_size = 4 + 4 + 4 + 4 + 1 + 20;
static_assert( sizeof( protocol::transaction_id_type ) == 20, "transaction id must be 20 bytes" );

/// bytes a match frame takes before the packed operation body
constexpr size_t match_frame_overhead( size_t rule_count )
{
  return sizeof( frame_header ) + match_coords_size + 8 * rule_count;
}

/**
 * Builds frames in a scratch buffer owned by the writer thread, one at a
 * time, for the caller to copy into a block's match buffer. Allocates once
 * at construction (the scratch is the frame budget) and never again.
 *
 * The budget is the bound the packed operation is checked against, minus
 * the frame's own overhead, so every frame built here fits the budget by
 * construction; an operation that does not fit is replaced by the
 * body_omitted flag rather than failing the frame. Packing goes through a
 * bounded stream in one pass - no pack_size pre-pass, so an operation is
 * traversed once.
 */
class frame_encoder
{
public:

  /// `budget` is the largest frame the encoder may build
  explicit frame_encoder( size_t budget )
    : _scratch( budget )
  {
    FC_ASSERT( budget > match_frame_overhead( 0 ), "frame budget of ${b} bytes cannot hold a match frame", ("b", budget) );
  }

  frame_encoder( const frame_encoder& ) = delete;
  frame_encoder& operator=( const frame_encoder& ) = delete;

  /// the largest frame this encoder builds
  size_t budget() const { return _scratch.size(); }
  /// the frame built by the last encode_* call
  const std::byte* data() const { return _scratch.data(); }
  /// its length
  size_t size() const { return _size; }

  /// A match frame for `op`, matched by the `rule_count` rules in `rule_ids`.
  /// The body is left out, and the frame flagged, when the packed operation
  /// does not fit the budget alongside the frame's overhead.
  void encode_match( const match_coords& coords, const uint64_t* rule_ids, size_t rule_count,
                     const protocol::operation& op )
  {
    FC_ASSERT( rule_count <= std::numeric_limits< uint16_t >::max(),
               "${n} matched rules exceed what a frame can carry", ("n", rule_count) );
    const size_t overhead = match_frame_overhead( rule_count );
    FC_ASSERT( overhead <= budget(), "${n} matched rules exceed the frame budget", ("n", rule_count) );

    std::byte* p = _scratch.data() + sizeof( frame_header );
    p = put( p, coords.block_num );
    p = put( p, coords.trx_in_block );
    p = put( p, coords.op_in_trx );
    p = put( p, coords.timestamp.sec_since_epoch() );
    p = put( p, uint8_t( coords.virtual_op ? 1 : 0 ) );
    p = put( p, coords.trx_id.data(), coords.trx_id.data_size() );
    for( size_t i = 0; i < rule_count; ++i )
      p = put( p, rule_ids[ i ] );
    assert( size_t( p - _scratch.data() ) == overhead );

    uint8_t flags = 0;
    size_t  body  = 0;
    bounded_stream ds( reinterpret_cast< char* >( p ), budget() - overhead );
    fc::raw::pack( ds, op );
    if( ds.overflowed() )
      flags |= frame_header::FLAG_BODY_OMITTED;   // the packed body does not fit the budget
    else
      body = ds.tellp();

    finish( frame_kind::match, flags, uint16_t( rule_count ), match_coords_size + 8 * rule_count + body );
  }

  /// a block_applied frame: the block's number and id
  void encode_block_applied( uint32_t block_num, const protocol::block_id_type& block_id )
  {
    std::byte* p = _scratch.data() + sizeof( frame_header );
    p = put( p, block_num );
    p = put( p, block_id.data(), block_id.data_size() );
    finish( frame_kind::block_applied, 0, 0, size_t( p - _scratch.data() ) - sizeof( frame_header ) );
  }

  /// an irreversible frame: everything up to and including `block_num` is final
  void encode_irreversible( uint32_t block_num )
  {
    put( _scratch.data() + sizeof( frame_header ), block_num );
    finish( frame_kind::irreversible, 0, 0, sizeof( block_num ) );
  }

  /// a switch_fork frame: everything above `new_head_block_num` is discarded
  void encode_switch_fork( uint32_t new_head_block_num )
  {
    put( _scratch.data() + sizeof( frame_header ), new_head_block_num );
    finish( frame_kind::switch_fork, 0, 0, sizeof( new_head_block_num ) );
  }

private:
  template< typename T >
  static std::byte* put( std::byte* p, const T& v )
  {
    static_assert( std::is_trivially_copyable_v< T >, "frame fields are copied byte-wise" );
    std::memcpy( p, &v, sizeof( v ) );
    return p + sizeof( v );
  }

  static std::byte* put( std::byte* p, const char* bytes, size_t n )
  {
    std::memcpy( p, bytes, n );
    return p + n;
  }

  /// stamp the header once the payload is in place and record the frame length
  void finish( frame_kind kind, uint8_t flags, uint16_t rule_count, size_t payload )
  {
    assert( sizeof( frame_header ) + payload <= budget() );
    const frame_header h{ uint32_t( payload ), uint8_t( kind ), flags, rule_count };
    std::memcpy( _scratch.data(), &h, sizeof( h ) );
    _size = sizeof( frame_header ) + payload;
  }

  std::vector< std::byte > _scratch;
  size_t                   _size = 0;
};

/// One frame as the sink sees it: the header and a view of the payload,
/// which lives in the block's match buffer for as long as the sink holds it.
struct frame_view
{
  frame_header     header;
  const std::byte* payload = nullptr;

  frame_kind kind() const { return frame_kind( header.kind ); }
  bool body_omitted() const { return header.flags & frame_header::FLAG_BODY_OMITTED; }
};

/**
 * Walks the frames packed back to back in `len` bytes at `data`: one
 * block's match buffer as the writer sealed it. Every step checks that the
 * frame stays within the buffer; a violation throws fc::exception - an
 * encoder-bug tripwire, not a race check: a released block is the sink's alone.
 */
class frame_reader
{
public:
  frame_reader( const std::byte* data, size_t len ) : _cur( data ), _end( data + len ) {}

  /// true while frames remain
  bool more() const { return _cur < _end; }

  /// the next frame; call only while more()
  frame_view next()
  {
    FC_ASSERT( more(), "no frames left" );
    FC_ASSERT( size_t( _end - _cur ) >= sizeof( frame_header ), "frame header runs past the end of the buffer" );
    frame_view f;
    std::memcpy( &f.header, _cur, sizeof( f.header ) );
    _cur += sizeof( f.header );
    FC_ASSERT( size_t( _end - _cur ) >= f.header.len,
               "frame of ${n} bytes runs past the end of the buffer", ("n", f.header.len) );
    f.payload = _cur;
    _cur += f.header.len;
    return f;
  }

private:
  const std::byte* _cur;
  const std::byte* _end;
};

/// A decoded match frame: coordinates, rule ids, and a view of the packed
/// operation body (empty when body_omitted). The views point into the block's buffer.
struct match_frame
{
  match_coords     coords;
  const std::byte* rule_ids   = nullptr;
  uint16_t         rule_count = 0;
  const std::byte* body       = nullptr;
  size_t           body_len   = 0;
  bool             body_omitted = false;

  /// the i-th matched rule's subscription id
  uint64_t rule_id( size_t i ) const
  {
    assert( i < rule_count );
    uint64_t id;
    std::memcpy( &id, rule_ids + 8 * i, sizeof( id ) );
    return id;
  }

  /// the operation, unpacked from the body; call only when !body_omitted
  protocol::operation unpack_op() const
  {
    FC_ASSERT( !body_omitted, "match frame carries no operation body" );
    return fc::raw::unpack_from_char_array< protocol::operation >(
      reinterpret_cast< const char* >( body ), uint32_t( body_len ) );
  }
};

/// decode a frame_kind::match frame; throws fc::exception on a malformed payload
inline match_frame decode_match( const frame_view& f )
{
  FC_ASSERT( f.kind() == frame_kind::match, "not a match frame" );
  const size_t fixed = match_coords_size + 8 * size_t( f.header.rule_count );
  FC_ASSERT( f.header.len >= fixed, "match frame payload of ${n} bytes is shorter than its ${c} rule ids need",
             ("n", f.header.len)("c", f.header.rule_count) );

  match_frame m;
  const std::byte* p = f.payload;
  uint32_t timestamp;
  uint8_t  virtual_op;
  std::memcpy( &m.coords.block_num,    p, 4 ); p += 4;
  std::memcpy( &m.coords.trx_in_block, p, 4 ); p += 4;
  std::memcpy( &m.coords.op_in_trx,    p, 4 ); p += 4;
  std::memcpy( &timestamp,             p, 4 ); p += 4;
  std::memcpy( &virtual_op,            p, 1 ); p += 1;
  std::memcpy( m.coords.trx_id.data(), p, m.coords.trx_id.data_size() ); p += m.coords.trx_id.data_size();
  m.coords.timestamp  = fc::time_point_sec( timestamp );
  m.coords.virtual_op = virtual_op != 0;
  m.rule_ids     = p;
  m.rule_count   = f.header.rule_count;
  m.body         = p + 8 * size_t( m.rule_count );
  m.body_len     = f.header.len - fixed;
  m.body_omitted = f.body_omitted();
  FC_ASSERT( !m.body_omitted || m.body_len == 0, "match frame flagged body_omitted carries ${n} body bytes", ("n", m.body_len) );
  return m;
}

/// decode a frame_kind::block_applied frame
inline void decode_block_applied( const frame_view& f, uint32_t& block_num, protocol::block_id_type& block_id )
{
  FC_ASSERT( f.kind() == frame_kind::block_applied, "not a block_applied frame" );
  FC_ASSERT( f.header.len == 4 + block_id.data_size(), "block_applied frame has ${n} payload bytes", ("n", f.header.len) );
  std::memcpy( &block_num, f.payload, 4 );
  std::memcpy( block_id.data(), f.payload + 4, block_id.data_size() );
}

/// decode a frame_kind::irreversible or frame_kind::switch_fork frame
inline uint32_t decode_block_num( const frame_view& f )
{
  FC_ASSERT( f.kind() == frame_kind::irreversible || f.kind() == frame_kind::switch_fork,
             "frame kind ${k} carries no bare block number", ("k", f.header.kind) );
  FC_ASSERT( f.header.len == 4, "block number frame has ${n} payload bytes", ("n", f.header.len) );
  uint32_t block_num;
  std::memcpy( &block_num, f.payload, 4 );
  return block_num;
}

} } } // hive::plugins::stinger
