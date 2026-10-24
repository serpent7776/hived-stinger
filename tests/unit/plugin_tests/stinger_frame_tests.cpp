#ifdef IS_TEST_NET
#include <boost/test/unit_test.hpp>

#include <hive/plugins/stinger/frame.hpp>

#include <hive/protocol/operations.hpp>

#include <fc/exception/exception.hpp>
#include <fc/io/raw.hpp>

#include <cstring>
#include <string>
#include <vector>

using namespace hive::plugins::stinger;
using namespace hive::protocol;

namespace {

constexpr size_t TEST_BUDGET = 4096;

match_coords sample_coords()
{
  match_coords c;
  c.block_num    = 123456;
  c.trx_in_block = 7;
  c.op_in_trx    = -1;
  c.timestamp    = fc::time_point_sec( 1700000000 );
  c.virtual_op   = true;
  c.trx_id       = fc::ripemd160::hash( std::string( "trx" ) );
  return c;
}

operation sample_transfer()
{
  transfer_operation op;
  op.from   = "alice";
  op.to     = "bob";
  op.amount = asset( 1000, HIVE_SYMBOL );
  op.memo   = "memo";
  return op;
}

/// a custom_json op whose packed size is controlled through the json length
operation custom_json_of_size( size_t json_len )
{
  custom_json_operation op;
  op.id   = "x";
  op.json = std::string( json_len, 'j' );
  return op;
}

/// operations have no operator==; compare their packed forms
bool same_op( const operation& a, const operation& b )
{
  return fc::raw::pack_to_vector( a ) == fc::raw::pack_to_vector( b );
}

/// append the encoder's current frame to a block's buffer, as the writer does
void append( std::vector< std::byte >& buf, const frame_encoder& enc )
{
  buf.insert( buf.end(), enc.data(), enc.data() + enc.size() );
}

/// copy the encoder's current frame into `buf` and read it back as the only frame
frame_view round_trip( std::vector< std::byte >& buf, const frame_encoder& enc )
{
  buf.clear();
  append( buf, enc );
  frame_reader reader( buf.data(), buf.size() );
  BOOST_REQUIRE( reader.more() );
  frame_view f = reader.next();
  BOOST_CHECK( !reader.more() );
  return f;
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE( stinger_frame )

BOOST_AUTO_TEST_CASE( match_frame_round_trip )
{
  frame_encoder enc( TEST_BUDGET );

  const match_coords coords = sample_coords();
  const operation    op     = sample_transfer();
  const uint64_t     ids[]  = { 1, 0xffffffffffffffffull, 42 };
  enc.encode_match( coords, ids, 3, op );
  BOOST_CHECK_EQUAL( enc.size(), match_frame_overhead( 3 ) + fc::raw::pack_size( op ) );

  std::vector< std::byte > buf;
  frame_view f = round_trip( buf, enc );
  BOOST_CHECK( f.kind() == frame_kind::match );
  BOOST_CHECK( !f.body_omitted() );
  BOOST_CHECK_EQUAL( f.header.rule_count, 3u );
  BOOST_CHECK_EQUAL( f.header.len, enc.size() - sizeof( frame_header ) );

  match_frame m = decode_match( f );
  BOOST_CHECK_EQUAL( m.coords.block_num, coords.block_num );
  BOOST_CHECK_EQUAL( m.coords.trx_in_block, coords.trx_in_block );
  BOOST_CHECK_EQUAL( m.coords.op_in_trx, coords.op_in_trx );
  BOOST_CHECK( m.coords.timestamp == coords.timestamp );
  BOOST_CHECK_EQUAL( m.coords.virtual_op, coords.virtual_op );
  BOOST_CHECK( m.coords.trx_id == coords.trx_id );
  BOOST_REQUIRE_EQUAL( m.rule_count, 3u );
  for( size_t i = 0; i < 3; ++i )
    BOOST_CHECK_EQUAL( m.rule_id( i ), ids[ i ] );
  BOOST_CHECK( !m.body_omitted );
  BOOST_CHECK_EQUAL( m.body_len, fc::raw::pack_size( op ) );
  BOOST_CHECK( same_op( m.unpack_op(), op ) );
}

BOOST_AUTO_TEST_CASE( match_frame_without_rules )
{
  frame_encoder enc( TEST_BUDGET );
  const operation op = sample_transfer();
  enc.encode_match( sample_coords(), nullptr, 0, op );

  std::vector< std::byte > buf;
  match_frame m = decode_match( round_trip( buf, enc ) );
  BOOST_CHECK_EQUAL( m.rule_count, 0u );
  BOOST_CHECK( same_op( m.unpack_op(), op ) );
}

BOOST_AUTO_TEST_CASE( body_omitted_when_op_exceeds_budget )
{
  // the budget is the whole frame, so the body gets budget - overhead bytes:
  // an op packing to exactly that fits, one byte more is left out
  const operation small  = custom_json_of_size( 100 );
  const size_t    budget = match_frame_overhead( 1 ) + fc::raw::pack_size( small );
  const uint64_t  id     = 7;

  frame_encoder enc( budget );
  enc.encode_match( sample_coords(), &id, 1, small );
  BOOST_CHECK_EQUAL( enc.size(), budget );
  {
    std::vector< std::byte > buf;
    match_frame m = decode_match( round_trip( buf, enc ) );
    BOOST_CHECK( !m.body_omitted );
    BOOST_CHECK( same_op( m.unpack_op(), small ) );
  }

  const operation large = custom_json_of_size( 101 );
  enc.encode_match( sample_coords(), &id, 1, large );
  BOOST_CHECK_EQUAL( enc.size(), match_frame_overhead( 1 ) );
  {
    std::vector< std::byte > buf;
    frame_view f = round_trip( buf, enc );
    BOOST_CHECK( f.body_omitted() );
    match_frame m = decode_match( f );
    BOOST_CHECK( m.body_omitted );
    BOOST_CHECK_EQUAL( m.body_len, 0u );
    BOOST_CHECK_EQUAL( m.rule_count, 1u );
    BOOST_CHECK_EQUAL( m.rule_id( 0 ), id );
    BOOST_CHECK_THROW( m.unpack_op(), fc::exception );
  }
}

BOOST_AUTO_TEST_CASE( encoder_rejects_impossible_frames )
{
  BOOST_CHECK_THROW( frame_encoder( match_frame_overhead( 0 ) ), fc::exception );

  frame_encoder enc( 1024 );
  const operation op = sample_transfer();
  // more rule ids than the budget holds
  std::vector< uint64_t > ids( 200, 1 );
  BOOST_CHECK_THROW( enc.encode_match( sample_coords(), ids.data(), ids.size(), op ), fc::exception );
  // more rule ids than rule_count can count
  frame_encoder wide( 1 << 20 );
  ids.assign( 65536, 1 );
  BOOST_CHECK_THROW( wide.encode_match( sample_coords(), ids.data(), ids.size(), op ), fc::exception );
}

BOOST_AUTO_TEST_CASE( lifecycle_frames_round_trip )
{
  frame_encoder enc( TEST_BUDGET );
  const block_id_type block_id = fc::ripemd160::hash( std::string( "block" ) );

  std::vector< std::byte > buf;
  enc.encode_block_applied( 1000, block_id );
  append( buf, enc );
  enc.encode_irreversible( 980 );
  append( buf, enc );
  enc.encode_switch_fork( 995 );
  append( buf, enc );
  BOOST_CHECK_EQUAL( buf.size(), 3 * sizeof( frame_header ) + 24 + 4 + 4 );

  frame_reader reader( buf.data(), buf.size() );
  frame_view f = reader.next();
  BOOST_CHECK( f.kind() == frame_kind::block_applied );
  uint32_t      num;
  block_id_type id;
  decode_block_applied( f, num, id );
  BOOST_CHECK_EQUAL( num, 1000u );
  BOOST_CHECK( id == block_id );
  BOOST_CHECK_THROW( decode_match( f ), fc::exception );
  BOOST_CHECK_THROW( decode_block_num( f ), fc::exception );

  f = reader.next();
  BOOST_CHECK( f.kind() == frame_kind::irreversible );
  BOOST_CHECK_EQUAL( decode_block_num( f ), 980u );
  BOOST_CHECK_THROW( decode_block_applied( f, num, id ), fc::exception );

  f = reader.next();
  BOOST_CHECK( f.kind() == frame_kind::switch_fork );
  BOOST_CHECK_EQUAL( decode_block_num( f ), 995u );

  BOOST_CHECK( !reader.more() );
  BOOST_CHECK_THROW( reader.next(), fc::exception );
}

BOOST_AUTO_TEST_CASE( reader_rejects_frame_running_past_buffer )
{
  // a buffer whose only frame claims more payload than the buffer holds
  std::vector< std::byte > buf( sizeof( frame_header ) + 4 );
  frame_header fh{ 100, uint8_t( frame_kind::match ), 0, 0 };
  std::memcpy( buf.data(), &fh, sizeof( fh ) );

  frame_reader reader( buf.data(), buf.size() );
  BOOST_CHECK( reader.more() );
  BOOST_CHECK_THROW( reader.next(), fc::exception );

  // a header that itself does not fit
  frame_reader truncated( buf.data(), 3 );
  BOOST_CHECK( truncated.more() );
  BOOST_CHECK_THROW( truncated.next(), fc::exception );

  // an empty buffer has no frames and says so
  frame_reader empty( buf.data(), 0 );
  BOOST_CHECK( !empty.more() );
  BOOST_CHECK_THROW( empty.next(), fc::exception );
}

BOOST_AUTO_TEST_CASE( decoder_rejects_short_match_payload )
{
  // a match frame claiming two rule ids but carrying only the coordinates
  std::vector< std::byte > buf( sizeof( frame_header ) + match_coords_size );
  frame_header fh{ match_coords_size, uint8_t( frame_kind::match ), 0, 2 };
  std::memcpy( buf.data(), &fh, sizeof( fh ) );

  frame_reader reader( buf.data(), buf.size() );
  frame_view f = reader.next();
  BOOST_CHECK_THROW( decode_match( f ), fc::exception );
}

BOOST_AUTO_TEST_SUITE_END()
#endif
