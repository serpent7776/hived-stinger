#ifdef IS_TEST_NET
#include <boost/test/unit_test.hpp>

#include <hive/plugins/stinger/frame.hpp>
#include <hive/plugins/stinger/page_buffer.hpp>

#include <hive/protocol/operations.hpp>

#include <fc/exception/exception.hpp>
#include <fc/io/raw.hpp>

#include <cstring>
#include <string>
#include <vector>

using namespace hive::plugins::stinger;
using namespace hive::protocol;

namespace {

constexpr size_t TEST_PAGE_SIZE  = 4096;
constexpr size_t TEST_PAGE_COUNT = 2;

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

/// push the encoder's current frame through a page buffer and read it back
frame_view round_trip( page_buffer& buf, const frame_encoder& enc, const std::byte*& page )
{
  buf.append( enc.data(), enc.size() );
  buf.rotate();
  page = buf.take( nullptr );
  BOOST_REQUIRE( page != nullptr );
  page_reader reader( page, TEST_PAGE_SIZE );
  BOOST_REQUIRE( reader.more() );
  frame_view f = reader.next();
  BOOST_CHECK( !reader.more() );
  return f;
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE( stinger_frame )

BOOST_AUTO_TEST_CASE( match_frame_round_trip )
{
  page_buffer   buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );
  frame_encoder enc( buf.capacity() );

  const match_coords coords = sample_coords();
  const operation    op     = sample_transfer();
  const uint64_t     ids[]  = { 1, 0xffffffffffffffffull, 42 };
  enc.encode_match( coords, ids, 3, op );
  BOOST_CHECK_EQUAL( enc.size(), match_frame_overhead( 3 ) + fc::raw::pack_size( op ) );

  const std::byte* page;
  frame_view f = round_trip( buf, enc, page );
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
  page_buffer   buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );
  frame_encoder enc( buf.capacity() );
  const operation op = sample_transfer();
  enc.encode_match( sample_coords(), nullptr, 0, op );

  const std::byte* page;
  match_frame m = decode_match( round_trip( buf, enc, page ) );
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
    page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );
    const std::byte* page;
    match_frame m = decode_match( round_trip( buf, enc, page ) );
    BOOST_CHECK( !m.body_omitted );
    BOOST_CHECK( same_op( m.unpack_op(), small ) );
  }

  const operation large = custom_json_of_size( 101 );
  enc.encode_match( sample_coords(), &id, 1, large );
  BOOST_CHECK_EQUAL( enc.size(), match_frame_overhead( 1 ) );
  {
    page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );
    const std::byte* page;
    frame_view f = round_trip( buf, enc, page );
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
  page_buffer   buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );
  frame_encoder enc( buf.capacity() );
  const block_id_type block_id = fc::ripemd160::hash( std::string( "block" ) );

  enc.encode_block_applied( 1000, block_id );
  buf.append( enc.data(), enc.size() );
  enc.encode_irreversible( 980 );
  buf.append( enc.data(), enc.size() );
  enc.encode_switch_fork( 995 );
  buf.append( enc.data(), enc.size() );
  buf.rotate();

  const std::byte* page = buf.take( nullptr );
  BOOST_REQUIRE( page != nullptr );
  page_reader reader( page, TEST_PAGE_SIZE );
  BOOST_CHECK_EQUAL( reader.header().used, 3 * sizeof( frame_header ) + 24 + 4 + 4 );

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

BOOST_AUTO_TEST_CASE( reader_rejects_frame_running_past_page )
{
  // a page whose only frame claims more payload than the page holds
  std::vector< std::byte > page( TEST_PAGE_SIZE );
  page_header  ph{ 0, sizeof( frame_header ) + 4, 0 };
  frame_header fh{ 100, uint8_t( frame_kind::irreversible ), 0, 0 };
  std::memcpy( page.data(), &ph, sizeof( ph ) );
  std::memcpy( page.data() + sizeof( ph ), &fh, sizeof( fh ) );

  page_reader reader( page.data(), TEST_PAGE_SIZE );
  BOOST_CHECK( reader.more() );
  BOOST_CHECK_THROW( reader.next(), fc::exception );

  // a header that itself does not fit
  ph.used = 3;
  std::memcpy( page.data(), &ph, sizeof( ph ) );
  page_reader truncated( page.data(), TEST_PAGE_SIZE );
  BOOST_CHECK_THROW( truncated.next(), fc::exception );

  // a page header claiming more used bytes than the page holds is rejected
  // before anything past the page is touched
  ph.used = TEST_PAGE_SIZE - sizeof( page_header ) + 1;
  std::memcpy( page.data(), &ph, sizeof( ph ) );
  BOOST_CHECK_THROW( page_reader( page.data(), TEST_PAGE_SIZE ), fc::exception );
  ph.used = TEST_PAGE_SIZE - sizeof( page_header );
  std::memcpy( page.data(), &ph, sizeof( ph ) );
  BOOST_CHECK_NO_THROW( page_reader( page.data(), TEST_PAGE_SIZE ) );
}

BOOST_AUTO_TEST_CASE( decoder_rejects_short_match_payload )
{
  std::vector< std::byte > page( TEST_PAGE_SIZE );
  // a match frame claiming two rule ids but carrying only the coordinates
  page_header  ph{ 0, sizeof( frame_header ) + match_coords_size, 0 };
  frame_header fh{ match_coords_size, uint8_t( frame_kind::match ), 0, 2 };
  std::memcpy( page.data(), &ph, sizeof( ph ) );
  std::memcpy( page.data() + sizeof( ph ), &fh, sizeof( fh ) );

  page_reader reader( page.data(), TEST_PAGE_SIZE );
  frame_view f = reader.next();
  BOOST_CHECK_THROW( decode_match( f ), fc::exception );
}

BOOST_AUTO_TEST_SUITE_END()
#endif
