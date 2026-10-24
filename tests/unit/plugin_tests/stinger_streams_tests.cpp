#ifdef IS_TEST_NET
#include <boost/test/unit_test.hpp>

#include <hive/plugins/stinger/streams.hpp>

#include <fc/exception/exception.hpp>

#include <cstddef>
#include <vector>

using namespace hive::plugins::stinger;

namespace {

/// some frame bytes; the streams never look inside a frame
const std::byte FRAME[ 4 ] = { std::byte{ 0xde }, std::byte{ 0xad }, std::byte{ 0xbe }, std::byte{ 0xef } };

void add_frames( block_matches& b, uint32_t n )
{
  for( uint32_t i = 0; i < n; ++i )
    b.append( FRAME, sizeof( FRAME ) );
}

/// release_up_to's blocks, in the order handed out
std::vector< block_matches > release( reversible_window& w, uint32_t up_to )
{
  std::vector< block_matches > out;
  w.release_up_to( up_to, [&]( block_matches&& b ) { out.push_back( std::move( b ) ); } );
  return out;
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE( stinger_streams )

BOOST_AUTO_TEST_CASE( block_matches_append_counts_frames_and_bytes )
{
  block_matches b;
  BOOST_CHECK( b.cls == delivery_class::irreversible );
  BOOST_CHECK( !b.marker );
  add_frames( b, 3 );
  BOOST_CHECK_EQUAL( b.frames, 3u );
  BOOST_CHECK_EQUAL( b.bytes.size(), 3 * sizeof( FRAME ) );
}

BOOST_AUTO_TEST_CASE( window_seals_and_releases_in_block_order )
{
  reversible_window w;
  BOOST_CHECK( w.empty() );
  BOOST_CHECK_EQUAL( w.last_block(), 0u );

  add_frames( w.current(), 2 );
  w.seal( 10 );
  w.seal( 11 );               // no frames: nothing sealed
  add_frames( w.current(), 3 );
  w.seal( 12 );
  BOOST_CHECK_EQUAL( w.size(), 2u );
  BOOST_CHECK_EQUAL( w.first_block(), 10u );
  BOOST_CHECK_EQUAL( w.last_block(), 12u );
  BOOST_CHECK_EQUAL( w.frames(), 5u );
  BOOST_CHECK_EQUAL( w.current().frames, 0u );

  auto out = release( w, 9 );
  BOOST_CHECK( out.empty() );
  out = release( w, 11 );
  BOOST_REQUIRE_EQUAL( out.size(), 1u );
  BOOST_CHECK_EQUAL( out[ 0 ].block_num, 10u );
  BOOST_CHECK_EQUAL( out[ 0 ].frames, 2u );
  BOOST_CHECK( out[ 0 ].cls == delivery_class::irreversible );
  BOOST_CHECK_EQUAL( w.first_block(), 12u );
  out = release( w, 100 );
  BOOST_REQUIRE_EQUAL( out.size(), 1u );
  BOOST_CHECK_EQUAL( out[ 0 ].block_num, 12u );
  BOOST_CHECK( w.empty() );
}

BOOST_AUTO_TEST_CASE( window_drops_from_the_top )
{
  reversible_window w;
  for( uint32_t b = 5; b <= 8; ++b )
  {
    add_frames( w.current(), b );
    w.seal( b );
  }
  // a fork to head 6 pops 7 and 8
  auto d = w.drop_from( 7 );
  BOOST_CHECK_EQUAL( d.blocks, 2u );
  BOOST_CHECK_EQUAL( d.frames, 7u + 8u );
  BOOST_CHECK_EQUAL( w.last_block(), 6u );
  // nothing above 6: no-op
  d = w.drop_from( 7 );
  BOOST_CHECK_EQUAL( d.blocks, 0u );
  // everything
  d = w.drop_from( 0 );
  BOOST_CHECK_EQUAL( d.blocks, 2u );
  BOOST_CHECK( w.empty() );
}

BOOST_AUTO_TEST_CASE( window_rejects_sealing_out_of_order )
{
  reversible_window w;
  add_frames( w.current(), 1 );
  w.seal( 5 );
  add_frames( w.current(), 1 );
  BOOST_CHECK_THROW( w.seal( 5 ), fc::exception );
  BOOST_CHECK_THROW( w.seal( 4 ), fc::exception );
  // the window is unchanged and the block can still be sealed in order
  BOOST_CHECK_EQUAL( w.size(), 1u );
  w.seal( 6 );
  BOOST_CHECK_EQUAL( w.last_block(), 6u );
}

BOOST_AUTO_TEST_CASE( window_discards_the_block_being_applied )
{
  reversible_window w;
  add_frames( w.current(), 4 );
  w.discard_current();
  BOOST_CHECK_EQUAL( w.current().frames, 0u );
  BOOST_CHECK( w.current().bytes.empty() );
  w.seal( 1 );
  BOOST_CHECK( w.empty() );
}

BOOST_AUTO_TEST_CASE( disabled_head_stream_emits_nothing )
{
  head_stream h;
  BOOST_CHECK( !h.enabled() );
  BOOST_CHECK_THROW( h.close( 1, FRAME, sizeof( FRAME ) ), fc::exception );
  BOOST_CHECK( !h.irreversible( 1 ) );
  BOOST_CHECK( !h.switch_fork( 0 ) );
  BOOST_CHECK( !h.retract_from( 1 ) );
}

BOOST_AUTO_TEST_CASE( head_stream_closes_blocks_in_order )
{
  head_stream h( true );
  BOOST_CHECK_EQUAL( h.last_block(), 0u );
  add_frames( h.current(), 2 );
  block_matches b = h.close( 7, FRAME, sizeof( FRAME ) );
  BOOST_CHECK( b.cls == delivery_class::head );
  BOOST_CHECK( !b.marker );
  BOOST_CHECK_EQUAL( b.block_num, 7u );
  BOOST_CHECK_EQUAL( b.frames, 3u );          // two matches and the block_applied frame
  BOOST_CHECK_EQUAL( h.last_block(), 7u );
  BOOST_CHECK_EQUAL( h.current().frames, 0u );
  // a block without matches still closes: the marker is the stream's clock
  b = h.close( 8, FRAME, sizeof( FRAME ) );
  BOOST_CHECK_EQUAL( b.frames, 1u );
  BOOST_CHECK_THROW( h.close( 8, FRAME, sizeof( FRAME ) ), fc::exception );
  BOOST_CHECK_THROW( h.close( 3, FRAME, sizeof( FRAME ) ), fc::exception );
}

BOOST_AUTO_TEST_CASE( head_stream_marks_irreversible_once_per_height_after_start )
{
  head_stream h( true );
  // nothing announced yet: replay and startup stay silent
  BOOST_CHECK( !h.irreversible( 3 ) );
  h.close( 5, FRAME, sizeof( FRAME ) );
  BOOST_CHECK( h.irreversible( 3 ) );
  BOOST_CHECK( !h.irreversible( 3 ) );   // re-notified after a fork switch
  BOOST_CHECK( !h.irreversible( 2 ) );
  BOOST_CHECK( h.irreversible( 5 ) );
}

BOOST_AUTO_TEST_CASE( head_stream_retracts_only_what_it_announced )
{
  head_stream h( true );
  for( uint32_t b = 1; b <= 10; ++b )
    h.close( b, FRAME, sizeof( FRAME ) );

  // a fork to a height the stream has not passed: nothing to say
  BOOST_CHECK( !h.switch_fork( 10 ) );
  BOOST_CHECK( !h.switch_fork( 12 ) );
  BOOST_CHECK_EQUAL( h.last_block(), 10u );
  // a fork below: name the new head, stand there
  auto marker = h.switch_fork( 7 );
  BOOST_REQUIRE( marker );
  BOOST_CHECK_EQUAL( *marker, 7u );
  BOOST_CHECK_EQUAL( h.last_block(), 7u );
  // the new branch is announced from 8 again
  BOOST_CHECK_THROW( h.close( 7, FRAME, sizeof( FRAME ) ), fc::exception );
  h.close( 8, FRAME, sizeof( FRAME ) );

  // a block rolled back after it was announced: switch_fork just below it
  marker = h.retract_from( 8 );
  BOOST_REQUIRE( marker );
  BOOST_CHECK_EQUAL( *marker, 7u );
  BOOST_CHECK( !h.retract_from( 8 ) );    // already below
  BOOST_CHECK( !h.retract_from( 0 ) );
}

BOOST_AUTO_TEST_CASE( head_stream_marker_batch )
{
  block_matches m = head_stream::marker( 42, FRAME, sizeof( FRAME ) );
  BOOST_CHECK( m.cls == delivery_class::head );
  BOOST_CHECK( m.marker );
  BOOST_CHECK_EQUAL( m.block_num, 42u );
  BOOST_CHECK_EQUAL( m.frames, 1u );
  BOOST_CHECK_EQUAL( m.bytes.size(), sizeof( FRAME ) );
}

BOOST_AUTO_TEST_CASE( block_announced_then_rolled_back )
{
  // the plugin's retract_from: a later handler threw after post_apply_block,
  // so both streams were told about block 10 before it was undone
  reversible_window w;
  head_stream       h( true );
  add_frames( w.current(), 1 );
  add_frames( h.current(), 1 );
  w.seal( 10 );
  h.close( 10, FRAME, sizeof( FRAME ) );

  BOOST_CHECK_EQUAL( w.drop_from( 10 ).blocks, 1u );
  auto marker = h.retract_from( 10 );
  BOOST_REQUIRE( marker );
  BOOST_CHECK_EQUAL( *marker, 9u );

  // the real block 10 then seals and closes without complaint
  add_frames( w.current(), 2 );
  w.seal( 10 );
  BOOST_CHECK_EQUAL( w.last_block(), 10u );
  BOOST_CHECK_EQUAL( h.close( 10, FRAME, sizeof( FRAME ) ).block_num, 10u );
}

BOOST_AUTO_TEST_SUITE_END()
#endif
