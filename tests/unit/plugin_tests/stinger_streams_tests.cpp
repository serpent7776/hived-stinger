#ifdef IS_TEST_NET
#include <boost/test/unit_test.hpp>

#include <hive/plugins/stinger/streams.hpp>

#include <fc/exception/exception.hpp>

#include <cstddef>
#include <string>
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

namespace {

/// a batch of `nbytes` bytes of class `cls` for block `block_num`
block_matches batch( delivery_class cls, uint32_t block_num, size_t nbytes )
{
  block_matches b;
  b.cls       = cls;
  b.block_num = block_num;
  b.bytes.assign( nbytes, std::byte{ 0x55 } );
  b.frames    = 1;
  return b;
}

/// what is queued, oldest first, as "<class><block>" for a batch, "M<block>"
/// for a marker and "D<class><first>-<last>x<count>" for a tombstone
std::vector< std::string > drain( ready_queue& q )
{
  std::vector< std::string > out;
  block_matches b;
  while( q.pop( b ) )
  {
    const char cls = b.cls == delivery_class::head ? 'H' : 'I';
    if( b.marker )
      out.push_back( "M" + std::to_string( b.block_num ) );
    else if( b.is_dropped() )
      out.push_back( std::string( "D" ) + cls + std::to_string( b.dropped.first_block ) + "-"
        + std::to_string( b.dropped.last_block ) + "x" + std::to_string( b.dropped.batches ) );
    else
      out.push_back( cls + std::to_string( b.block_num ) );
  }
  return out;
}

const auto irr  = delivery_class::irreversible;
const auto head = delivery_class::head;

} // anonymous namespace

BOOST_AUTO_TEST_SUITE( stinger_ready_queue )

BOOST_AUTO_TEST_CASE( queue_accounts_bytes_on_push_and_pop )
{
  ready_queue q( 1000 );
  BOOST_CHECK( q.empty() );
  BOOST_CHECK_EQUAL( q.bytes(), 0u );
  BOOST_CHECK_EQUAL( q.push( batch( irr, 1, 300 ) ), 0u );
  BOOST_CHECK_EQUAL( q.push( batch( head, 1, 400 ) ), 0u );
  BOOST_CHECK_EQUAL( q.size(), 2u );
  BOOST_CHECK_EQUAL( q.bytes(), 700u );
  block_matches b;
  BOOST_REQUIRE( q.pop( b ) );
  BOOST_CHECK_EQUAL( b.block_num, 1u );
  BOOST_CHECK( b.cls == irr );
  BOOST_CHECK( !b.is_dropped() );
  BOOST_CHECK_EQUAL( q.bytes(), 400u );
  BOOST_REQUIRE( q.pop( b ) );
  BOOST_CHECK( b.cls == head );
  BOOST_CHECK( !q.pop( b ) );
  BOOST_CHECK_EQUAL( q.bytes(), 0u );
  BOOST_CHECK_EQUAL( q.dropped(), 0u );
}

BOOST_AUTO_TEST_CASE( queue_over_budget_drops_the_oldest_matches_first_and_leaves_a_tombstone )
{
  ready_queue q( 1000 );
  q.push( batch( irr, 1, 400 ) );
  q.push( batch( irr, 2, 400 ) );
  // 1200 > 1000: block 1 goes, 800 fit
  BOOST_CHECK_EQUAL( q.push( batch( irr, 3, 400 ) ), 1u );
  BOOST_CHECK_EQUAL( q.bytes(), 800u );
  BOOST_CHECK_EQUAL( q.dropped(), 1u );
  BOOST_CHECK_EQUAL( q.size(), 3u );  // the tombstone stands where block 1 was
  // 1700 > 1000: blocks 2 and 3 go, into the same tombstone
  BOOST_CHECK_EQUAL( q.push( batch( irr, 4, 900 ) ), 2u );
  BOOST_CHECK_EQUAL( q.bytes(), 900u );
  BOOST_CHECK_EQUAL( q.dropped(), 3u );
  const std::vector< std::string > expected{ "DI1-3x3", "I4" };
  BOOST_CHECK( drain( q ) == expected );
}

BOOST_AUTO_TEST_CASE( queue_never_drops_the_batch_just_pushed )
{
  // one batch over the whole budget still goes through, and nothing is
  // dropped for it when it is alone
  ready_queue q( 1000 );
  BOOST_CHECK_EQUAL( q.push( batch( irr, 7, 5000 ) ), 0u );
  BOOST_CHECK_EQUAL( q.bytes(), 5000u );
  BOOST_CHECK_EQUAL( q.size(), 1u );
  // once something else is pushed behind it, it is what was waiting: it goes
  BOOST_CHECK_EQUAL( q.push( batch( irr, 8, 100 ) ), 1u );
  BOOST_CHECK_EQUAL( q.bytes(), 100u );
  // and what was waiting ahead of an oversized one goes for it, it stays
  BOOST_CHECK_EQUAL( q.push( batch( irr, 9, 5000 ) ), 1u );
  const std::vector< std::string > expected{ "DI7-8x2", "I9" };
  BOOST_CHECK( drain( q ) == expected );
}

BOOST_AUTO_TEST_CASE( queue_keeps_markers_and_tombstones_never_cross_them )
{
  ready_queue q( 1000 );
  q.push( head_stream::marker( 1, FRAME, sizeof( FRAME ) ) );
  q.push( batch( head, 2, 300 ) );
  q.push( batch( irr, 2, 300 ) );
  q.push( head_stream::marker( 2, FRAME, sizeof( FRAME ) ) );
  q.push( batch( head, 3, 300 ) );
  // 1208 with the markers: the oldest batch of matches goes, block 2's head
  // batch, and the marker ahead of it stays
  BOOST_CHECK_EQUAL( q.push( batch( irr, 3, 300 ) ), 1u );
  BOOST_CHECK_EQUAL( q.bytes(), 900u + 2 * sizeof( FRAME ) );
  // the next one takes block 2's irreversible batch: its own class, its own
  // tombstone beside the head one
  BOOST_CHECK_EQUAL( q.push( batch( irr, 4, 300 ) ), 1u );
  // and the next takes block 3's head batch, which is behind marker 2, so
  // it does not fold into block 2's head tombstone
  BOOST_CHECK_EQUAL( q.push( batch( head, 4, 300 ) ), 1u );
  BOOST_CHECK_EQUAL( q.bytes(), 900u + 2 * sizeof( FRAME ) );
  const std::vector< std::string > expected{ "M1", "DH2-2x1", "DI2-2x1", "M2", "DH3-3x1", "I3", "I4", "H4" };
  BOOST_CHECK( drain( q ) == expected );
  BOOST_CHECK_EQUAL( q.dropped(), 3u );
}

BOOST_AUTO_TEST_CASE( queue_folds_same_class_tombstones_past_the_other_class )
{
  // head and irreversible batches alternate; all of them dropped, the run
  // holds one tombstone per class, not one per batch
  ready_queue q( 1000 );
  for( uint32_t b = 1; b <= 4; ++b )
  {
    q.push( batch( head, b, 300 ) );
    q.push( batch( irr, b, 300 ) );
  }
  BOOST_CHECK_EQUAL( q.dropped(), 5u );   // 2400 pushed, 900 fit
  BOOST_CHECK_EQUAL( q.push( batch( head, 5, 300 ) ), 1u );
  BOOST_CHECK_EQUAL( q.push( batch( irr, 5, 300 ) ), 1u );
  const std::vector< std::string > expected{ "DH1-4x4", "DI1-3x3", "I4", "H5", "I5" };
  BOOST_CHECK( drain( q ) == expected );
}

BOOST_AUTO_TEST_CASE( queue_reports_a_drop_after_the_switch_fork_that_preceded_it )
{
  // a fork switch to block 99 is queued, then the new branch's blocks 100
  // and 101; a sink that stalled gets the retraction first, then the drop,
  // then block 102 - never the drop before the retraction
  ready_queue q( 1000 );
  q.push( head_stream::marker( 99, FRAME, sizeof( FRAME ) ) );
  q.push( batch( head, 100, 400 ) );
  q.push( batch( head, 101, 400 ) );
  BOOST_CHECK_EQUAL( q.push( batch( head, 102, 400 ) ), 1u );
  BOOST_CHECK_EQUAL( q.push( batch( head, 103, 400 ) ), 1u );
  const std::vector< std::string > expected{ "M99", "DH100-101x2", "H102", "H103" };
  BOOST_CHECK( drain( q ) == expected );
}

BOOST_AUTO_TEST_CASE( queue_evicts_for_a_marker_too_and_keeps_markers_when_nothing_else_is_left )
{
  ready_queue q( 1000 );
  q.push( batch( irr, 1, 600 ) );
  q.push( batch( irr, 2, 400 ) );
  // a marker that takes the queue over budget evicts the oldest matches
  BOOST_CHECK_EQUAL( q.push( head_stream::marker( 2, FRAME, sizeof( FRAME ) ) ), 1u );
  BOOST_CHECK_EQUAL( q.bytes(), 400u + sizeof( FRAME ) );

  // only markers left to drop: the queue stays over budget rather than lose one
  ready_queue m( 10 );
  m.push( head_stream::marker( 1, FRAME, sizeof( FRAME ) ) );
  m.push( head_stream::marker( 2, FRAME, sizeof( FRAME ) ) );
  BOOST_CHECK_EQUAL( m.push( head_stream::marker( 3, FRAME, sizeof( FRAME ) ) ), 0u );
  BOOST_CHECK_EQUAL( m.size(), 3u );
  BOOST_CHECK_EQUAL( m.dropped(), 0u );
}

BOOST_AUTO_TEST_CASE( queue_eviction_resumes_behind_what_was_popped )
{
  // pops ahead of the tombstone run, then more evictions: the scan start
  // follows the queue
  ready_queue q( 1000 );
  q.push( head_stream::marker( 1, FRAME, sizeof( FRAME ) ) );
  q.push( batch( irr, 1, 400 ) );
  q.push( batch( irr, 2, 400 ) );
  q.push( batch( irr, 3, 400 ) );   // drops 1
  block_matches b;
  BOOST_REQUIRE( q.pop( b ) );      // the marker
  BOOST_CHECK( b.marker );
  q.push( batch( irr, 4, 400 ) );   // drops 2, into block 1's tombstone
  BOOST_REQUIRE( q.pop( b ) );      // the tombstone
  BOOST_CHECK( b.is_dropped() );
  BOOST_CHECK_EQUAL( b.dropped.batches, 2u );
  q.push( batch( irr, 5, 400 ) );   // drops 3: a fresh tombstone at the front
  const std::vector< std::string > expected{ "DI3-3x1", "I4", "I5" };
  BOOST_CHECK( drain( q ) == expected );
  BOOST_CHECK_EQUAL( q.bytes(), 0u );
}

BOOST_AUTO_TEST_CASE( dropped_range_spans_what_it_was_given )
{
  dropped_range r;
  BOOST_CHECK_EQUAL( r.batches, 0u );
  r.add( 10 );
  BOOST_CHECK_EQUAL( r.first_block, 10u );
  BOOST_CHECK_EQUAL( r.last_block, 10u );
  r.add( 12 );
  r.add( 11 );
  BOOST_CHECK_EQUAL( r.batches, 3u );
  BOOST_CHECK_EQUAL( r.first_block, 10u );
  BOOST_CHECK_EQUAL( r.last_block, 12u );

  dropped_range other;
  other.add( 20 );
  other.merge( r );
  BOOST_CHECK_EQUAL( other.batches, 4u );
  BOOST_CHECK_EQUAL( other.first_block, 10u );
  BOOST_CHECK_EQUAL( other.last_block, 20u );
  dropped_range empty;
  empty.merge( other );
  BOOST_CHECK_EQUAL( empty.batches, 4u );
  BOOST_CHECK_EQUAL( empty.first_block, 10u );
  other.merge( dropped_range{} );
  BOOST_CHECK_EQUAL( other.batches, 4u );
}

BOOST_AUTO_TEST_SUITE_END()
#endif
