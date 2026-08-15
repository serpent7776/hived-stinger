#ifdef IS_TEST_NET
#include <boost/test/unit_test.hpp>

#include <hive/plugins/stinger/page_buffer.hpp>

#include <fc/exception/exception.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <thread>
#include <vector>

using hive::plugins::stinger::page_buffer;
using hive::plugins::stinger::page_header;

namespace {

constexpr size_t TEST_PAGE_SIZE  = 4096;
constexpr size_t TEST_PAGE_COUNT = 4;

/// test frame: header + deterministic payload so the consumer can verify
/// byte-exactness; the real frame format is a separate commit
struct frame_hdr
{
  uint32_t len;   // payload bytes following this header
  uint64_t seq;
};

std::byte payload_byte( uint64_t seq, uint32_t i )
{
  return std::byte( uint8_t( seq * 31 + i ) );
}

void append_frame( page_buffer& buf, uint64_t seq, uint32_t len )
{
  std::vector< std::byte > scratch( sizeof( frame_hdr ) + len );
  frame_hdr fh{ len, seq };
  std::memcpy( scratch.data(), &fh, sizeof( fh ) );
  for( uint32_t i = 0; i < len; ++i )
    scratch[ sizeof( fh ) + i ] = payload_byte( seq, i );
  buf.append( scratch.data(), scratch.size() );
}

struct decoded_frame
{
  uint64_t seq;
  uint32_t len;
  bool     byte_exact;
};

struct decoded_page
{
  uint64_t page_index;
  uint32_t used;
  std::vector< decoded_frame > frames;
};

decoded_page decode_page( const std::byte* p )
{
  decoded_page out;
  page_header h;
  std::memcpy( &h, p, sizeof( h ) );
  out.page_index = h.page_index;
  out.used       = h.used;
  const std::byte* cur = p + sizeof( page_header );
  const std::byte* end = cur + h.used;
  while( cur < end )
  {
    frame_hdr fh;
    std::memcpy( &fh, cur, sizeof( fh ) );
    cur += sizeof( fh );
    BOOST_REQUIRE( cur + fh.len <= end ); // encoder tripwire: frame walks off the page
    bool exact = true;
    for( uint32_t i = 0; i < fh.len; ++i )
      if( cur[ i ] != payload_byte( fh.seq, i ) )
        { exact = false; break; }
    out.frames.push_back( { fh.seq, fh.len, exact } );
    cur += fh.len;
  }
  return out;
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE( stinger_page_buffer )

BOOST_AUTO_TEST_CASE( invalid_geometry_rejected )
{
  BOOST_CHECK_THROW( page_buffer( 4095, 4 ), fc::exception );  // not a power of two
  BOOST_CHECK_THROW( page_buffer( 16, 4 ), fc::exception );    // no room past the header
  BOOST_CHECK_THROW( page_buffer( 4096, 3 ), fc::exception );  // not a power of two
  BOOST_CHECK_THROW( page_buffer( 4096, 1 ), fc::exception );  // below the minimum
  // capacity would not fit page_header::used; rejected before anything is allocated
  BOOST_CHECK_THROW( page_buffer( size_t( 1 ) << 33, 4 ), fc::exception );
}

BOOST_AUTO_TEST_CASE( single_page_fill_and_flush )
{
  page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );
  BOOST_CHECK_EQUAL( buf.capacity(), TEST_PAGE_SIZE - sizeof( page_header ) );

  append_frame( buf, 1, 100 );
  append_frame( buf, 2, 0 );
  append_frame( buf, 3, 250 );

  // nothing published until a rotation
  BOOST_CHECK( buf.take( nullptr ) == nullptr );
  BOOST_CHECK_EQUAL( buf.stats().pages_published, 0u );

  buf.rotate();
  std::byte* p = buf.take( nullptr );
  BOOST_REQUIRE( p != nullptr );
  decoded_page page = decode_page( p );
  BOOST_CHECK_EQUAL( page.page_index, 0u );
  BOOST_CHECK_EQUAL( page.used, 3 * sizeof( frame_hdr ) + 100 + 0 + 250 );
  BOOST_REQUIRE_EQUAL( page.frames.size(), 3u );
  for( size_t i = 0; i < 3; ++i )
  {
    BOOST_CHECK_EQUAL( page.frames[ i ].seq, i + 1 );
    BOOST_CHECK( page.frames[ i ].byte_exact );
  }

  page_buffer::stats_t st = buf.stats();
  BOOST_CHECK_EQUAL( st.pages_published, 1u );
  BOOST_CHECK_EQUAL( st.pages_dropped, 0u );
  BOOST_CHECK_EQUAL( st.frames, 3u );
  BOOST_CHECK_EQUAL( st.bytes, 3 * sizeof( frame_hdr ) + 350 );

  BOOST_CHECK( buf.take( p ) == nullptr );
}

BOOST_AUTO_TEST_CASE( empty_rotate_is_noop )
{
  page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );

  buf.rotate();
  buf.rotate();
  BOOST_CHECK_EQUAL( buf.stats().pages_published, 0u );
  BOOST_CHECK( buf.take( nullptr ) == nullptr );

  // still a no-op right after a flush emptied the page
  append_frame( buf, 1, 10 );
  buf.rotate();
  buf.rotate();
  BOOST_CHECK_EQUAL( buf.stats().pages_published, 1u );
}

BOOST_AUTO_TEST_CASE( frame_exactly_filling_capacity )
{
  page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );

  // one frame filling the page's *capacity* (page_size - 16, not page_size)
  const uint32_t len = uint32_t( buf.capacity() - sizeof( frame_hdr ) );
  append_frame( buf, 7, len );
  BOOST_CHECK_EQUAL( buf.stats().pages_published, 0u );  // fit without rotating

  // the next frame cannot fit and triggers the rotation
  append_frame( buf, 8, 10 );
  BOOST_CHECK_EQUAL( buf.stats().pages_published, 1u );

  std::byte* p = buf.take( nullptr );
  BOOST_REQUIRE( p != nullptr );
  decoded_page page = decode_page( p );
  BOOST_CHECK_EQUAL( page.used, buf.capacity() );
  BOOST_REQUIRE_EQUAL( page.frames.size(), 1u );
  BOOST_CHECK_EQUAL( page.frames[ 0 ].seq, 7u );
  BOOST_CHECK( page.frames[ 0 ].byte_exact );
  buf.take( p );
}

BOOST_AUTO_TEST_CASE( oversize_frame_rejected )
{
  page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );

  // one byte over capacity: the append backstop throws, the frame is
  // dropped and block application would continue (HIVE_TRY_NOTIFY)
  std::vector< std::byte > frame( buf.capacity() + 1, std::byte( 0xab ) );
  BOOST_CHECK_THROW( buf.append( frame.data(), frame.size() ), fc::exception );

  // the buffer stays usable
  append_frame( buf, 1, 10 );
  buf.rotate();
  std::byte* p = buf.take( nullptr );
  BOOST_REQUIRE( p != nullptr );
  decoded_page page = decode_page( p );
  BOOST_REQUIRE_EQUAL( page.frames.size(), 1u );
  BOOST_CHECK( page.frames[ 0 ].byte_exact );
  buf.take( p );
}

BOOST_AUTO_TEST_CASE( empty_frame_rejected )
{
  page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );

  // a frame of no bytes would be counted yet leave nothing in the page
  std::byte nothing{};
  BOOST_CHECK_THROW( buf.append( &nothing, 0 ), fc::exception );
  buf.rotate();
  BOOST_CHECK_EQUAL( buf.stats().pages_published, 0u );

  // the buffer stays usable and the rejected frame was not counted
  append_frame( buf, 1, 10 );
  buf.rotate();
  BOOST_CHECK_EQUAL( buf.stats().frames, 1u );
}

BOOST_AUTO_TEST_CASE( auto_rotation_on_page_full )
{
  page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );

  // append until the first automatic rotation
  const uint32_t len = 500;
  uint64_t seq = 0;
  while( buf.stats().pages_published == 0 )
    append_frame( buf, ++seq, len );
  buf.rotate();  // publish the second, partial page too

  std::byte* p = buf.take( nullptr );
  BOOST_REQUIRE( p != nullptr );
  decoded_page first = decode_page( p );
  BOOST_CHECK_EQUAL( first.page_index, 0u );
  BOOST_CHECK( first.used <= buf.capacity() );

  p = buf.take( p );
  BOOST_REQUIRE( p != nullptr );
  decoded_page second = decode_page( p );
  BOOST_CHECK_EQUAL( second.page_index, 1u );

  // every appended frame arrived exactly once, in order, byte-exact
  BOOST_REQUIRE_EQUAL( first.frames.size() + second.frames.size(), seq );
  uint64_t expected = 0;
  for( const decoded_page* page : { &first, &second } )
    for( const decoded_frame& f : page->frames )
    {
      BOOST_CHECK_EQUAL( f.seq, ++expected );
      BOOST_CHECK( f.byte_exact );
    }
  buf.take( p );
}

BOOST_AUTO_TEST_CASE( eviction_drops_oldest_exactly )
{
  page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );

  // publish N+3 pages without draining: 3 evictions, ring keeps the newest N
  const uint64_t total = TEST_PAGE_COUNT + 3;
  for( uint64_t i = 0; i < total; ++i )
  {
    append_frame( buf, i, 32 );
    buf.rotate();
  }

  page_buffer::stats_t st = buf.stats();
  BOOST_CHECK_EQUAL( st.pages_published, total );
  BOOST_CHECK_EQUAL( st.pages_dropped, 3u );

  // drain: precisely pages 3..6, in order
  std::byte* p = nullptr;
  for( uint64_t i = 3; i < total; ++i )
  {
    p = buf.take( p );
    BOOST_REQUIRE( p != nullptr );
    decoded_page page = decode_page( p );
    BOOST_CHECK_EQUAL( page.page_index, i );
    BOOST_REQUIRE_EQUAL( page.frames.size(), 1u );
    BOOST_CHECK_EQUAL( page.frames[ 0 ].seq, i );
    BOOST_CHECK( page.frames[ 0 ].byte_exact );
  }
  BOOST_CHECK( buf.take( p ) == nullptr );
}

BOOST_AUTO_TEST_CASE( conservation_with_full_ring_and_consumer_holding )
{
  page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );

  // fill the ready ring
  for( uint64_t i = 0; i < TEST_PAGE_COUNT; ++i )
  {
    append_frame( buf, i, 64 );
    buf.rotate();
  }

  // the consumer takes a page and holds it - the pool is now at its tightest:
  // producer 1, consumer 1, ready N-1, free 2
  std::byte* held = buf.take( nullptr );
  BOOST_REQUIRE( held != nullptr );
  decoded_page held_before = decode_page( held );
  BOOST_CHECK_EQUAL( held_before.page_index, 0u );

  // keep publishing without returning: the ring refills to N and every
  // further page evicts; the free stack must never empty (the assert in
  // rotate() is the tripwire - it caught a real N+1 pool-seeding bug)
  const uint64_t laps = 4 * ( TEST_PAGE_COUNT + 2 );
  for( uint64_t i = 0; i < laps; ++i )
  {
    append_frame( buf, TEST_PAGE_COUNT + i, 64 );
    buf.rotate();
  }

  page_buffer::stats_t st = buf.stats();
  BOOST_CHECK_EQUAL( st.pages_published, TEST_PAGE_COUNT + laps );
  // ring was left with N-1, refilled by 1, then every page evicted one
  BOOST_CHECK_EQUAL( st.pages_dropped, laps - 1 );

  // the held page was reachable from neither container the whole time
  decoded_page held_after = decode_page( held );
  BOOST_CHECK_EQUAL( held_after.page_index, 0u );
  BOOST_REQUIRE_EQUAL( held_after.frames.size(), 1u );
  BOOST_CHECK_EQUAL( held_after.frames[ 0 ].seq, 0u );
  BOOST_CHECK( held_after.frames[ 0 ].byte_exact );

  // accepted + dropped + still-in-ring == published, exactly
  uint64_t accepted = 1;  // the held page
  std::byte* p = buf.take( held );
  while( p )
  {
    ++accepted;
    p = buf.take( p );
  }
  BOOST_CHECK_EQUAL( accepted + st.pages_dropped, st.pages_published );
}

BOOST_AUTO_TEST_CASE( slow_consumer_holds_page_across_laps )
{
  page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );

  // publish a page with known content and take it
  append_frame( buf, 42, 1000 );
  buf.rotate();
  std::byte* held = buf.take( nullptr );
  BOOST_REQUIRE( held != nullptr );
  std::vector< std::byte > snapshot( held, held + TEST_PAGE_SIZE );

  // lap the pool many times over while the page is held: every other page
  // is published, evicted and reused again and again
  const uint64_t laps = 50 * ( TEST_PAGE_COUNT + 2 );
  for( uint64_t i = 0; i < laps; ++i )
  {
    append_frame( buf, 1000 + i, 700 );
    buf.rotate();
  }
  BOOST_CHECK_EQUAL( buf.stats().pages_dropped, laps - TEST_PAGE_COUNT );

  // ownership-by-reachability: the contents are still byte-exact
  BOOST_CHECK( std::memcmp( held, snapshot.data(), TEST_PAGE_SIZE ) == 0 );

  // once returned, the page circulates again: the producer refills it and
  // it comes back out of the ring
  std::byte* p = buf.take( held );
  uint64_t seen = 0;
  for( uint64_t i = 0; seen == 0 && i < 4 * ( TEST_PAGE_COUNT + 2 ); ++i )
  {
    append_frame( buf, 5000 + i, 10 );
    buf.rotate();
    p = buf.take( p );
    BOOST_REQUIRE( p != nullptr );
    if( p == held )
      ++seen;
  }
  BOOST_CHECK_EQUAL( seen, 1u );
}

BOOST_AUTO_TEST_CASE( take_rejects_anything_but_the_held_page )
{
  page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );

  for( uint64_t i = 0; i < 2; ++i )
  {
    append_frame( buf, i, 32 );
    buf.rotate();
  }
  std::byte* first = buf.take( nullptr );
  BOOST_REQUIRE( first != nullptr );

  // a second page while one is held would run the free stack dry
  BOOST_CHECK_THROW( buf.take( nullptr ), fc::exception );
  // a pointer the buffer never handed out
  BOOST_CHECK_THROW( buf.take( first + 1 ), fc::exception );

  // the rejected calls changed nothing: the held page is still accepted
  // and the next page is still there
  std::byte* second = buf.take( first );
  BOOST_REQUIRE( second != nullptr );
  BOOST_CHECK_EQUAL( decode_page( second ).page_index, 1u );
  BOOST_CHECK( buf.take( second ) == nullptr );

  // a page handed back twice would end up on the free stack twice
  BOOST_CHECK_THROW( buf.take( second ), fc::exception );
  BOOST_CHECK_THROW( buf.take( first ), fc::exception );
  BOOST_CHECK( buf.take( nullptr ) == nullptr );

  // the pool is intact: lap it with nothing held
  for( uint64_t i = 0; i < 3 * ( TEST_PAGE_COUNT + 2 ); ++i )
  {
    append_frame( buf, 100 + i, 32 );
    buf.rotate();
  }
  uint64_t taken = 0;
  for( std::byte* p = buf.take( nullptr ); p; p = buf.take( p ) )
    ++taken;
  BOOST_CHECK_EQUAL( taken, TEST_PAGE_COUNT );
}

BOOST_AUTO_TEST_CASE( two_thread_torture )
{
  constexpr size_t   page_size   = 4096;
  constexpr size_t   page_count  = 8;
  constexpr uint64_t frame_count = 1'000'000;

  page_buffer buf( page_size, page_count );

  uint64_t pages_accepted  = 0;
  uint64_t frames_accepted = 0;
  uint64_t seq_regressions = 0;  // must stay 0: accepted frames arrive in order
  uint64_t corrupt_frames  = 0;  // must stay 0: accepted pages decode byte-exact
  uint64_t index_regressions = 0;

  std::thread sink( [ & ]
  {
    uint64_t last_seq = 0;
    uint64_t last_index = 0;
    uint64_t backoff = 0;
    std::byte* page = nullptr;  // the page we hold, returned on the next take
    for( ;; )
    {
      // observe stop BEFORE the take: stop is set after the final rotate,
      // so stop-then-empty means nothing more can arrive
      const bool s = buf.stopped();
      std::byte* p = buf.take( page );
      page = p;
      if( p )
      {
        decoded_page dp = decode_page( p );
        if( dp.page_index < last_index )
          ++index_regressions;
        last_index = dp.page_index;
        for( const decoded_frame& f : dp.frames )
        {
          if( !f.byte_exact )
            ++corrupt_frames;
          if( f.seq <= last_seq && last_seq != 0 )
            ++seq_regressions;
          last_seq = f.seq;
          ++frames_accepted;
        }
        ++pages_accepted;
        // artificial stall every so often, to force laps and drops - the
        // loss path must actually run in this test
        if( ( pages_accepted & 0x3ff ) == 0 )
          std::this_thread::sleep_for( std::chrono::microseconds( 200 ) );
        backoff = 0;
        continue;
      }
      if( s )
        break;
      if( ++backoff > 64 )
        std::this_thread::yield();
    }
  } );

  std::thread producer( [ & ]
  {
    std::vector< std::byte > scratch( page_size );
    for( uint64_t seq = 1; seq <= frame_count; ++seq )
    {
      const uint32_t len = uint32_t( seq % 991 );
      frame_hdr fh{ len, seq };
      std::memcpy( scratch.data(), &fh, sizeof( fh ) );
      for( uint32_t i = 0; i < len; ++i )
        scratch[ sizeof( fh ) + i ] = payload_byte( seq, i );
      buf.append( scratch.data(), sizeof( fh ) + len );
      if( seq % 293 == 0 )
        buf.rotate();  // forced rotation, as at a block boundary
    }
    buf.rotate();      // shutdown flush: publish the final partial page
    buf.stop();
  } );

  producer.join();
  sink.join();

  page_buffer::stats_t st = buf.stats();
  BOOST_TEST_MESSAGE( "torture: published=" << st.pages_published
                      << " accepted=" << pages_accepted
                      << " dropped=" << st.pages_dropped
                      << " frames_accepted=" << frames_accepted );
  BOOST_CHECK_EQUAL( corrupt_frames, 0u );
  BOOST_CHECK_EQUAL( seq_regressions, 0u );
  BOOST_CHECK_EQUAL( index_regressions, 0u );
  BOOST_CHECK_EQUAL( st.frames, frame_count );
  // exact conservation - an equality, no quarantine caveat
  BOOST_CHECK_EQUAL( pages_accepted + st.pages_dropped, st.pages_published );
}

BOOST_AUTO_TEST_CASE( shutdown_drain )
{
  page_buffer buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );

  uint64_t pages_accepted = 0;
  uint64_t frames_accepted = 0;
  uint64_t last_seq = 0;

  std::thread sink( [ & ]
  {
    std::byte* page = nullptr;
    for( ;; )
    {
      const bool s = buf.stopped();
      std::byte* p = buf.take( page );
      page = p;
      if( p )
      {
        decoded_page dp = decode_page( p );
        for( const decoded_frame& f : dp.frames )
        {
          BOOST_CHECK( f.byte_exact );
          last_seq = f.seq;
          ++frames_accepted;
        }
        ++pages_accepted;
        continue;
      }
      if( s )
        break;
      std::this_thread::yield();
    }
  } );

  append_frame( buf, 1, 200 );
  append_frame( buf, 2, 200 );
  buf.rotate();  // the shutdown flush hands over the partial page...
  buf.stop();    // ...before stop, under the same mutex ordering
  sink.join();

  // the final partial page arrived and the sink exited
  BOOST_CHECK_EQUAL( pages_accepted, 1u );
  BOOST_CHECK_EQUAL( frames_accepted, 2u );
  BOOST_CHECK_EQUAL( last_seq, 2u );

  // stop with nothing pending: a fresh sink sees stop-then-empty and exits
  page_buffer empty_buf( TEST_PAGE_SIZE, TEST_PAGE_COUNT );
  empty_buf.rotate();  // empty flush is a no-op
  empty_buf.stop();
  std::thread empty_sink( [ & ]
  {
    std::byte* page = nullptr;
    for( ;; )
    {
      const bool s = empty_buf.stopped();
      std::byte* p = empty_buf.take( page );
      page = p;
      if( !p )
      {
        if( s ) break;
        std::this_thread::yield();
      }
    }
    BOOST_CHECK( page == nullptr );
  } );
  empty_sink.join();
}

BOOST_AUTO_TEST_SUITE_END()
#endif
