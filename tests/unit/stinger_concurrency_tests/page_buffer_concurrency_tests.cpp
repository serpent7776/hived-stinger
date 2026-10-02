// Concurrency tests of the stinger page buffer. They are a standalone
// executable - linking only fc, no chain libraries and no tcmalloc - so
// they can run under a race detector:
//
//   valgrind --tool=helgrind --fair-sched=yes ./stinger_page_buffer_concurrency_test
//   valgrind --tool=drd      --fair-sched=yes ./stinger_page_buffer_concurrency_test
//
//   ninja stinger_page_buffer_concurrency_test_tsan    (ThreadSanitizer build)
//   setarch $(uname -m) -R ./stinger_page_buffer_concurrency_test_tsan
//
// --fair-sched=yes matters: valgrind runs one thread at a time and by default
// lets the producer starve the sink, so little is interleaved. setarch -R
// turns off ASLR, without which TSAN fails to start on recent kernels with
// "unexpected memory mapping".
//
// Worker threads never use Boost.Test, whose assertions are not thread-safe
// and would show up in the detector's report: they only count what they see,
// and the main thread asserts on the counts after the join.
#define BOOST_TEST_MODULE stinger_page_buffer_concurrency
#include <boost/test/included/unit_test.hpp>

#include <hive/plugins/stinger/page_buffer.hpp>

#include <atomic>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

using hive::plugins::stinger::page_buffer;
using hive::plugins::stinger::page_header;

namespace {

constexpr size_t   PAGE_SIZE   = 4096;
constexpr uint32_t MAX_PAYLOAD = 200;

/// test frame: header + payload derived from the sequence number, so the
/// consumer can verify a frame knowing nothing but what it reads
struct frame_hdr
{
  uint64_t seq;
  uint32_t len;   // payload bytes following this header
  uint32_t pad;
};

/// The buffer under test lives on the heap: drd does not check accesses to
/// stack variables by default, and would miss a race on the buffer's members.
std::unique_ptr< page_buffer > make_buffer( size_t page_count )
{
  return std::make_unique< page_buffer >( PAGE_SIZE, page_count );
}

uint32_t payload_len( uint64_t seq )
{
  return uint32_t( seq % ( MAX_PAYLOAD + 1 ) );
}

std::byte payload_byte( uint64_t seq, uint32_t i )
{
  return std::byte( uint8_t( seq * 31 + i ) );
}

void append_frame( page_buffer& buf, uint64_t seq )
{
  std::byte scratch[ sizeof( frame_hdr ) + MAX_PAYLOAD ];
  const frame_hdr fh{ seq, payload_len( seq ), 0 };
  std::memcpy( scratch, &fh, sizeof( fh ) );
  for( uint32_t i = 0; i < fh.len; ++i )
    scratch[ sizeof( fh ) + i ] = payload_byte( seq, i );
  buf.append( scratch, sizeof( fh ) + fh.len );
}

/// what a sink thread saw; filled by that thread, read by the main thread
/// after the join. Sequence numbers start at 1.
struct sink_result
{
  uint64_t pages    = 0;
  uint64_t frames   = 0;
  uint64_t last_seq = 0;
  uint64_t last_index = 0;
  uint64_t malformed_pages   = 0;  // a frame walks off its page
  uint64_t corrupt_frames    = 0;  // payload does not match the sequence number
  uint64_t seq_regressions   = 0;  // frames out of order or repeated
  uint64_t seq_gaps          = 0;  // frames missing - legal only where pages were dropped
  uint64_t index_regressions = 0;  // pages out of order or repeated
};

/// decode a taken page in place and fold what it holds into `r`
void check_page( const std::byte* p, sink_result& r )
{
  page_header h;
  std::memcpy( &h, p, sizeof( h ) );
  if( r.pages != 0 && h.page_index <= r.last_index )
    ++r.index_regressions;
  r.last_index = h.page_index;
  ++r.pages;
  if( h.used > PAGE_SIZE - sizeof( page_header ) )
  {
    ++r.malformed_pages;
    return;
  }

  const std::byte* cur = p + sizeof( page_header );
  const std::byte* end = cur + h.used;
  while( cur < end )
  {
    frame_hdr fh;
    if( size_t( end - cur ) < sizeof( fh ) )
    {
      ++r.malformed_pages;
      return;
    }
    std::memcpy( &fh, cur, sizeof( fh ) );
    cur += sizeof( fh );
    if( fh.len != payload_len( fh.seq ) || size_t( end - cur ) < fh.len )
    {
      ++r.malformed_pages;
      return;
    }
    for( uint32_t i = 0; i < fh.len; ++i )
      if( cur[ i ] != payload_byte( fh.seq, i ) )
      {
        ++r.corrupt_frames;
        break;
      }
    if( fh.seq <= r.last_seq )
      ++r.seq_regressions;
    else if( fh.seq != r.last_seq + 1 )
      ++r.seq_gaps;
    r.last_seq = fh.seq;
    ++r.frames;
    cur += fh.len;
  }
}

/// sink thread body: take and check pages until the stream is stopped and
/// drained
void drain( page_buffer& buf, sink_result& r )
{
  std::byte* page = nullptr;  // the page we hold, returned on the next take
  for( ;; )
  {
    // observe stop BEFORE the take: stop is set after the final rotate, so
    // stop-then-empty means nothing more can arrive
    const bool stopped = buf.stopped();
    page = buf.take( page );
    if( page )
      check_page( page, r );
    else if( stopped )
      break;
    else
      std::this_thread::yield();
  }
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE( stinger_page_buffer_concurrency )

// Producer and sink run flat out against a small ring, so pages are handed
// over, evicted and recycled continuously. Whatever the interleaving, the
// sink must see intact frames in order and every page must be accounted for.
BOOST_AUTO_TEST_CASE( stream_stays_ordered_and_accounted )
{
  constexpr uint64_t frame_count = 200'000;

  const auto owner = make_buffer( 4 );
  page_buffer& buf = *owner;
  sink_result seen;

  std::thread sink( [ & ] { drain( buf, seen ); } );
  std::thread producer( [ & ]
  {
    for( uint64_t seq = 1; seq <= frame_count; ++seq )
    {
      append_frame( buf, seq );
      if( seq % 97 == 0 )
        buf.rotate();  // forced rotation, as at a block boundary
    }
    buf.rotate();      // shutdown flush: publish the final partial page
    buf.stop();
  } );
  producer.join();
  sink.join();

  const page_buffer::stats_t st = buf.stats();
  BOOST_TEST_MESSAGE( "stream: published=" << st.pages_published << " dropped=" << st.pages_dropped
                      << " frames_seen=" << seen.frames );
  BOOST_CHECK_EQUAL( seen.malformed_pages, 0u );
  BOOST_CHECK_EQUAL( seen.corrupt_frames, 0u );
  BOOST_CHECK_EQUAL( seen.seq_regressions, 0u );
  BOOST_CHECK_EQUAL( seen.index_regressions, 0u );
  BOOST_CHECK_EQUAL( st.frames, frame_count );
  // exact conservation: every published page was either taken or dropped
  BOOST_CHECK_EQUAL( seen.pages + st.pages_dropped, st.pages_published );
  // frames go missing only with a dropped page, and the last page is never
  // the one dropped
  BOOST_CHECK( seen.seq_gaps <= st.pages_dropped );
  BOOST_CHECK_EQUAL( seen.last_seq, frame_count );
  if( st.pages_dropped == 0 )
    BOOST_CHECK_EQUAL( seen.frames, frame_count );
}

// The sink holds each taken page while the producer laps the whole pool
// several times over. The producer must never write into a held page: a
// race detector reports the write, and the contents must not change.
BOOST_AUTO_TEST_CASE( held_page_is_never_overwritten )
{
  constexpr size_t page_count = 4;
  constexpr int    rounds     = 50;

  const auto owner = make_buffer( page_count );
  page_buffer& buf = *owner;
  std::atomic< bool > done{ false };
  uint64_t changed_pages = 0;

  std::thread producer( [ & ]
  {
    for( uint64_t seq = 1; !done.load(); ++seq )
    {
      append_frame( buf, seq );
      if( seq % 8 == 0 )
      {
        buf.rotate();
        // this loop ends only when the sink gets to run, and valgrind's
        // default scheduler lets a thread that never blocks starve the rest
        std::this_thread::yield();
      }
    }
  } );
  std::thread sink( [ & ]
  {
    std::vector< std::byte > snapshot( PAGE_SIZE );
    std::byte* held = nullptr;
    for( int round = 0; round < rounds; ++round )
    {
      while( ( held = buf.take( held ) ) == nullptr )
        std::this_thread::yield();
      std::memcpy( snapshot.data(), held, PAGE_SIZE );

      const uint64_t target = buf.stats().pages_published + 4 * ( page_count + 2 );
      while( buf.stats().pages_published < target )
        std::this_thread::yield();

      if( std::memcmp( held, snapshot.data(), PAGE_SIZE ) != 0 )
        ++changed_pages;
    }
    done = true;
  } );
  sink.join();
  producer.join();

  BOOST_CHECK_EQUAL( changed_pages, 0u );
  // each round outran the ring by far more than its capacity, so the
  // eviction path did run
  BOOST_CHECK( buf.stats().pages_dropped >= uint64_t( rounds ) * page_count );
}

// A third thread reads stats() while the stream runs. Each snapshot is taken
// under the lock, so it must be internally consistent and never go backwards.
BOOST_AUTO_TEST_CASE( stats_snapshots_are_consistent )
{
  constexpr uint64_t frame_count = 50'000;

  const auto owner = make_buffer( 4 );
  page_buffer& buf = *owner;
  sink_result seen;
  std::atomic< bool > done{ false };
  uint64_t snapshots     = 0;
  uint64_t regressions   = 0;  // a counter went backwards
  uint64_t inconsistent  = 0;  // counters of one snapshot contradict each other

  std::thread observer( [ & ]
  {
    page_buffer::stats_t prev;
    while( !done.load() )
    {
      const page_buffer::stats_t st = buf.stats();
      if( st.pages_published < prev.pages_published || st.pages_dropped < prev.pages_dropped ||
          st.frames < prev.frames || st.bytes < prev.bytes )
        ++regressions;
      if( st.pages_dropped > st.pages_published || st.frames > frame_count ||
          st.bytes < st.frames * sizeof( frame_hdr ) ||
          st.bytes > st.frames * ( sizeof( frame_hdr ) + MAX_PAYLOAD ) )
        ++inconsistent;
      prev = st;
      ++snapshots;
      std::this_thread::yield();
    }
  } );
  std::thread sink( [ & ] { drain( buf, seen ); } );
  std::thread producer( [ & ]
  {
    for( uint64_t seq = 1; seq <= frame_count; ++seq )
    {
      append_frame( buf, seq );
      if( seq % 31 == 0 )
        buf.rotate();
    }
    buf.rotate();
    buf.stop();
  } );
  producer.join();
  sink.join();
  done = true;
  observer.join();

  BOOST_TEST_MESSAGE( "stats: snapshots=" << snapshots );
  BOOST_CHECK_EQUAL( regressions, 0u );
  BOOST_CHECK_EQUAL( inconsistent, 0u );
  BOOST_CHECK_EQUAL( seen.malformed_pages, 0u );
  BOOST_CHECK_EQUAL( seen.corrupt_frames, 0u );
}

// The shape of a node shutdown: the writer thread stops appending and exits
// holding a partial page, and a different thread then does the final
// rotate() and stop(). The join orders that thread after the writer, so it
// may touch the producer's state. Each round is small enough that the ring
// is never overrun, so every frame must arrive exactly once.
BOOST_AUTO_TEST_CASE( shutdown_flush_from_another_thread_delivers_everything )
{
  constexpr size_t page_count = 8;

  for( uint64_t round = 0; round < 50; ++round )
  {
    const uint64_t frame_count = 1 + round * 3;

    const auto owner = make_buffer( page_count );
    page_buffer& buf = *owner;
    sink_result seen;

    std::thread sink( [ & ] { drain( buf, seen ); } );
    std::thread writer( [ & ]
    {
      for( uint64_t seq = 1; seq <= frame_count; ++seq )
        append_frame( buf, seq );
    } );
    writer.join();
    buf.rotate();  // the shutdown flush hands over the partial page...
    buf.stop();    // ...before stop, under the same mutex ordering
    sink.join();

    const page_buffer::stats_t st = buf.stats();
    BOOST_REQUIRE( st.pages_published <= page_count );  // the round did fit the ring
    BOOST_CHECK_EQUAL( st.pages_dropped, 0u );
    BOOST_CHECK_EQUAL( seen.pages, st.pages_published );
    BOOST_CHECK_EQUAL( seen.frames, frame_count );
    BOOST_CHECK_EQUAL( seen.last_seq, frame_count );
    BOOST_CHECK_EQUAL( seen.seq_gaps, 0u );
    BOOST_CHECK_EQUAL( seen.seq_regressions, 0u );
    BOOST_CHECK_EQUAL( seen.index_regressions, 0u );
    BOOST_CHECK_EQUAL( seen.malformed_pages, 0u );
    BOOST_CHECK_EQUAL( seen.corrupt_frames, 0u );
  }
}

BOOST_AUTO_TEST_SUITE_END()
