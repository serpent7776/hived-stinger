#pragma once

#include <fc/exception/exception.hpp>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace hive { namespace plugins { namespace stinger {

/**
 * Header of every page handed to the consumer, stamped by rotate() before
 * the page is published.
 */
struct page_header
{
  /// sequence number of the page; a debug tripwire for index-arithmetic bugs
  uint64_t page_index;
  /// number of frame bytes following this header
  uint32_t used;
  /// pads the header to 16 bytes; always stamped 0
  uint32_t reserved;
};
static_assert( sizeof( page_header ) == 16, "page header must be 16 bytes" );

/**
 * Single-producer / single-consumer transport of preallocated pages.
 *
 * The producer appends frames into its current page with plain memcpy and
 * pointer bumps (no lock, no atomics on the per-frame path), and hands a
 * filled page over in rotate() through a fixed-capacity ready ring guarded by
 * one std::mutex. When the ring is full the oldest page is evicted
 * (newest-wins), counted exactly at the drop site. The consumer takes the
 * front page and returns the previously taken one in a single lock
 * acquisition; a taken page is reachable from neither container, so ownership
 * transfer falls out of reachability and the page can be decoded in place, as
 * slowly as needed.
 *
 * N+2 pages circulate between four places: 1 held by the producer, up to 1
 * by the consumer, up to N in the ready ring, the rest on the free stack.
 * The conservation invariant
 *
 *   producer(1) + consumer(0..1) + |ready| + |free| == N + 2
 *
 * is the entire correctness story; the assert in rotate() is its tripwire
 * and a complete diagnostic for pool-accounting bugs - keep it.
 *
 * LOAD-BEARING INVARIANT: nothing that can block or allocate runs inside
 * the lock. Headers are stamped outside; the ready ring and the free stack
 * are preallocated fixed arrays - a std::deque/std::vector push can malloc
 * under the lock, and malloc takes its own lock, which is exactly the tail
 * being minimized. Decode, pack, and logging happen outside. std::mutex (a
 * futex: uncontended lock/unlock is one CAS each way, no syscall), never a
 * spinlock - a spinlock turns a preempted holder into a full quantum of
 * burned CPU for the waiter. Any future "just log the drop inside the lock"
 * patch is the regression to reject.
 *
 * Contract: exactly one producer thread calls append()/rotate() (pinned by
 * a debug-only assert on first append), exactly one consumer thread calls
 * take(), and the consumer holds at most one taken page at a time - the
 * conservation bound on the free stack depends on it, so take() enforces it
 * in every build.
 */
class page_buffer
{
public:
  /**
   * Allocate and prefault all N+2 pages up front. `page_size` is the bytes
   * per page, header included: a power of two, larger than the header and
   * small enough that its capacity fits page_header::used. `page_count` is
   * N, the capacity of the ready ring: a power of two, at least 2. Throws
   * fc::exception for any other geometry.
   */
  page_buffer( size_t page_size, size_t page_count )
    : _page_size( page_size ), _page_count( page_count )
  {
    FC_ASSERT( page_size > sizeof( page_header ) && ( page_size & ( page_size - 1 ) ) == 0,
               "page size must be a power of two larger than the page header" );
    FC_ASSERT( page_size - sizeof( page_header ) <= std::numeric_limits< uint32_t >::max(),
               "page size must not exceed 4 GiB" );
    FC_ASSERT( page_count >= 2 && ( page_count & ( page_count - 1 ) ) == 0,
               "page count must be a power of two, at least 2" );
    FC_ASSERT( page_count + 2 <= std::numeric_limits< size_t >::max() / page_size,
               "total size of the pages must fit size_t" );

    // All N+2 pages are slices of one allocation, zeroed by make_unique and
    // so prefaulted - the first lap takes no minor faults on the writer
    // thread - and pass through the free stack.
    _ready.resize( page_count, nullptr );
    _free.resize( page_count + 2, nullptr );
    _storage = std::make_unique< std::byte[] >( ( page_count + 2 ) * page_size );
    for( size_t i = 0; i < page_count + 2; ++i )
      _free[ i ] = _storage.get() + i * page_size;
    _free_count = page_count + 2;

    // wire page 0: the producer starts on the last free page
    _page.open( _free[ --_free_count ], _page_size );
    assert( pages_conserved() );
  }

  page_buffer( const page_buffer& ) = delete;
  page_buffer& operator=( const page_buffer& ) = delete;

  /// bytes per page, header included
  size_t page_size() const { return _page_size; }
  /// capacity of the ready ring (N); N+2 pages exist in total
  size_t page_count() const { return _page_count; }
  /// frame bytes one page can hold
  size_t capacity() const { return _page_size - sizeof( page_header ); }

  // ---- producer (single writer thread) ----

  /**
   * Append one frame to the current page, rotating first if it does not
   * fit. Takes the mutex only when it rotates. The length backstop is
   * unreachable when the serialization-side scratch bound is right; it
   * exists because a single rotate() followed by an unconditional memcpy is
   * a heap overflow the moment the two bounds drift apart. An empty frame
   * is rejected too: it would be counted but leave nothing in the page for
   * the consumer to find. Throwing here is safe by construction - the
   * plugin's handlers run inside HIVE_TRY_NOTIFY, which catches
   * fc::exception, elogs and continues.
   */
  void append( const void* frame, size_t len )
  {
    assert( on_writer_thread() );  // pins the single-producer assumption
    FC_ASSERT( len > 0, "frame must not be empty" );
    FC_ASSERT( len <= capacity(), "frame of ${len} bytes exceeds page capacity ${cap}",
               ( "len", len )( "cap", capacity() ) );
    if( len > _page.room() )
      rotate();
    std::memcpy( _page.write, frame, len );
    _page.write += len;
    ++_frames;                     // plain producer-private counters - no
    _bytes += len;                 // lock, no atomics on the per-frame path
  }

  /**
   * Close and publish the current page (page full, block boundary, or
   * shutdown flush) and take a fresh one. A no-op when the page is empty,
   * so a matchless block takes no lock at all.
   */
  void rotate()
  {
    if( _page.empty() )
      return;                                  // empty page: nothing to hand over

    page_header* h = reinterpret_cast< page_header* >( _page.begin );  // stamp OUTSIDE the lock
    h->page_index  = _cur;
    h->used        = uint32_t( _page.used() );
    h->reserved    = 0;

    std::byte* fresh;
    {
      std::lock_guard< std::mutex > g( _mx );
      if( _ready_count == _page_count )        // full: newest-wins, evict the oldest
      {
        _free[ _free_count++ ] = ready_pop_front();
        ++_pages_dropped;                      // exact accounting, at the drop site
      }
      ready_push_back( _page.begin );
      // The free stack cannot be empty here: we just pushed our page
      // (holding none), the eviction capped |ready| <= N, the consumer
      // holds at most 1, so by conservation |free| >= (N+2) - N - 1 = 1.
      assert( _free_count > 0 );
      fresh = _free[ --_free_count ];
      ++_pages_published;
      _frames_total = _frames;                 // fold the metrics, once per page
      _bytes_total  = _bytes;
      assert( pages_conserved() );
    }
    ++_cur;
    _page.open( fresh, _page_size );
  }

  // ---- consumer (single sink thread) ----

  /**
   * Return the oldest ready page, or nullptr if none is ready. `finished`
   * is the previously taken page (or nullptr), handed back into the free
   * stack - one lock acquisition per page covers both directions. The
   * returned page is exclusively the consumer's until handed back.
   *
   * `finished` must be exactly what the previous take() returned. Anything
   * else - a page handed back twice, a second page taken while one is
   * held, a foreign pointer - would break conservation and corrupt the
   * pool on the producer's side, so it throws fc::exception and leaves the
   * buffer untouched.
   */
  std::byte* take( std::byte* finished )
  {
    bool handed_back_held_page;
    std::byte* taken = nullptr;
    {
      std::lock_guard< std::mutex > g( _mx );
      handed_back_held_page = finished == _taken;
      if( handed_back_held_page )
      {
        if( finished )
          _free[ _free_count++ ] = finished;
        _taken = _ready_count == 0 ? nullptr : ready_pop_front();
        taken = _taken;
      }
      assert( pages_conserved() );
    }
    // checked under the lock, reported OUTSIDE it: a failing FC_ASSERT allocates
    FC_ASSERT( handed_back_held_page, "take() must be handed the page the previous take() returned" );
    return taken;
  }

  // ---- shutdown ----

  /**
   * Mark the stream ended. Set by the shutdown thread *after* its final
   * rotate() pushed the last page under the same mutex, so a consumer that
   * observes stopped() and then an empty take() knows the ring is drained
   * and nothing more can arrive.
   */
  void stop()
  {
    std::lock_guard< std::mutex > g( _mx );
    _stop = true;
  }

  /// true once stop() has been called
  bool stopped()
  {
    std::lock_guard< std::mutex > g( _mx );
    return _stop;
  }

  // ---- metrics ----

  struct stats_t
  {
    /// pages handed to the ready ring, including those later dropped
    uint64_t pages_published = 0;
    /// pages evicted from a full ready ring before the consumer took them
    uint64_t pages_dropped   = 0;
    /// frames appended, as of the last rotate() - the current partial page's
    /// tail is folded in when it is published
    uint64_t frames          = 0;
    /// frame bytes appended, as of the last rotate(), same as `frames`
    uint64_t bytes           = 0;
  };

  /// consistent snapshot of the counters, taken under the lock
  stats_t stats()
  {
    std::lock_guard< std::mutex > g( _mx );
    return { _pages_published, _pages_dropped, _frames_total, _bytes_total };
  }

private:
  /// The page the producer is filling: where it starts, where the next frame
  /// goes and where it ends.
  struct page
  {
    /// first byte of the page; the header is stamped here in rotate()
    std::byte* begin = nullptr;
    /// append cursor: the next free byte
    std::byte* write = nullptr;
    /// one past the last byte of the page
    std::byte* end   = nullptr;

    /// start filling `p`, a page of `size` bytes, right after its header
    void open( std::byte* p, size_t size )
    {
      begin = p;
      write = p + sizeof( page_header );
      end   = p + size;
    }

    /// true while no frame has been appended
    bool empty() const { return write == begin + sizeof( page_header ); }
    /// frame bytes appended so far
    size_t used() const { return size_t( write - begin ) - sizeof( page_header ); }
    /// frame bytes that still fit
    size_t room() const { return size_t( end - write ); }
  };

  /// debug-only SPSC guard: pins the first caller as the producer and
  /// returns whether the caller is that thread; always true under NDEBUG
  bool on_writer_thread()
  {
#ifndef NDEBUG
    const auto self = std::this_thread::get_id();
    if( _writer == std::thread::id() )
      _writer = self;                // pinned on first append
    return _writer == self;
#else
    return true;
#endif
  }

  /// remove and return the oldest ready page; caller holds _mx, ring not empty
  std::byte* ready_pop_front()
  {
    std::byte* p = _ready[ _ready_head ];
    _ready_head  = ( _ready_head + 1 ) & ( _page_count - 1 );
    --_ready_count;
    return p;
  }

  /// append a page as the newest ready one; caller holds _mx, ring not full
  void ready_push_back( std::byte* p )
  {
    _ready[ ( _ready_head + _ready_count ) & ( _page_count - 1 ) ] = p;
    ++_ready_count;
  }

  /// true if all N+2 pages are accounted for: ready, free, the one the
  /// producer is filling and the one the consumer may hold; caller holds _mx
  /// and the producer holds a page (not mid-rotate)
  bool pages_conserved() const
  {
    return _ready_count + _free_count + 1 + ( _taken ? 1 : 0 ) == _page_count + 2;
  }

  /// bytes per page, header included; a power of two
  const size_t _page_size;
  /// N: capacity of the ready ring; a power of two, so it masks ring indices
  const size_t _page_count;

  // producer-private, never touched by the consumer; on its own cache line

  /// the producer thread, pinned on first append; debug-only SPSC guard
  alignas( 64 ) std::thread::id _writer{};
  /// the page being filled
  page       _page;
  /// monotonic page index == pages published; stamped into the header
  uint64_t   _cur    = 0;
  /// frames appended since construction
  uint64_t   _frames = 0;
  /// frame bytes appended since construction
  uint64_t   _bytes  = 0;

  // everything below is touched only under _mx; critical sections are a
  // handful of pointer/index ops - nothing blocking, no allocation, ever

  /// guards every member below it
  alignas( 64 ) std::mutex _mx;
  /// fixed ring of N slots: filled pages awaiting the sink
  std::vector< std::byte* > _ready;
  /// slot of the oldest ready page
  size_t     _ready_head  = 0;
  /// number of ready pages, 0..N
  size_t     _ready_count = 0;
  /// fixed stack of N+2 slots: pages held by nobody; all pass through at construction
  std::vector< std::byte* > _free;
  /// number of pages on the free stack; the top is _free[ _free_count - 1 ]
  size_t     _free_count  = 0;
  /// the page the consumer holds, nullptr if none; what take() must get back
  std::byte* _taken = nullptr;
  /// pages handed to the ready ring, including those later dropped
  uint64_t   _pages_published = 0;
  /// pages evicted from a full ready ring before the consumer took them
  uint64_t   _pages_dropped   = 0;
  /// copy of _frames as of the last rotate(), readable by stats()
  uint64_t   _frames_total    = 0;
  /// copy of _bytes as of the last rotate(), readable by stats()
  uint64_t   _bytes_total     = 0;
  /// set by stop(): the stream has ended
  bool       _stop = false;

  /// owns the N+2 pages as one block; written at construction, immutable after
  std::unique_ptr< std::byte[] > _storage;
};

} } } // hive::plugins::stinger
