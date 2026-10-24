#pragma once

#include <hive/plugins/stinger/rules.hpp>

#include <fc/exception/exception.hpp>

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <utility>
#include <vector>

namespace hive { namespace plugins { namespace stinger {

/**
 * The bookkeeping of the two delivery streams, with no threading and no
 * encoding: the plugin owns the frames and the handoff to the sink, these
 * own the rules of what a block's frames are allowed to do - when a block
 * is sealed, released, dropped or retracted. Pure, so they are tested
 * without a node (tests/unit/plugin_tests/stinger_streams_tests.cpp).
 */

/// One batch of frames for one delivery class, back to back, as the writer
/// built them on its thread. For the irreversible class: a block's matches,
/// handed to the sink once the block is irreversible. For the head class: a
/// block's matches closed by its block_applied frame, handed over as the
/// block is applied, or a lone irreversible or switch_fork marker.
struct block_matches
{
  delivery_class           cls       = delivery_class::irreversible;
  /// a lone irreversible or switch_fork frame for the head stream; never
  /// dropped when the ready queue is full
  bool                     marker    = false;
  uint32_t                 block_num = 0;
  uint32_t                 frames    = 0;
  std::vector< std::byte > bytes;

  /// add one frame
  void append( const std::byte* frame, size_t len )
  {
    bytes.insert( bytes.end(), frame, frame + len );
    ++frames;
  }
};

/**
 * The irreversible stream's reversible window: the block being applied,
 * and the sealed blocks that are not irreversible yet, in ascending block
 * order. Frames go into current() as the block applies; seal() moves them
 * into the window at post_apply_block; release_up_to() hands them out at
 * irreversible_block; drop_from() forgets blocks a fork popped or the chain
 * rolled back.
 */
class reversible_window
{
public:
  /// the block being applied
  block_matches& current() { return _current; }
  /// the block being applied is not going to exist
  void discard_current() { _current = block_matches{}; }

  /// Seal the block being applied under `block_num`; a block with no frames
  /// leaves nothing to seal. The window is ascending: a sealed block at or
  /// above block_num must have been dropped first (see drop_from), so one
  /// still here is a logic error, not a chain event.
  void seal( uint32_t block_num )
  {
    FC_ASSERT( _pending.empty() || _pending.back().block_num < block_num,
      "block ${b} sealed while block ${p} is still pending", ("b", block_num)("p", _pending.back().block_num) );
    if( _current.frames != 0 )
    {
      _current.block_num = block_num;
      _pending.push_back( std::move( _current ) );
    }
    _current = block_matches{};
  }

  /// hand out every sealed block up to and including block_num, oldest first
  template< typename F >
  void release_up_to( uint32_t block_num, F&& f )
  {
    while( !_pending.empty() && _pending.front().block_num <= block_num )
    {
      f( std::move( _pending.front() ) );
      _pending.pop_front();
    }
  }

  struct dropped_t
  {
    size_t   blocks = 0;
    uint64_t frames = 0;
  };
  /// forget the sealed blocks at or above `first`; what was forgotten
  dropped_t drop_from( uint32_t first )
  {
    dropped_t d;
    while( !_pending.empty() && _pending.back().block_num >= first )
    {
      d.frames += _pending.back().frames;
      _pending.pop_back();
      ++d.blocks;
    }
    return d;
  }

  bool   empty() const { return _pending.empty(); }
  size_t size() const { return _pending.size(); }
  /// the lowest and highest sealed block; 0 when empty
  uint32_t first_block() const { return _pending.empty() ? 0 : _pending.front().block_num; }
  uint32_t last_block() const { return _pending.empty() ? 0 : _pending.back().block_num; }
  /// frames held in sealed blocks
  uint64_t frames() const
  {
    uint64_t n = 0;
    for( const auto& b : _pending )
      n += b.frames;
    return n;
  }

private:
  block_matches               _current;
  std::deque< block_matches > _pending;
};

/**
 * The head stream's bookkeeping: the block being applied and the last block
 * the stream was told about, which is what a retraction has to go below.
 * Nothing is emitted until the first block is closed, which keeps replay
 * and startup out of the stream; a stream that is not enabled (no head
 * rules) never emits anything. The plugin encodes the frames; this decides
 * whether a marker is due.
 */
class head_stream
{
public:
  explicit head_stream( bool enabled = false ) : _enabled( enabled ) {}

  /// false when no rule is delivered at head: the stream does not exist
  bool enabled() const { return _enabled; }
  /// the last block announced; 0 before the first
  uint32_t last_block() const { return _last_block; }

  /// the block being applied
  block_matches& current() { return _current; }
  /// the block being applied is not going to exist
  void discard_current() { _current = block_matches{ delivery_class::head }; }

  /// Close the block being applied under `block_num` with its block_applied
  /// frame and return the batch to hand over; the stream has now seen the
  /// block. Only for a validated block of an enabled stream.
  block_matches close( uint32_t block_num, const std::byte* block_applied, size_t len )
  {
    FC_ASSERT( _enabled, "head stream is not enabled" );
    FC_ASSERT( block_num > _last_block, "head block ${b} closed after block ${l}", ("b", block_num)("l", _last_block) );
    _current.append( block_applied, len );
    _current.block_num = block_num;
    _last_block        = block_num;
    block_matches b = std::move( _current );
    _current = block_matches{ delivery_class::head };
    return b;
  }

  /// true when an irreversible marker for block_num is due: the stream has
  /// started and this height was not marked yet (the chain re-notifies a
  /// height after a fork switch)
  bool irreversible( uint32_t block_num )
  {
    if( _last_block == 0 || block_num <= _last_irreversible )
      return false;
    _last_irreversible = block_num;
    return true;
  }

  /// the height a switch_fork marker must name when blocks the stream has
  /// seen are above `new_head`; none otherwise. The stream then stands at
  /// new_head
  std::optional< uint32_t > switch_fork( uint32_t new_head )
  {
    if( _last_block <= new_head )
      return std::nullopt;
    _last_block = new_head;
    return new_head;
  }

  /// retract block_num and everything above it: a switch_fork just below
  std::optional< uint32_t > retract_from( uint32_t block_num )
  {
    return block_num == 0 ? std::nullopt : switch_fork( block_num - 1 );
  }

  /// a batch holding one marker frame
  static block_matches marker( uint32_t block_num, const std::byte* frame, size_t len )
  {
    block_matches b{ delivery_class::head };
    b.marker    = true;
    b.block_num = block_num;
    b.append( frame, len );
    return b;
  }

private:
  bool          _enabled;
  block_matches _current{ delivery_class::head };
  uint32_t      _last_block        = 0;
  uint32_t      _last_irreversible = 0;
};

} } } // hive::plugins::stinger
