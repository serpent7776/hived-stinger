#include <hive/chain/hive_fwd.hpp>

#include <hive/plugins/stinger/frame.hpp>
#include <hive/plugins/stinger/rules.hpp>
#include <hive/plugins/stinger/stinger_plugin.hpp>
#include <hive/plugins/stinger/streams.hpp>

#include <hive/chain/database.hpp>
#include <hive/chain/notifications.hpp>

#include <hive/protocol/asset.hpp>
#include <hive/protocol/forward_impacted.hpp>
#include <hive/protocol/operations.hpp>

#include <hive/utilities/signal.hpp>

#include <fc/thread/thread.hpp>

extern "C" {
#include <betree.h>
}

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#define STINGER_RULES_FILE_KEY "stinger-rules-file"

namespace hive { namespace plugins { namespace stinger {

using namespace hive::chain;

namespace detail {

constexpr const char* STINGER_ATTR_OP_TYPE           = "op_type";
constexpr const char* STINGER_ATTR_IMPACTED_ACCOUNTS = "impacted_accounts";
constexpr const char* STINGER_ATTR_AMOUNT            = "amount";
constexpr const char* STINGER_ATTR_SYMBOL            = "symbol";
constexpr const char* STINGER_ATTR_CUSTOM_ID         = "custom_id";

/// The largest match frame the encoder builds: the block size ceiling, which
/// bounds any operation in a block, plus the overhead of a frame carrying as
/// many rule ids as the header can count. So a body only ever fails to fit
/// (and is left out, flagged) for a virtual operation beyond anything the
/// chain generates, however many rules match it. Allocated once, on the
/// writer thread.
constexpr size_t STINGER_FRAME_BUDGET =
  size_t( HIVE_MAX_BLOCK_SIZE ) + match_frame_overhead( std::numeric_limits< uint16_t >::max() );

/// Batches the writer holds for the sink before dropping the oldest batch
/// of matches: the writer never waits for the sink. Head-stream markers are
/// never dropped - a lost switch_fork would leave the consumer holding
/// retracted blocks as valid - and are small, so they may push the queue
/// past this while a sink stalls. A block costs one batch per class in use
/// plus one for its irreversible marker, so with both classes this is about
/// 17 minutes of sink outage at one block per 3 seconds; config knob later.
constexpr size_t STINGER_READY_BATCHES_MAX = 1024;

/// drops are reported at most this often
constexpr std::chrono::seconds DROP_LOG_INTERVAL{ 10 };


/// be-tree event variable ids as returned by betree_add_*_variable, used as betree_set_variable slots
struct event_slots
{
  size_t op_type;
  size_t impacted_accounts;
  size_t amount;
  size_t symbol;
  size_t custom_id;
};

template< typename Op, typename = void >
struct has_asset_amount : std::false_type {};
template< typename Op >
struct has_asset_amount< Op,
  std::enable_if_t< std::is_same_v< decltype( Op::amount ), protocol::asset > > > : std::true_type {};

/// pulls the per-op-type event attributes out of an operation
struct event_attribute_visitor
{
  typedef void result_type;

  std::optional< protocol::asset >& amount;
  std::optional< std::string >&     custom_id;

  void operator()( const protocol::custom_json_operation& op ) const
  {
    custom_id = std::string( op.id );
  }

  template< typename Op >
  void operator()( const Op& op ) const
  {
    if constexpr( has_asset_amount< Op >::value )
      amount = op.amount;
  }
};

/**
 * Matches each operation against the be-tree rule index and delivers the
 * matches in one of two streams, chosen per rule (rules.hpp):
 *
 * Irreversible, the default: a block's matches are delivered once the
 * block is irreversible, so the consumer never sees a match that can be
 * undone and the stream carries match frames only (frame.hpp). The
 * reversible window is the plugin's (reversible_window, streams.hpp):
 * frames accumulate in the block being applied, are sealed at
 * post_apply_block, dropped on fail_apply_block (the block was rolled back
 * mid-application; its leading transactions had already matched) and on
 * switch_fork (everything above the new head), and released at
 * irreversible_block.
 * With one-block irreversibility the window is normally one block deep.
 *
 * Head: a block's matches are delivered as the block is applied, closed by
 * a block_applied frame, so the consumer sees them a block or two before
 * the irreversible stream would show them and must be able to retract. The
 * stream (head_stream, streams.hpp) carries switch_fork when blocks it has
 * seen are popped and irreversible when its retraction state for a height
 * can go. A block that
 * fails before post_apply_block never reaches this stream; one rolled back
 * after it - a later plugin's handler threw, or state migration did - is
 * retracted by a switch_fork below it from fail_apply_block, or, if the
 * chain undid it without a signal, when the next block at that height is
 * sealed. Nothing is emitted until the first validated block, which keeps
 * replay and startup out of the stream. A rule file without head rules
 * produces none of this.
 *
 * An operation matched by rules of both classes puts the same frame, with
 * every matched rule id, in both batches; the sink reports to each stream
 * only the rules of that class.
 *
 * All writer state is touched under _writer_mutex only: by the writer
 * thread, and by the main thread for the irreversible and fork signals the
 * chain fires during startup and replay, when it is empty. The one
 * cross-thread point to the sink is _ready, a mutex-guarded queue of
 * batches it waits on, in the writer's order, so each stream is in order.
 * The writer never blocks on the sink: a full queue drops the oldest batch,
 * counted and warned about.
 *
 * Matches held at shutdown - blocks applied but not yet irreversible - are
 * dropped and their block numbers logged: a restart replays those blocks
 * with matching off, so they are never delivered. Normally zero or one
 * block.
 */
class stinger_plugin_impl
{
public:
  stinger_plugin_impl( appbase::application& app ) :
    _chain_plugin( app.get_plugin< hive::plugins::chain::chain_plugin >() ),
    _db( _chain_plugin.db() ) {}
  /// stops the sink if plugin_shutdown never ran (initialized but never
  /// started, or startup aborted): a joinable std::thread must not be destroyed
  ~stinger_plugin_impl() { stop_sink(); }

  void on_post_apply_operation( const operation_notification& note );
  void on_post_apply_block( const block_notification& note );
  void on_fail_apply_block( const block_notification& note );
  void on_irreversible_block( uint32_t block_num );
  void on_switch_fork( uint32_t new_head_block_num );

  /// both streams forget blocks at or above block_num: pending blocks are
  /// dropped and the head stream gets a switch_fork below it
  void retract_from( uint32_t block_num );
  /// hand a batch to the sink; the writer's side of _ready
  void push_ready( block_matches&& b );

  /// the sink thread's body
  void sink_loop();
  /// deliver every frame of one batch to its class's stream
  void deliver_block( const block_matches& b );
  /// deliver a match to one class's stream: the matched rules of that class
  void deliver_match( const match_frame& m, delivery_class cls );
  /// stop producing, drop what is not irreversible, stop and join the sink; idempotent
  void stop_sink();

  plugins::chain::chain_plugin& _chain_plugin;
  chain::database&              _db;
  fc::path                      rules_file;
  rules_config                  rules;

  /// be-tree subscription index; built once in plugin_initialize - replay
  /// delivers operation notifications during the chain plugin's startup,
  /// before this plugin's plugin_startup runs
  std::unique_ptr< betree, decltype( &betree_free ) > _matcher{ nullptr, &betree_free };

  /// event variable indices assigned by declare_rule_attributes for _matcher
  event_slots _slots{};

  /// subscription id -> rule, for a rule's delivery class on the writer
  /// thread and its name on the sink thread; built in plugin_initialize
  /// together with the index and never written again
  std::map< uint64_t, const rule_definition* > _rules_by_id;

  /// the writer thread's frame scratch
  std::unique_ptr< frame_encoder > _encoder;

  /// writer-private: the two streams' bookkeeping (streams.hpp). The head
  /// stream is enabled in plugin_initialize when a rule asks for it
  reversible_window _window;
  head_stream       _head;
  /// writer-private counters, folded into the shutdown report
  uint64_t _batches_released = 0;
  uint64_t _frames_released  = 0;
  uint64_t _batches_dropped  = 0;
  /// _batches_dropped as of the last drop warning, and when it was logged
  uint64_t                              _drops_logged = 0;
  std::chrono::steady_clock::time_point _drop_log_time{};

  /// the release point: batches the sink has not taken yet, in the
  /// writer's order. _ready_stopped is set by stop_sink() after the final push
  std::mutex                  _ready_mutex;
  std::condition_variable     _ready_cv;
  std::deque< block_matches > _ready;
  bool                        _ready_stopped = false;
  /// the sink thread; a plain std::thread, not scheduled against appbase
  std::thread                      _sink;

  /// Serializes the signal handlers against stop_sink() on the shutdown
  /// thread. signals2's disconnect() stops new invocations but does not
  /// wait for one already running, and a handler may be mid-way through
  /// the window when stop_sink() wants to report it. A handler holds the lock
  /// for its whole body; stop_sink() takes it to set _stopping, which waits
  /// out a running handler, and every handler entered afterwards returns at
  /// once. Uncontended on every call but that one, so it costs nothing next
  /// to the be-tree search, and it also covers the irreversible and fork
  /// signals the chain fires from the main thread during startup.
  std::mutex _writer_mutex;
  /// under _writer_mutex
  bool       _stopping = false;
  /// set by the sink thread if it exits on an exception: handlers stop
  /// producing for a sink that will never drain, and stop_sink() reports it
  std::atomic< bool >     _sink_failed{ false };

  /// sink-private: batches delivered, and per stream the last block it was
  /// told about - a tripwire for the order each stream promises
  uint64_t _batches_delivered          = 0;
  uint32_t _last_irreversible_delivered = 0;
  uint32_t _last_head_delivered         = 0;

  chain::database::signal_connection_ptr post_apply_operation_connection;
  chain::database::signal_connection_ptr post_apply_block_connection;
  chain::database::signal_connection_ptr fail_apply_block_connection;
  chain::database::signal_connection_ptr irreversible_block_connection;
  chain::database::signal_connection_ptr switch_fork_connection;
};

/**
 * Every handler starts with one of these and returns at once when it
 * reports inactive: it holds _writer_mutex for the handler's body and
 * checks the two reasons not to run - stop_sink() has begun, or the sink
 * thread has died. A scope object because handlers throw by design (the
 * FC_ASSERTs around the be-tree calls and the frame encoder) and the lock
 * must go with the stack.
 */
class handler_scope
{
public:
  explicit handler_scope( stinger_plugin_impl& impl )
    : _lock( impl._writer_mutex ),
      _active( !impl._stopping && !impl._sink_failed.load( std::memory_order_relaxed ) )
  {
    if( !_active )
      _lock.unlock();
  }
  handler_scope( const handler_scope& ) = delete;
  handler_scope& operator=( const handler_scope& ) = delete;

  /// false once stop_sink() has begun, or the sink thread has died: the
  /// handler must not touch writer state (or has nobody to produce for)
  explicit operator bool() const { return _active; }

private:
  std::unique_lock< std::mutex > _lock;
  bool                           _active;
};

void stinger_plugin_impl::on_post_apply_operation( const operation_notification& note )
{
  handler_scope scope( *this );
  if( !scope )
    return;

  // Only ops applied as part of an actual block may produce matches:
  if( !_db.is_validating_block() )
    return;

  std::optional< protocol::asset > amount;
  std::optional< std::string >     custom_id;
  note.op.visit( event_attribute_visitor{ amount, custom_id } );

  fc::flat_set< protocol::account_name_type > impacted;
  app::operation_get_impacted_accounts( note.op, impacted );

  std::unique_ptr< betree_event, decltype( &betree_free_event ) > event(
    betree_make_event( _matcher.get() ), &betree_free_event );
  FC_ASSERT( event, "stinger: failed to create be-tree event" );

  betree_set_variable( event.get(), _slots.op_type,
    betree_make_integer_variable( STINGER_ATTR_OP_TYPE, note.op.which() ) );

  // owned here until it is attached to the event, which frees it with the
  // event; a throw in the loop must not leak it
  std::unique_ptr< betree_string_list, decltype( &betree_free_string_list ) > accounts(
    betree_make_string_list( impacted.size() ), &betree_free_string_list );
  size_t account_idx = 0;
  for( const auto& account : impacted )
    betree_add_string( accounts.get(), account_idx++, std::string( account ).c_str() );
  betree_set_variable( event.get(), _slots.impacted_accounts,
    betree_make_string_list_variable( STINGER_ATTR_IMPACTED_ACCOUNTS, accounts.release() ) );

  if( amount )
  {
    betree_set_variable( event.get(), _slots.amount,
      betree_make_integer_variable( STINGER_ATTR_AMOUNT, amount->amount.value ) );
    betree_set_variable( event.get(), _slots.symbol,
      betree_make_string_variable( STINGER_ATTR_SYMBOL,
        protocol::legacy_asset::from_asset( *amount ).asset_num_to_string().c_str() ) );
  }

  if( custom_id )
    betree_set_variable( event.get(), _slots.custom_id,
      betree_make_string_variable( STINGER_ATTR_CUSTOM_ID, custom_id->c_str() ) );

  std::unique_ptr< report, decltype( &free_report ) > matches( make_report(), &free_report );
  FC_ASSERT( matches, "stinger: failed to create be-tree report" );
  FC_ASSERT( betree_search_with_event( _matcher.get(), event.get(), matches.get() ),
    "stinger: be-tree search failed at block ${b} trx ${t} op ${o}",
    ("b", note.block)("t", note.trx_in_block)("o", note.op_in_trx) );

  if( matches->matched == 0 )
    return;

  // One match frame per operation, carrying every rule that matched it: the
  // body is packed once and the sink fans out. The frame goes into the
  // batch of every class with a matched rule; an id the index returned but
  // no rule owns cannot happen, and goes to the irreversible batch so the
  // sink reports it.
  bool to_irreversible = false;
  bool to_head         = false;
  for( size_t i = 0; i < matches->matched && !( to_irreversible && to_head ); ++i )
  {
    const auto rule = _rules_by_id.find( matches->subs[ i ] );
    if( rule != _rules_by_id.end() && rule->second->delivery == delivery_class::head )
      to_head = true;
    else
      to_irreversible = true;
  }
  // Positions travel as int32 in the frame; the sources are int32 and uint32
  // in database, so only op_in_trx can be narrowed, and only by a value no
  // transaction can hold. Tripwire, not a path real traffic takes.
  FC_ASSERT( note.op_in_trx >= 0 && note.op_in_trx <= std::numeric_limits< int32_t >::max(),
    "stinger: op_in_trx ${o} does not fit a match frame", ("o", note.op_in_trx) );
  match_coords coords;
  coords.block_num    = uint32_t( note.block );
  coords.trx_in_block = int32_t( note.trx_in_block );
  coords.op_in_trx    = int32_t( note.op_in_trx );
  coords.timestamp    = note.timestamp;
  coords.virtual_op   = note.virtual_op;
  coords.trx_id       = note.trx_id;
  static_assert( std::is_same_v< betree_sub_t, uint64_t >, "rule ids are stored as uint64" );
  _encoder->encode_match( coords, matches->subs, matches->matched, note.op );
  if( to_irreversible )
    _window.current().append( _encoder->data(), _encoder->size() );
  if( to_head )
    _head.current().append( _encoder->data(), _encoder->size() );
}

void stinger_plugin_impl::on_post_apply_block( const block_notification& note )
{
  handler_scope scope( *this );
  if( !scope )
    return;

  // A block at or above this height that both streams were told about and
  // that the chain undid without fail_apply_block or switch_fork (a throw
  // after post_apply_block in apply_block's storage flush does that). The
  // block being sealed is the real one; retract and drop the phantoms
  if( _head.last_block() >= note.block_num || _window.last_block() >= note.block_num )
  {
    elog( "stinger: block ${b} sealed while blocks up to ${h} were announced and ${p} pending - the chain undid them silently; retracting",
      ("b", note.block_num)("h", _head.last_block())("p", _window.last_block()) );
    retract_from( note.block_num );
  }

  // Head stream: the block's matches, if any, closed by block_applied, one
  // batch per validated block - the marker is the stream's clock, so a block
  // without matches is distinguishable from a stream that has not reached
  // it. Only validated blocks (ops were matched); replay never gets here.
  if( _head.enabled() && _db.is_validating_block() )
  {
    _encoder->encode_block_applied( note.block_num, note.block_id );
    push_ready( _head.close( note.block_num, _encoder->data(), _encoder->size() ) );
  }
  else
    _head.discard_current();

  // Irreversible stream: seal the block's matches; a block without matches
  // (every block of a replay, where matching is off) leaves nothing to seal
  _window.seal( note.block_num );
}

void stinger_plugin_impl::on_fail_apply_block( const block_notification& note )
{
  handler_scope scope( *this );
  if( !scope )
    return;

  // The block threw and was rolled back. Usually mid-application: whatever
  // its leading transactions matched belongs to a block that will not
  // exist, and neither stream has seen it since the head batch is pushed at
  // post_apply_block. But a throw after post_apply_block - another plugin's
  // handler, or state migration - also lands here, with both streams told
  // about the block: then it is retracted like a fork below it
  const uint32_t frames = _window.current().frames + _head.current().frames;
  if( frames != 0 )
    dlog( "stinger: block ${b} failed to apply - ${n} match(es) dropped", ("b", note.block_num)("n", frames) );
  _window.discard_current();
  _head.discard_current();
  if( _head.last_block() >= note.block_num || _window.last_block() >= note.block_num )
  {
    wlog( "stinger: block ${b} rolled back after it was announced - retracting", ("b", note.block_num) );
    retract_from( note.block_num );
  }
}

void stinger_plugin_impl::retract_from( uint32_t block_num )
{
  _window.drop_from( block_num );
  if( const auto below = _head.retract_from( block_num ) )
  {
    _encoder->encode_switch_fork( *below );
    push_ready( head_stream::marker( *below, _encoder->data(), _encoder->size() ) );
  }
}

void stinger_plugin_impl::on_irreversible_block( uint32_t block_num )
{
  handler_scope scope( *this );
  if( !scope )
    return;

  // Irreversible stream: release every sealed block up to and including block_num
  _window.release_up_to( block_num, [this]( block_matches&& b ) { push_ready( std::move( b ) ); } );

  // Head stream: the consumer can drop its retraction state up to here
  if( _head.irreversible( block_num ) )
  {
    _encoder->encode_irreversible( block_num );
    push_ready( head_stream::marker( block_num, _encoder->data(), _encoder->size() ) );
  }
}

void stinger_plugin_impl::on_switch_fork( uint32_t new_head_block_num )
{
  handler_scope scope( *this );
  if( !scope )
    return;

  // the blocks above the new head were popped; the new branch is applied
  // afterwards and its ops are matched fresh. Fires between the pops and
  // the first apply, when switching back after a block of the new branch
  // failed, and at startup after the state is undone to the last
  // irreversible block, when nothing is pending. The block being applied is
  // empty at all three unless a block was undone without a signal; dropped
  // either way
  const uint32_t held = _window.current().frames + _head.current().frames;
  _window.discard_current();
  _head.discard_current();
  const auto dropped = _window.drop_from( new_head_block_num + 1 );
  if( dropped.blocks != 0 || held != 0 )
    dlog( "stinger: fork switch to block ${b} - ${k} block(s), ${n} match(es) dropped",
      ("b", new_head_block_num)("k", dropped.blocks)("n", dropped.frames + held) );

  // Head stream: retract what it has seen above the new head, if anything
  if( const auto below = _head.switch_fork( new_head_block_num ) )
  {
    _encoder->encode_switch_fork( *below );
    push_ready( head_stream::marker( *below, _encoder->data(), _encoder->size() ) );
  }
}

void stinger_plugin_impl::push_ready( block_matches&& b )
{
  uint64_t dropped_total = 0;
  _frames_released += b.frames;
  ++_batches_released;
  {
    std::lock_guard< std::mutex > lock( _ready_mutex );
    _ready.push_back( std::move( b ) );
    // evict the oldest batch of matches; markers stay, see STINGER_READY_BATCHES_MAX
    while( _ready.size() > STINGER_READY_BATCHES_MAX )
    {
      const auto victim = std::find_if( _ready.begin(), _ready.end(),
        []( const block_matches& q ) { return !q.marker; } );
      if( victim == _ready.end() )
        break;
      _ready.erase( victim );
      ++_batches_dropped;
      dropped_total = _batches_dropped;
    }
  }
  _ready_cv.notify_one();

  // reported outside the lock, rate limited; the clock is read on the drop path only
  if( dropped_total != 0 )
  {
    const auto now = std::chrono::steady_clock::now();
    if( _drops_logged == 0 || now - _drop_log_time >= DROP_LOG_INTERVAL )
    {
      wlog( "stinger: sink is behind - dropped ${n} batch(es) of matches, ${t} in total",
        ("n", dropped_total - _drops_logged)("t", dropped_total) );
      _drops_logged  = dropped_total;
      _drop_log_time = now;
    }
  }
}

void stinger_plugin_impl::sink_loop()
{
  try
  {
    // fc's log context prints the current fc::thread's name; current() wraps
    // a foreign thread on first use, and set_name is direct on the current thread
    fc::thread::current().set_name( "stinger_sink" );

    for( ;; )
    {
      block_matches b;
      {
        std::unique_lock< std::mutex > lock( _ready_mutex );
        _ready_cv.wait( lock, [this]() { return !_ready.empty() || _ready_stopped; } );
        if( _ready.empty() )
          break;  // stopped, and stop is set after the final release: drained
        b = std::move( _ready.front() );
        _ready.pop_front();
      }
      // a decode tripwire or a sink bug drops this block, not hived: an
      // exception escaping a std::thread is std::terminate
      try
      {
        deliver_block( b );
      }
      catch( const fc::exception& e )
      {
        elog( "stinger: sink dropped block ${b}: ${e}", ("b", b.block_num)("e", e.to_detail_string()) );
      }
      catch( const std::exception& e )
      {
        elog( "stinger: sink dropped block ${b}: ${e}", ("b", b.block_num)("e", e.what()) );
      }
      catch( ... )
      {
        elog( "stinger: sink dropped block ${b}: unknown exception", ("b", b.block_num) );
      }
    }
    return;
  }
  catch( const fc::exception& e )
  {
    elog( "stinger: sink thread died, match delivery has stopped: ${e}", ("e", e.to_detail_string()) );
  }
  catch( const std::exception& e )
  {
    elog( "stinger: sink thread died, match delivery has stopped: ${e}", ("e", e.what()) );
  }
  catch( ... )
  {
    elog( "stinger: sink thread died, match delivery has stopped: unknown exception" );
  }
  _sink_failed.store( true, std::memory_order_relaxed );
}

void stinger_plugin_impl::deliver_block( const block_matches& b )
{
  ++_batches_delivered;
  if( b.cls == delivery_class::irreversible )
  {
    // irreversibility is monotonic and the writer releases in block order
    FC_ASSERT( b.block_num > _last_irreversible_delivered,
      "stinger: block ${b} delivered after block ${l}", ("b", b.block_num)("l", _last_irreversible_delivered) );
    _last_irreversible_delivered = b.block_num;
  }

  frame_reader reader( b.bytes.data(), b.bytes.size() );
  uint32_t frames = 0;
  while( reader.more() )
  {
    const frame_view f = reader.next();
    ++frames;
    switch( f.kind() )
    {
      case frame_kind::match:
        deliver_match( decode_match( f ), b.cls );
        break;
      case frame_kind::block_applied:
      {
        FC_ASSERT( b.cls == delivery_class::head, "stinger: block_applied frame in the irreversible stream" );
        uint32_t                block_num;
        protocol::block_id_type block_id;
        decode_block_applied( f, block_num, block_id );
        // blocks are applied in order, and a switch_fork lowered the mark
        FC_ASSERT( block_num > _last_head_delivered,
          "stinger: head block ${b} delivered after block ${l}", ("b", block_num)("l", _last_head_delivered) );
        _last_head_delivered = block_num;
        dlog( "stinger: head block ${b} ${id} applied", ("b", block_num)("id", block_id) );
        break;
      }
      case frame_kind::irreversible:
      {
        FC_ASSERT( b.cls == delivery_class::head, "stinger: irreversible frame in the irreversible stream" );
        dlog( "stinger: head blocks up to ${b} irreversible", ("b", decode_block_num( f )) );
        break;
      }
      case frame_kind::switch_fork:
      {
        FC_ASSERT( b.cls == delivery_class::head, "stinger: switch_fork frame in the irreversible stream" );
        const uint32_t new_head = decode_block_num( f );
        ilog( "stinger: head blocks above ${b} retracted by a fork switch", ("b", new_head) );
        _last_head_delivered = new_head;
        break;
      }
      default:
        FC_THROW_EXCEPTION( fc::assert_exception, "stinger: unknown frame kind ${k} in block ${b}",
          ("k", f.header.kind)("b", b.block_num) );
    }
  }
  FC_ASSERT( frames == b.frames, "stinger: block ${b} carries ${n} frame(s), sealed with ${s}",
    ("b", b.block_num)("n", frames)("s", b.frames) );
}

void stinger_plugin_impl::deliver_match( const match_frame& m, delivery_class cls )
{
  // the only sink for now is the log, one line per matched rule of the class
  fc::variant op_body;
  bool        unpacked = false;

  for( size_t i = 0; i < m.rule_count; ++i )
  {
    const uint64_t id   = m.rule_id( i );
    const auto     rule = _rules_by_id.find( id );
    if( rule == _rules_by_id.end() )
    {
      if( cls == delivery_class::irreversible )
        elog( "stinger: matched subscription id ${i} has no rule", ("i", id) );
      continue;
    }
    if( rule->second->delivery != cls )
      continue;   // the other stream's rule
    if( !unpacked )
    {
      // the body is unpacked once per frame, and only for a stream that reports it
      if( m.body_omitted )
        op_body = "<omitted: operation exceeds the frame budget>";
      else
        fc::to_variant( m.unpack_op(), op_body );
      unpacked = true;
    }
    ilog( "stinger: match rule=${r} delivery=${d} block=${b} trx_id=${t} trx_in_block=${tib} op_in_trx=${oit} virtual_op=${v} op=${op}",
      ("r", rule->second->name)("d", cls)("b", m.coords.block_num)("t", m.coords.trx_id)
      ("tib", m.coords.trx_in_block)("oit", m.coords.op_in_trx)("v", m.coords.virtual_op)("op", op_body) );
  }
}

void stinger_plugin_impl::stop_sink()
{
  if( !_sink.joinable() )
    return;  // never started, or already stopped

  // 1. no handler from here on touches writer state: taking the lock waits
  // out one already running, and every later one sees _stopping and returns
  {
    std::lock_guard< std::mutex > lock( _writer_mutex );
    _stopping = true;
  }
  // 2. no new handler invocations
  hive::utilities::disconnect_signal( post_apply_operation_connection );
  hive::utilities::disconnect_signal( post_apply_block_connection );
  hive::utilities::disconnect_signal( fail_apply_block_connection );
  hive::utilities::disconnect_signal( irreversible_block_connection );
  hive::utilities::disconnect_signal( switch_fork_connection );
  // 3. this thread owns the writer-private state now. What is not
  // irreversible is not delivered; say what that was
  const uint32_t held_frames = _window.current().frames;
  if( !_window.empty() || held_frames != 0 )
  {
    ilog( "stinger: ${k} block(s) ${a}..${z} with ${n} match(es) were not irreversible at shutdown and are not delivered",
      ("k", _window.size() + ( held_frames != 0 ? 1 : 0 ))
      ("a", _window.first_block())("z", _window.last_block())("n", _window.frames() + held_frames) );
    _window.drop_from( 0 );
    _window.discard_current();
  }
  // 4. the stream has ended; set after the final release, under the same
  // mutex, so the sink's "stopped and empty" means drained
  {
    std::lock_guard< std::mutex > lock( _ready_mutex );
    _ready_stopped = true;
  }
  _ready_cv.notify_one();
  // 5. the sink exits once it has drained the queue
  _sink.join();

  if( _sink_failed.load( std::memory_order_relaxed ) )
    elog( "stinger: sink had died - ${r} batch(es) released, ${c} delivered, ${d} dropped, ${f} frame(s)",
      ("r", _batches_released)("c", _batches_delivered)("d", _batches_dropped)("f", _frames_released) );
  else
    ilog( "stinger: sink stopped - ${r} batch(es) released, ${c} delivered, ${d} dropped, ${f} frame(s)",
      ("r", _batches_released)("c", _batches_delivered)("d", _batches_dropped)("f", _frames_released) );
}

/// registers the rule attributes and returns the event variable id betree assigned to each
event_slots declare_rule_attributes( betree* tree )
{
  event_slots slots;

  // op.which() over hive::protocol::operation (virtual ops included)
  slots.op_type = betree_add_integer_variable( tree, STINGER_ATTR_OP_TYPE, false, 0, hive::protocol::operation::count() - 1 );
  // operation_get_impacted_accounts of the op
  slots.impacted_accounts = betree_add_string_list_variable( tree, STINGER_ATTR_IMPACTED_ACCOUNTS, false, SIZE_MAX );
  // primary asset of ops that carry one: amount in satoshis + symbol name;
  // undefined for ops without an asset
  slots.amount = betree_add_integer_variable( tree, STINGER_ATTR_AMOUNT, true, 0, INT64_MAX );
  slots.symbol = betree_add_string_variable( tree, STINGER_ATTR_SYMBOL, true, SIZE_MAX );
  // custom_json_operation id; undefined for other op types
  slots.custom_id = betree_add_string_variable( tree, STINGER_ATTR_CUSTOM_ID, true, SIZE_MAX );

  return slots;
}

} // detail

stinger_plugin::stinger_plugin() {}
stinger_plugin::~stinger_plugin() {}

void stinger_plugin::set_program_options( boost::program_options::options_description& cli, boost::program_options::options_description& cfg )
{
  cfg.add_options()
    ( STINGER_RULES_FILE_KEY, boost::program_options::value<std::string>(), "Path to the file with stinger rule definitions." )
    ;
}

void stinger_plugin::plugin_initialize( const boost::program_options::variables_map& options )
{
  try
  {
    ilog( "stinger: plugin_initialize() begin" );
    my = std::make_unique< detail::stinger_plugin_impl >( get_app() );

    FC_ASSERT( options.count( STINGER_RULES_FILE_KEY ),
      "stinger: " STINGER_RULES_FILE_KEY " is required when the stinger plugin is enabled" );
    my->rules_file = options.at( STINGER_RULES_FILE_KEY ).as< std::string >();
    my->rules = load_rules_file( my->rules_file );
    ilog( "stinger: loaded ${n} rule(s) from ${f}",
      ("n", my->rules.rules.size())("f", my->rules_file.generic_string()) );

    // the index must be ready before any signal handler is connected
    my->_matcher.reset( betree_make() );
    FC_ASSERT( my->_matcher, "stinger: failed to create be-tree matcher" );
    my->_slots = detail::declare_rule_attributes( my->_matcher.get() );
    size_t head_rules = 0;
    for( const auto& rule : my->rules.rules )
    {
      FC_ASSERT( betree_insert( my->_matcher.get(), rule.id, rule.match.c_str() ),
        "stinger: rule \"${n}\" (id ${i}) failed to parse: ${e}",
        ("n", rule.name)("i", rule.id)("e", rule.match) );
      my->_rules_by_id.emplace( rule.id, &rule );
      if( rule.delivery == delivery_class::head )
        ++head_rules;
    }
    my->_head = head_stream( head_rules != 0 );
    ilog( "stinger: subscription index built, ${n} rule(s), ${h} delivered at head",
      ("n", my->rules.rules.size())("h", head_rules) );

    // the encoder and the sink thread must exist before the handlers
    // connect, for the same reason as the index; the sink reads
    // _rules_by_id, which is complete by now and never changes again
    my->_encoder = std::make_unique< frame_encoder >( detail::STINGER_FRAME_BUDGET );
    my->_sink    = std::thread( [this]() { my->sink_loop(); } );
    ilog( "stinger: sink started" );

    my->post_apply_operation_connection = my->_db.add_post_apply_operation_handler(
      [&]( const operation_notification& note ) { try { my->on_post_apply_operation( note ); } FC_LOG_AND_RETHROW() }, *this, 0 );
    my->post_apply_block_connection = my->_db.add_post_apply_block_handler(
      [&]( const block_notification& note ) { try { my->on_post_apply_block( note ); } FC_LOG_AND_RETHROW() }, *this, 0 );
    my->fail_apply_block_connection = my->_db.add_fail_apply_block_handler(
      [&]( const block_notification& note ) { try { my->on_fail_apply_block( note ); } FC_LOG_AND_RETHROW() }, *this, 0 );
    my->irreversible_block_connection = my->_db.add_irreversible_block_handler(
      [&]( uint32_t block_num ) { try { my->on_irreversible_block( block_num ); } FC_LOG_AND_RETHROW() }, *this, 0 );
    my->switch_fork_connection = my->_db.add_switch_fork_handler(
      [&]( uint32_t new_head_block_num ) { try { my->on_switch_fork( new_head_block_num ); } FC_LOG_AND_RETHROW() }, *this, 0 );

    ilog( "stinger: plugin_initialize() end" );
  } FC_CAPTURE_AND_RETHROW()
}

void stinger_plugin::plugin_startup()
{
  // nothing to do
}

void stinger_plugin::plugin_shutdown()
{
  // stop_sink() disconnects the signals and waits out in-flight handlers
  // before the index goes away, so no handler can run on a freed be-tree
  my->stop_sink();
  my->_matcher.reset();
}

} } } // hive::plugins::stinger
