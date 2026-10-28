#include <hive/chain/hive_fwd.hpp>

#include <hive/plugins/stinger/frame.hpp>
#include <hive/plugins/stinger/records.hpp>
#include <hive/plugins/stinger/rules.hpp>
#include <hive/plugins/stinger/socket_sink.hpp>
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
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#define STINGER_RULES_FILE_KEY "stinger-rules-file"
#define STINGER_SOCKET_KEY     "stinger-socket"
#define STINGER_MATCH_DURING_SYNC_KEY "stinger-match-during-sync"

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

/// The longest line the socket carries, newline included; a match whose
/// line would be longer goes out without its operation body. Well under
/// the send buffer, so a line always fits a consumer that is keeping up,
/// and well over any operation the chain carries today: the transaction
/// limit is 64 KB, and only a hostile body (control characters render six
/// bytes each) gets anywhere near this.
constexpr size_t STINGER_SOCKET_LINE_MAX = 1024 * 1024;

/// how long the sink waits for a batch before it looks at the socket for
/// consumers connecting or leaving; only when there is a socket
constexpr std::chrono::milliseconds SINK_POLL_INTERVAL{ 500 };


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
 * sealed. A rule file without head rules produces none of this.
 *
 * Only validated blocks are matched, never replayed ones, and by default
 * only once the node is live: matching is armed by the chain's
 * end_of_syncing signal, so a node catching up from a cold start neither
 * floods its consumers with years of history nor pays the matcher on every
 * synced block. stinger-match-during-sync arms it at startup instead, for a
 * consumer that wants the backfill. Either way the socket's hello
 * (records.hpp) tells a consumer where each stream stands when it connects.
 *
 * An operation matched by rules of both classes puts the same frame, with
 * every matched rule id, in both batches; the sink reports to each stream
 * only the rules of that class.
 *
 * The sink thread decodes a batch and hands each match to the rules' exec
 * targets (rules.hpp): a log line per rule for `log`, and for `socket` one
 * JSON line per class on the unix socket (records.hpp, socket_sink.hpp),
 * naming every socket rule of that class that matched. The head stream's
 * markers go to the socket as well, when there is one. The socket is
 * served by the sink thread between batches, so it never touches the
 * writer either.
 *
 * All writer state is touched under _writer_mutex only: by the writer
 * thread, and by the main thread for the irreversible and fork signals the
 * chain fires during startup and replay, when it is empty. The
 * cross-thread point to the sink is _ready, a mutex-guarded queue of
 * batches it waits on, in the writer's order, so each stream is in order;
 * beside it two atomics the writer sets for the sink's hello record, and
 * _sink_failed the other way.
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
  void on_end_of_syncing();

  /// both streams forget blocks at or above block_num: pending blocks are
  /// dropped and the head stream gets a switch_fork below it, naming
  /// `prev_id`, the block at block_num - 1
  void retract_from( uint32_t block_num, const protocol::block_id_type& prev_id );
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
  /// under _writer_mutex: operations are matched. Set at initialization by
  /// stinger-match-during-sync, otherwise by the end_of_syncing signal
  bool       _armed = false;
  /// For the hello record (records.hpp); written on the writer thread,
  /// read by the sink, relaxed: the queue's mutex orders them against the
  /// batches they describe.
  /// The lowest block whose operations were matched; 0 until there is one.
  /// Usually the first matched block, but a fork that reaches below it
  /// re-applies, and so matches, lower blocks. One below it is the floor
  /// of the irreversible stream's position: the blocks below were never
  /// matched, so the stream never carries them
  std::atomic< uint32_t > _matched_from{ 0 };
  /// the highest block the chain has made irreversible. Stored before the
  /// window's blocks up to it are released to the sink, so a hello that
  /// reports it is never behind what the socket has delivered; it may run
  /// ahead by the sink's backlog, which the contract allows. Monotonic: the
  /// chain re-notifies heights after a fork switch driven by fast confirms
  std::atomic< uint32_t > _last_irreversible_seen{ 0 };
  /// set by the sink thread if it exits on an exception: handlers stop
  /// producing for a sink that will never drain, and stop_sink() reports it
  std::atomic< bool >     _sink_failed{ false };

  /// sink-private: batches delivered, and per stream the last block it was
  /// told about - a tripwire for the order each stream promises, and for
  /// the head stream its position, with the block's id, for the hello record
  uint64_t                _batches_delivered           = 0;
  uint32_t                _last_irreversible_delivered = 0;
  uint32_t                _last_head_delivered         = 0;
  protocol::block_id_type _last_head_delivered_id;

  /// sink-private: the socket, when stinger-socket is set; created in
  /// plugin_initialize before the sink thread starts and destroyed by
  /// stop_sink() after it is joined
  std::unique_ptr< socket_sink >   _socket;
  record_writer                    _records{ STINGER_SOCKET_LINE_MAX };
  /// the socket rules of one class that matched a frame; scratch
  std::vector< std::string_view >  _socket_rules;

  chain::database::signal_connection_ptr post_apply_operation_connection;
  chain::database::signal_connection_ptr post_apply_block_connection;
  chain::database::signal_connection_ptr fail_apply_block_connection;
  chain::database::signal_connection_ptr irreversible_block_connection;
  chain::database::signal_connection_ptr switch_fork_connection;
  chain::database::signal_connection_ptr end_of_syncing_connection;
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

  // Only ops of a validated block may produce matches, and only once the
  // node is live (or from the start, with stinger-match-during-sync); a
  // replayed block is never matched
  if( !_armed || !_db.is_validating_block() )
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
    retract_from( note.block_num, note.prev_block_id );
  }

  // The block's operations went through the matcher: validated, and the
  // plugin armed. The lowest such block floors the position the hello
  // reports; stored before the block's batch is pushed, so a hello that sees
  // the batch sees it
  const bool matched = _armed && _db.is_validating_block();
  if( matched )
  {
    const uint32_t lowest = _matched_from.load( std::memory_order_relaxed );
    if( lowest == 0 || note.block_num < lowest )
      _matched_from.store( note.block_num, std::memory_order_relaxed );
  }

  // Head stream: the block's matches, if any, closed by block_applied, one
  // batch per matched block - the marker is the stream's clock, so a block
  // without matches is distinguishable from a stream that has not reached
  // it. A replayed block, or one applied before the node was live, never
  // gets here.
  if( _head.enabled() && matched )
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
    retract_from( note.block_num, note.prev_block_id );
  }
}

void stinger_plugin_impl::retract_from( uint32_t block_num, const protocol::block_id_type& prev_id )
{
  _window.drop_from( block_num );
  if( const auto below = _head.retract_from( block_num ) )
  {
    _encoder->encode_switch_fork( *below, prev_id );
    push_ready( head_stream::marker( *below, _encoder->data(), _encoder->size() ) );
  }
}

void stinger_plugin_impl::on_irreversible_block( uint32_t block_num )
{
  handler_scope scope( *this );
  if( !scope )
    return;

  // Irreversible stream: release every sealed block up to and including
  // block_num. The height goes to the sink first, so a hello written while
  // the releases are still queued reports it (see _last_irreversible_seen);
  // a re-notified lower height must not take it back
  if( block_num > _last_irreversible_seen.load( std::memory_order_relaxed ) )
    _last_irreversible_seen.store( block_num, std::memory_order_relaxed );
  _window.release_up_to( block_num, [this]( block_matches&& b ) { push_ready( std::move( b ) ); } );

  // Head stream: the consumer can drop its retraction state up to here
  if( _head.irreversible( block_num ) )
  {
    _encoder->encode_irreversible( block_num );
    push_ready( head_stream::marker( block_num, _encoder->data(), _encoder->size() ) );
  }
}

void stinger_plugin_impl::on_end_of_syncing()
{
  handler_scope scope( *this );
  if( !scope || _armed )
    return;

  // Fired by the writer thread between blocks when the head block is within
  // a minute of now (or at startup, when the chain runs without P2P), so
  // the next block applied is matched whole
  _armed = true;
  ilog( "stinger: node is live at block ${b} - matching from the next one", ("b", _db.head_block_num()) );
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

  // The chain stands at the new head when it signals (sync_block_writer pops
  // down to it first, startup undoes down to it), so the switch_fork marker
  // names the head's id. Tripwire, before either stream is touched: a wrong
  // id would send consumers after a block that is not there. A handler that
  // bails here leaves both streams standing above the new head, and the
  // next block sealed below them is retracted as a silent undo
  // (on_post_apply_block); bailing after the window was cut and before the
  // head stream was would instead leave the head stream unable to close
  // any block until the chain passed its old height
  const uint32_t chain_head = _db.head_block_num();
  FC_ASSERT( chain_head == new_head_block_num,
    "stinger: fork switch to block ${b} signalled with the chain at ${h}", ("b", new_head_block_num)("h", chain_head) );

  const auto dropped = _window.drop_from( new_head_block_num + 1 );
  if( dropped.blocks != 0 || held != 0 )
    dlog( "stinger: fork switch to block ${b} - ${k} block(s), ${n} match(es) dropped",
      ("b", new_head_block_num)("k", dropped.blocks)("n", dropped.frames + held) );

  // Head stream: retract what it has seen above the new head, if anything
  if( const auto below = _head.switch_fork( new_head_block_num ) )
  {
    _encoder->encode_switch_fork( *below, _db.head_block_id() );
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

    const auto ready = [this]() { return !_ready.empty() || _ready_stopped; };
    for( ;; )
    {
      block_matches b;
      bool          have = false;
      {
        std::unique_lock< std::mutex > lock( _ready_mutex );
        // with a socket the wait is bounded, so consumers are accepted and
        // reaped while no batch arrives
        if( _socket )
          _ready_cv.wait_for( lock, SINK_POLL_INTERVAL, ready );
        else
          _ready_cv.wait( lock, ready );
        if( !_ready.empty() )
        {
          b = std::move( _ready.front() );
          _ready.pop_front();
          have = true;
        }
        else if( _ready_stopped )
          break;  // stop is set after the final release: drained
      }
      // before the batch, so a consumer that just connected gets it
      if( _socket )
      {
        stream_position position;
        position.head_block = _last_head_delivered;
        position.head_id    = _last_head_delivered_id;
        position.irreversible = irreversible_position( _last_irreversible_seen.load( std::memory_order_relaxed ),
                                                       _matched_from.load( std::memory_order_relaxed ) );
        _socket->poll( _records.hello( position ) );
      }
      if( !have )
        continue;
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
        decode_block_with_id( f, block_num, block_id );
        // blocks are applied in order, and a switch_fork lowered the mark
        FC_ASSERT( block_num > _last_head_delivered,
          "stinger: head block ${b} delivered after block ${l}", ("b", block_num)("l", _last_head_delivered) );
        _last_head_delivered    = block_num;
        _last_head_delivered_id = block_id;
        dlog( "stinger: head block ${b} ${id} applied", ("b", block_num)("id", block_id) );
        if( _socket )
          _socket->send( _records.block_applied( block_num, block_id ) );
        break;
      }
      case frame_kind::irreversible:
      {
        FC_ASSERT( b.cls == delivery_class::head, "stinger: irreversible frame in the irreversible stream" );
        const uint32_t block_num = decode_block_num( f );
        dlog( "stinger: head blocks up to ${b} irreversible", ("b", block_num) );
        if( _socket )
          _socket->send( _records.irreversible( block_num ) );
        break;
      }
      case frame_kind::switch_fork:
      {
        FC_ASSERT( b.cls == delivery_class::head, "stinger: switch_fork frame in the irreversible stream" );
        uint32_t                new_head;
        protocol::block_id_type new_head_id;
        decode_block_with_id( f, new_head, new_head_id );
        ilog( "stinger: head blocks above ${b} retracted by a fork switch", ("b", new_head) );
        _last_head_delivered    = new_head;
        _last_head_delivered_id = new_head_id;
        if( _socket )
          _socket->send( _records.switch_fork( new_head, new_head_id ) );
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
  // log rules get a line each; socket rules are collected into one record
  fc::variant op_body;
  bool        unpacked = false;
  _socket_rules.clear();

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
    switch( rule->second->exec.type )
    {
      case exec_type::socket:
        _socket_rules.push_back( rule->second->name );
        break;
      case exec_type::log:
        if( !unpacked )
        {
          // the body is unpacked once per frame, and only when a log rule wants it
          if( m.body_omitted )
            op_body = "<omitted: operation exceeds the frame budget>";
          else
            fc::to_variant( m.unpack_op(), op_body );
          unpacked = true;
        }
        ilog( "stinger: match rule=${r} delivery=${d} block=${b} trx_id=${t} trx_in_block=${tib} op_in_trx=${oit} virtual_op=${v} op=${op}",
          ("r", rule->second->name)("d", cls)("b", m.coords.block_num)("t", m.coords.trx_id)
          ("tib", m.coords.trx_in_block)("oit", m.coords.op_in_trx)("v", m.coords.virtual_op)("op", op_body) );
        break;
    }
  }

  // rendered even with nobody connected: seq counts what the node emitted
  if( !_socket_rules.empty() && _socket )
    _socket->send( _records.match( cls, _socket_rules, m ) );
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
  hive::utilities::disconnect_signal( end_of_syncing_connection );
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
  // 6. the socket goes with the sink thread that served it; consumers see EOF
  if( _socket )
  {
    const auto& s = _socket->stats();
    ilog( "stinger: socket ${p} closed - ${a} connection(s) accepted, ${l} line(s) / ${b} byte(s) sent, ${r} record(s) rendered, ${c} closed by consumers, ${d} dropped behind, ${f} failed",
      ("p", _socket->path().generic_string())("a", s.accepted)("l", s.lines)("b", s.bytes)("r", _records.last_seq())
      ("c", s.closed_by_peer)("d", s.dropped_behind)("f", s.failed) );
    _socket.reset();
  }
}

/// registers the rule attributes and returns the event variable id betree assigned to each
event_slots declare_rule_attributes( betree* tree )
{
  // an attribute is an identifier in the match expression, where an
  // operation name is rewritten to its number: the vocabularies must not meet
  for( const char* attr : { STINGER_ATTR_OP_TYPE, STINGER_ATTR_IMPACTED_ACCOUNTS, STINGER_ATTR_AMOUNT,
                            STINGER_ATTR_SYMBOL, STINGER_ATTR_CUSTOM_ID } )
    FC_ASSERT( resolve_operation_names( attr ) == attr,
      "stinger: attribute \"${a}\" is also an operation name", ("a", attr) );

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
    ( STINGER_SOCKET_KEY, boost::program_options::value<std::string>(),
      "Path of the unix socket the stinger match stream is served on, as newline-delimited JSON. Required when a rule's exec type is socket." )
    ( STINGER_MATCH_DURING_SYNC_KEY, boost::program_options::value<bool>()->default_value( false ),
      "Match operations of blocks received while the node is still syncing (backfill). By default matching starts when the node enters live mode. Replayed blocks are never matched." )
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
      FC_ASSERT( betree_insert( my->_matcher.get(), rule.id, rule.expr.c_str() ),
        "stinger: rule \"${n}\" (id ${i}) failed to parse: ${m}${e}",
        ("n", rule.name)("i", rule.id)("m", rule.match)
        ("e", rule.expr == rule.match ? std::string() : " (compiled as: " + rule.expr + ")") );
      my->_rules_by_id.emplace( rule.id, &rule );
      if( rule.delivery == delivery_class::head )
        ++head_rules;
    }
    my->_head = head_stream( head_rules != 0 );
    ilog( "stinger: subscription index built, ${n} rule(s), ${h} delivered at head",
      ("n", my->rules.rules.size())("h", head_rules) );

    // when matching starts: now, or when the chain says the node is live
    my->_armed = options.at( STINGER_MATCH_DURING_SYNC_KEY ).as< bool >();
    if( my->_armed )
      ilog( "stinger: matching during sync (" STINGER_MATCH_DURING_SYNC_KEY ")" );
    else
      ilog( "stinger: matching starts when the node is live" );

    // the socket, if asked for; a rule that needs one and finds none is a
    // configuration error, found now and not at the first match
    const size_t socket_rules = std::count_if( my->rules.rules.begin(), my->rules.rules.end(),
      []( const rule_definition& r ) { return r.exec.type == exec_type::socket; } );
    if( options.count( STINGER_SOCKET_KEY ) )
    {
      my->_socket = std::make_unique< socket_sink >( fc::path( options.at( STINGER_SOCKET_KEY ).as< std::string >() ) );
      ilog( "stinger: serving ${n} rule(s) on socket ${p}", ("n", socket_rules)("p", my->_socket->path().generic_string()) );
    }
    else
      FC_ASSERT( socket_rules == 0,
        "stinger: ${n} rule(s) have exec type socket but " STINGER_SOCKET_KEY " is not set", ("n", socket_rules) );

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
    my->end_of_syncing_connection = my->_db.add_end_of_syncing_handler(
      [&]() { try { my->on_end_of_syncing(); } FC_LOG_AND_RETHROW() }, *this, 0 );

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
