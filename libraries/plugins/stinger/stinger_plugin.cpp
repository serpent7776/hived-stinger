#include <hive/chain/hive_fwd.hpp>

#include <hive/plugins/stinger/frame.hpp>
#include <hive/plugins/stinger/page_buffer.hpp>
#include <hive/plugins/stinger/rules.hpp>
#include <hive/plugins/stinger/stinger_plugin.hpp>

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
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>

#define STINGER_RULES_FILE_KEY "stinger-rules-file"

namespace hive { namespace plugins { namespace stinger {

using namespace hive::chain;

namespace detail {

constexpr const char* STINGER_ATTR_OP_TYPE           = "op_type";
constexpr const char* STINGER_ATTR_IMPACTED_ACCOUNTS = "impacted_accounts";
constexpr const char* STINGER_ATTR_AMOUNT            = "amount";
constexpr const char* STINGER_ATTR_SYMBOL            = "symbol";
constexpr const char* STINGER_ATTR_CUSTOM_ID         = "custom_id";

/// page geometry of the match stream: 1 MiB pages, 16 in the ready ring
/// (18 MiB with the producer's and the consumer's page); config knobs later
constexpr size_t STINGER_PAGE_SIZE  = size_t( 1 ) << 20;
constexpr size_t STINGER_PAGE_COUNT = 16;

/// the sink thread's idle poll interval backs off between these two
constexpr std::chrono::microseconds SINK_IDLE_MIN{ 200 };
constexpr std::chrono::microseconds SINK_IDLE_MAX{ 2000 };

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
 * Stateless stream emitter: matches each operation against the be-tree rule
 * index and forwards results plus chain lifecycle signals as one ordered event
 * stream. Fork handling is the consumer's job; the stream carries everything
 * needed for it:
 *
 *   match          { rule name, block_num, trx_in_block, op_in_trx, virtual_op }
 *   block_applied  { block_num, block_id }   - seals a block; carries the id
 *                                              that identifies its branch
 *   switch_fork    { new_head_block_num }    - discard held state above N
 *   irreversible   { block_num }             - everything <= N is final
 *
 * Events are emitted in signal order on the writer thread; sinks must preserve
 * that order per subscriber. A consumer wanting safe delivery buffers matches
 * per block until irreversible and drops them on switch_fork; one wanting
 * lowest latency acts on match and compensates on switch_fork.
 *
 * A block that fails mid-application is the one non-fork way a block's
 * matches can be undone: its leading transactions have already emitted
 * matches when a later one throws and the block's state is rolled back.
 * fail_apply_block reports it as switch_fork{ block_num - 1 }, which
 * already means "discard everything above"; no block_applied follows.
 *
 * Delivery is off the writer thread: a handler packs its event into a frame
 * (frame.hpp) and appends it to the page buffer; a page is handed over at a
 * block boundary or when full, and the sink thread decodes it and delivers
 * it. The writer thread never blocks on the sink - a full ring drops the
 * oldest page, counted.
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

  /// the sink thread's body
  void sink_loop();
  /// deliver every frame of one taken page
  void deliver_page( const std::byte* page );
  void deliver_match( const match_frame& m );
  /// stop producing, flush the current page, stop and join the sink; idempotent
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

  /// subscription id -> rule, for turning report ids back into rule names;
  /// built in plugin_initialize together with the index, read by the sink
  /// thread afterwards and never written again
  std::map< uint64_t, const rule_definition* > _rules_by_id;

  /// the match stream's pages: appended on the writer thread, taken by the sink
  std::unique_ptr< page_buffer >   _pages;
  /// the writer thread's frame scratch; sized to a page's capacity
  std::unique_ptr< frame_encoder > _encoder;
  /// the sink thread; a plain std::thread, not scheduled against appbase
  std::thread                      _sink;

  /// Handshake between the signal handlers on the writer thread and
  /// stop_sink() on the shutdown thread. signals2's disconnect() stops new
  /// invocations but does not wait for one already running, and a handler
  /// may be mid-append when stop_sink() wants to rotate() the current page.
  /// Each handler enters by incrementing _in_handler and then checking
  /// _stopping (bailing if set); stop_sink() sets _stopping and then spins
  /// until _in_handler is 0. Both seq_cst, so the two sides cannot both miss
  /// each other: either the handler bails before touching producer state, or
  /// the spin waits for its release-decrement, which is the happens-before
  /// edge that makes the shutdown thread's rotate() legal.
  std::atomic< bool >     _stopping{ false };
  std::atomic< unsigned > _in_handler{ 0 };
  /// set by the sink thread if it exits on an exception: handlers stop
  /// producing into pages nobody will drain, and stop_sink() reports it
  std::atomic< bool >     _sink_failed{ false };

  /// sink-private: pages taken and the index of the last one, a tripwire
  /// for page-index arithmetic
  uint64_t _pages_consumed = 0;
  uint64_t _last_page_index = 0;

  chain::database::signal_connection_ptr post_apply_operation_connection;
  chain::database::signal_connection_ptr post_apply_block_connection;
  chain::database::signal_connection_ptr fail_apply_block_connection;
  chain::database::signal_connection_ptr irreversible_block_connection;
  chain::database::signal_connection_ptr switch_fork_connection;
};

/**
 * RAII side of the shutdown handshake: every handler starts with one of
 * these and returns at once when it reports inactive. A scope guard rather
 * than manual stores because handlers throw by design (the page buffer's
 * append backstop, the FC_ASSERTs around the be-tree calls) and a decrement
 * skipped by stack unwinding would hang stop_sink() forever.
 */
class handler_scope
{
public:
  explicit handler_scope( stinger_plugin_impl& impl ) : _impl( impl )
  {
    _impl._in_handler.fetch_add( 1, std::memory_order_seq_cst );
    _active = !_impl._stopping.load( std::memory_order_seq_cst )
           && !_impl._sink_failed.load( std::memory_order_relaxed );
    if( !_active )
      _impl._in_handler.fetch_sub( 1, std::memory_order_release );
  }
  ~handler_scope()
  {
    if( _active )
      _impl._in_handler.fetch_sub( 1, std::memory_order_release );
  }
  handler_scope( const handler_scope& ) = delete;
  handler_scope& operator=( const handler_scope& ) = delete;

  /// false once stop_sink() has begun, or the sink thread has died: the
  /// handler must not touch producer state (or has nobody to produce for)
  explicit operator bool() const { return _active; }

private:
  stinger_plugin_impl& _impl;
  bool                 _active;
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
  // body is packed once and the sink fans out. The block id of the
  // containing block is not known yet; the block_applied event that follows
  // the block's matches carries it.
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
  _pages->append( _encoder->data(), _encoder->size() );
}

void stinger_plugin_impl::on_post_apply_block( const block_notification& note )
{
  handler_scope scope( *this );
  if( !scope )
    return;

  // emit block_applied{ note.block_num, note.block_id } - seals the block's
  // matches, binds them to a branch, and tells consumers the stream has
  // advanced (a block with no matches is distinguishable from a stream that
  // has not reached it). Natural flush point for batched outbound sinks.
  if( _db.is_replaying_block() )
    return;

  dlog( "stinger: block ${b} ts=${ts}",
    ("b", note.block_num)("ts", note.full_block->get_block_header().timestamp) );

  // a page ends at a block boundary or when full, whichever comes first;
  // a block without matches leaves the page empty and this is a no-op
  _pages->rotate();
}

void stinger_plugin_impl::on_fail_apply_block( const block_notification& note )
{
  handler_scope scope( *this );
  if( !scope )
    return;

  // emit switch_fork{ note.block_num - 1 } - the block's matches already in
  // the stream belong to a block that will not exist; see the contract above.
  // Fires only from apply_block_extended, so it is on the writer thread and
  // never during replay.
}

void stinger_plugin_impl::on_irreversible_block( uint32_t block_num )
{
  handler_scope scope( *this );
  if( !scope )
    return;

  // emit irreversible{ block_num } - consumers holding matches for safe
  // delivery release everything <= block_num.
  //
  // Not yet emitted. When it is, gate it: this fires for every block of a
  // replay (migrate_irreversible_state) on the main thread, before the
  // chain's write queue exists, while matches are suppressed. Appending from
  // here would pin the page buffer's writer-thread check to the main thread
  // and emit irreversible frames for blocks no match frame refers to. The
  // guard is a live flag set in plugin_startup, which runs after the chain
  // plugin's replay; is_validating_block() is false here even when live.
}

void stinger_plugin_impl::on_switch_fork( uint32_t new_head_block_num )
{
  handler_scope scope( *this );
  if( !scope )
    return;

  // emit switch_fork{ new_head_block_num } - consumers discard (or compensate
  // for) everything with block_num > new_head_block_num; the new branch is
  // re-applied afterwards and its ops are matched fresh.
  //
  // Not yet emitted. When it is, gate it on the same live flag as
  // on_irreversible_block: this also fires from load_state_initial_data at
  // startup on the main thread, and when live it fires between the pops and
  // apply_block_extended, where is_validating_block() is false.
}

void stinger_plugin_impl::sink_loop()
{
  try
  {
    // fc's log context prints the current fc::thread's name; current() wraps
    // a foreign thread on first use, and set_name is direct on the current thread
    fc::thread::current().set_name( "stinger_sink" );

    std::byte* page = nullptr;
    auto idle = SINK_IDLE_MIN;
    for( ;; )
    {
      // Observed BEFORE take(): stop() follows the final rotate() under the
      // same mutex, so a stop seen here followed by an empty take() means
      // the ring is drained and nothing more can arrive. The other order
      // could miss the last page.
      const bool stopped = _pages->stopped();
      page = _pages->take( page );
      if( page )
      {
        idle = SINK_IDLE_MIN;
        // a decode tripwire or a sink bug drops this page, not hived: an
        // exception escaping a std::thread is std::terminate
        try
        {
          deliver_page( page );
        }
        catch( const fc::exception& e )
        {
          elog( "stinger: sink dropped a page: ${e}", ("e", e.to_detail_string()) );
        }
        catch( const std::exception& e )
        {
          elog( "stinger: sink dropped a page: ${e}", ("e", e.what()) );
        }
        catch( ... )
        {
          elog( "stinger: sink dropped a page: unknown exception" );
        }
        continue;
      }
      if( stopped )
        break;  // the loop only ends on an empty take: no page is held
      // nothing ready: the producer never signals, poll with a backoff
      std::this_thread::sleep_for( idle );
      idle = std::min( idle * 2, SINK_IDLE_MAX );
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

void stinger_plugin_impl::deliver_page( const std::byte* page )
{
  page_reader reader( page, _pages->page_size() );
  // indices only ever grow (dropped pages leave gaps); an index-arithmetic
  // bug on the producer's side shows up here
  FC_ASSERT( _pages_consumed == 0 || reader.header().page_index > _last_page_index,
    "stinger: page ${i} delivered after page ${l}", ("i", reader.header().page_index)("l", _last_page_index) );
  _last_page_index = reader.header().page_index;
  ++_pages_consumed;

  while( reader.more() )
  {
    const frame_view f = reader.next();
    switch( f.kind() )
    {
      case frame_kind::match:
        deliver_match( decode_match( f ) );
        break;
      case frame_kind::block_applied:
      case frame_kind::irreversible:
      case frame_kind::switch_fork:
        break;  // not emitted yet
      default:
        FC_THROW_EXCEPTION( fc::assert_exception, "stinger: unknown frame kind ${k} in page ${p}",
          ("k", f.header.kind)("p", reader.header().page_index) );
    }
  }
}

void stinger_plugin_impl::deliver_match( const match_frame& m )
{
  // the only sink for now is the log, one line per matched rule
  fc::variant op_body;
  if( m.body_omitted )
    op_body = "<omitted: operation exceeds the page capacity>";
  else
    fc::to_variant( m.unpack_op(), op_body );

  for( size_t i = 0; i < m.rule_count; ++i )
  {
    const uint64_t id   = m.rule_id( i );
    const auto     rule = _rules_by_id.find( id );
    if( rule == _rules_by_id.end() )
    {
      elog( "stinger: matched subscription id ${i} has no rule", ("i", id) );
      continue;
    }
    ilog( "stinger: match rule=${r} block=${b} trx_id=${t} trx_in_block=${tib} op_in_trx=${oit} virtual_op=${v} op=${op}",
      ("r", rule->second->name)("b", m.coords.block_num)("t", m.coords.trx_id)
      ("tib", m.coords.trx_in_block)("oit", m.coords.op_in_trx)("v", m.coords.virtual_op)("op", op_body) );
  }
}

void stinger_plugin_impl::stop_sink()
{
  if( !_sink.joinable() )
    return;  // never started, or already stopped

  // 1. no handler entered from here on touches producer state
  _stopping.store( true, std::memory_order_seq_cst );
  // 2. no new handler invocations
  hive::utilities::disconnect_signal( post_apply_operation_connection );
  hive::utilities::disconnect_signal( post_apply_block_connection );
  hive::utilities::disconnect_signal( fail_apply_block_connection );
  hive::utilities::disconnect_signal( irreversible_block_connection );
  hive::utilities::disconnect_signal( switch_fork_connection );
  // 3. wait out the invocations already dispatched - a finite set after 2.
  // seq_cst, not acquire: the store-buffering argument needs all four
  // operations in the one total order, or this load may still see 0 after
  // a handler's increment that preceded its own _stopping check
  while( _in_handler.load( std::memory_order_seq_cst ) != 0 )
    std::this_thread::yield();
  // 4. this thread is the producer now: hand over the partial current page
  _pages->rotate();
  // 5. the stream has ended; set after the final page was pushed
  _pages->stop();
  // 6. the sink exits once it has observed the stop and drained the ring
  _sink.join();

  const page_buffer::stats_t s = _pages->stats();
  if( _sink_failed.load( std::memory_order_relaxed ) )
    elog( "stinger: sink had died - ${p} page(s) published, ${c} consumed, ${d} dropped, ${f} frame(s), ${b} bytes",
      ("p", s.pages_published)("c", _pages_consumed)("d", s.pages_dropped)("f", s.frames)("b", s.bytes) );
  else
    ilog( "stinger: sink stopped - ${p} page(s) published, ${c} consumed, ${d} dropped, ${f} frame(s), ${b} bytes",
      ("p", s.pages_published)("c", _pages_consumed)("d", s.pages_dropped)("f", s.frames)("b", s.bytes) );
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
    for( const auto& rule : my->rules.rules )
    {
      FC_ASSERT( betree_insert( my->_matcher.get(), rule.id, rule.match.c_str() ),
        "stinger: rule \"${n}\" (id ${i}) failed to parse: ${e}",
        ("n", rule.name)("i", rule.id)("e", rule.match) );
      my->_rules_by_id.emplace( rule.id, &rule );
    }
    ilog( "stinger: subscription index built, ${n} rule(s)", ("n", my->rules.rules.size()) );

    // the pages and the sink thread must exist before the handlers connect,
    // for the same reason as the index; the sink reads _rules_by_id, which
    // is complete by now and never changes again
    my->_pages   = std::make_unique< page_buffer >( detail::STINGER_PAGE_SIZE, detail::STINGER_PAGE_COUNT );
    my->_encoder = std::make_unique< frame_encoder >( my->_pages->capacity() );
    my->_sink    = std::thread( [this]() { my->sink_loop(); } );
    ilog( "stinger: sink started, ${n} page(s) of ${s} bytes",
      ("n", my->_pages->page_count() + 2)("s", my->_pages->page_size()) );

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
