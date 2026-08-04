#include <hive/chain/hive_fwd.hpp>

#include <hive/plugins/stinger/stinger_plugin.hpp>

#include <hive/chain/database.hpp>
#include <hive/chain/notifications.hpp>

#include <hive/utilities/signal.hpp>

#include <memory>

#define STINGER_RULES_FILE_KEY "stinger-rules-file"

namespace hive { namespace plugins { namespace stinger {

using namespace hive::chain;

namespace detail {

/**
 * Stateless stream emitter: matches each operation against the be-tree rule
 * index and forwards results plus chain lifecycle signals as one ordered event
 * stream. Fork handling is the consumer's job; the stream carries everything
 * needed for it:
 *
 *   match          { rule_id, block_num, trx_in_block, op_in_trx, virtual_op }
 *   block_applied  { block_num, block_id }   - seals a block; carries the id
 *                                              that identifies its branch
 *   switch_fork    { new_head_block_num }    - discard held state above N
 *   irreversible   { block_num }             - everything <= N is final
 *
 * Events are emitted in signal order on the writer thread; sinks must preserve
 * that order per subscriber. A consumer wanting safe delivery buffers matches
 * per block until irreversible and drops them on switch_fork; one wanting
 * lowest latency acts on match and compensates on switch_fork.
 */
class stinger_plugin_impl
{
public:
  stinger_plugin_impl( appbase::application& app ) :
    _chain_plugin( app.get_plugin< hive::plugins::chain::chain_plugin >() ),
    _db( _chain_plugin.db() ) {}
  ~stinger_plugin_impl() {}

  void on_post_apply_operation( const operation_notification& note );
  void on_post_apply_block( const block_notification& note );
  void on_irreversible_block( uint32_t block_num );
  void on_switch_fork( uint32_t new_head_block_num );

  plugins::chain::chain_plugin& _chain_plugin;
  chain::database&              _db;
  fc::path                      rules_file;

  chain::database::signal_connection_ptr post_apply_operation_connection;
  chain::database::signal_connection_ptr post_apply_block_connection;
  chain::database::signal_connection_ptr irreversible_block_connection;
  chain::database::signal_connection_ptr switch_fork_connection;
};

void stinger_plugin_impl::on_post_apply_operation( const operation_notification& note )
{
  // build a be-tree event from the operation (real and virtual alike) -
  // note.op.which() as the op-type attribute, operation_get_impacted_accounts
  // as a string-list attribute, amounts/symbols as numeric attributes - run
  // betree_search and emit match{ rule_id, note.block, note.trx_in_block,
  // note.op_in_trx, note.virtual_op } for each matched rule. The block id of
  // the containing block is not known yet; the block_applied event that
  // follows the block's matches carries it.
  //
  // Only ops that are part of an actual block may produce matches;
  // pending/one-tx application rolls back silently and must not appear on the
  // stream. Ops during reindex are skipped by design.
  if( !_db.is_processing_block() || _db.is_replaying_block() )
    return;
}

void stinger_plugin_impl::on_post_apply_block( const block_notification& note )
{
  // emit block_applied{ note.block_num, note.block_id } - seals the block's
  // matches, binds them to a branch, and tells consumers the stream has
  // advanced (a block with no matches is distinguishable from a stream that
  // has not reached it). Natural flush point for batched outbound sinks.
  if( _db.is_replaying_block() )
    return;

  dlog( "stinger: block ${b} ts=${ts}",
    ("b", note.block_num)("ts", note.full_block->get_block_header().timestamp) );
}

void stinger_plugin_impl::on_irreversible_block( uint32_t block_num )
{
  // emit irreversible{ block_num } - consumers holding matches for safe
  // delivery release everything <= block_num.
}

void stinger_plugin_impl::on_switch_fork( uint32_t new_head_block_num )
{
  // emit switch_fork{ new_head_block_num } - consumers discard (or compensate
  // for) everything with block_num > new_head_block_num; the new branch is
  // re-applied afterwards and its ops are matched fresh.
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

    if( options.count( STINGER_RULES_FILE_KEY ) )
      my->rules_file = options.at( STINGER_RULES_FILE_KEY ).as< std::string >();

    my->post_apply_operation_connection = my->_db.add_post_apply_operation_handler(
      [&]( const operation_notification& note ) { try { my->on_post_apply_operation( note ); } FC_LOG_AND_RETHROW() }, *this, 0 );
    my->post_apply_block_connection = my->_db.add_post_apply_block_handler(
      [&]( const block_notification& note ) { try { my->on_post_apply_block( note ); } FC_LOG_AND_RETHROW() }, *this, 0 );
    my->irreversible_block_connection = my->_db.add_irreversible_block_handler(
      [&]( uint32_t block_num ) { try { my->on_irreversible_block( block_num ); } FC_LOG_AND_RETHROW() }, *this, 0 );
    my->switch_fork_connection = my->_db.add_switch_fork_handler(
      [&]( uint32_t new_head_block_num ) { try { my->on_switch_fork( new_head_block_num ); } FC_LOG_AND_RETHROW() }, *this, 0 );

    ilog( "stinger: plugin_initialize() end" );
  } FC_CAPTURE_AND_RETHROW()
}

void stinger_plugin::plugin_startup()
{
  try
  {
    ilog( "stinger: plugin_startup() begin" );
    // build the be-tree subscription index from rules_file: declare the typed
    // attribute domains (op type, impacted accounts, amounts, ...), then insert
    // each rule's boolean expression, keeping rule_id -> subscription id. The
    // index is in-memory only and cheap to rebuild here on every startup.
    ilog( "stinger: plugin_startup() end" );
  } FC_CAPTURE_AND_RETHROW()
}

void stinger_plugin::plugin_shutdown()
{
  hive::utilities::disconnect_signal( my->post_apply_operation_connection );
  hive::utilities::disconnect_signal( my->post_apply_block_connection );
  hive::utilities::disconnect_signal( my->irreversible_block_connection );
  hive::utilities::disconnect_signal( my->switch_fork_connection );
}

} } } // hive::plugins::stinger
