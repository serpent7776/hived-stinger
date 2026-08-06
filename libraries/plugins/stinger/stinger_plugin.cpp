#include <hive/chain/hive_fwd.hpp>

#include <hive/plugins/stinger/rules.hpp>
#include <hive/plugins/stinger/stinger_plugin.hpp>

#include <hive/chain/database.hpp>
#include <hive/chain/notifications.hpp>

#include <hive/protocol/operations.hpp>

#include <hive/utilities/signal.hpp>

extern "C" {
#include <betree.h>
}

#include <cstdint>
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
  rules_config                  rules;

  /// be-tree subscription index; built once in plugin_startup, then read-only
  /// on the writer thread (this be-tree fork has no subscription deletion, so
  /// dynamic rule reload means rebuilding the index).
  std::unique_ptr< betree, decltype( &betree_free ) > _matcher{ nullptr, &betree_free };

  chain::database::signal_connection_ptr post_apply_operation_connection;
  chain::database::signal_connection_ptr post_apply_block_connection;
  chain::database::signal_connection_ptr irreversible_block_connection;
  chain::database::signal_connection_ptr switch_fork_connection;
};

void stinger_plugin_impl::on_post_apply_operation( const operation_notification& note )
{
  // build a be-tree event from the operation (real and virtual alike) -
  // note.op.which() as the op-type attribute, operation_get_impacted_accounts
  // as a string-list attribute, amounts/symbols as numeric attributes
  // (betree_make_event + betree_make_*_variable) - run betree_search_with_event
  // on _matcher and emit match{ rule_id, note.block, note.trx_in_block,
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

/**
 * Attribute vocabulary rule exprs are written against. The event builder in
 * on_post_apply_operation must stamp exactly these attributes - extend both
 * together.
 */
void declare_rule_attributes( betree* tree )
{
  // op.which() over hive::protocol::operation (virtual ops included)
  betree_add_integer_variable( tree, "op_type", false, 0, hive::protocol::operation::count() - 1 );
  // operation_get_impacted_accounts of the op
  betree_add_string_list_variable( tree, "impacted_accounts", false, SIZE_MAX );
  // primary asset of ops that carry one: amount in satoshis + symbol name;
  // undefined for ops without an asset
  betree_add_integer_variable( tree, "amount", true, 0, INT64_MAX );
  betree_add_string_variable( tree, "symbol", true, SIZE_MAX );
  // custom_json_operation id; undefined for other op types
  betree_add_string_variable( tree, "custom_id", true, SIZE_MAX );
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
    my->_matcher.reset( betree_make() );
    FC_ASSERT( my->_matcher, "stinger: failed to create be-tree matcher" );
    detail::declare_rule_attributes( my->_matcher.get() );
    for( const auto& rule : my->rules.rules )
    {
      FC_ASSERT( betree_insert( my->_matcher.get(), rule.id, rule.match.c_str() ),
        "stinger: rule \"${n}\" (id ${i}) failed to parse: ${e}",
        ("n", rule.name)("i", rule.id)("e", rule.match) );
    }
    ilog( "stinger: subscription index built, ${n} rule(s)", ("n", my->rules.rules.size()) );
    ilog( "stinger: plugin_startup() end" );
  } FC_CAPTURE_AND_RETHROW()
}

void stinger_plugin::plugin_shutdown()
{
  hive::utilities::disconnect_signal( my->post_apply_operation_connection );
  hive::utilities::disconnect_signal( my->post_apply_block_connection );
  hive::utilities::disconnect_signal( my->irreversible_block_connection );
  hive::utilities::disconnect_signal( my->switch_fork_connection );
  my->_matcher.reset();
}

} } } // hive::plugins::stinger
