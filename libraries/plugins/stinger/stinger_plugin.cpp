#include <hive/chain/hive_fwd.hpp>

#include <hive/plugins/stinger/rules.hpp>
#include <hive/plugins/stinger/stinger_plugin.hpp>

#include <hive/chain/database.hpp>
#include <hive/chain/notifications.hpp>

#include <hive/protocol/asset.hpp>
#include <hive/protocol/forward_impacted.hpp>
#include <hive/protocol/operations.hpp>

#include <hive/utilities/signal.hpp>

extern "C" {
#include <betree.h>
}

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
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

  /// event variable indices assigned by declare_rule_attributes for _matcher
  event_slots _slots{};

  /// subscription id -> rule, for turning report ids back into rule names;
  /// built in plugin_startup together with the index
  std::map< uint64_t, const rule_definition* > _rules_by_id;

  chain::database::signal_connection_ptr post_apply_operation_connection;
  chain::database::signal_connection_ptr post_apply_block_connection;
  chain::database::signal_connection_ptr irreversible_block_connection;
  chain::database::signal_connection_ptr switch_fork_connection;
};

void stinger_plugin_impl::on_post_apply_operation( const operation_notification& note )
{
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

  // emit match{ rule name, block_num, trx_id, trx_in_block, op_in_trx,
  // virtual_op, op body } per matched rule. The block id of the containing
  // block is not known yet; the block_applied event that follows the block's
  // matches carries it. Sinks are not implemented yet - matches go to the log.
  fc::variant op_body;
  fc::to_variant( note.op, op_body );
  for( size_t i = 0; i < matches->matched; ++i )
  {
    auto rule = _rules_by_id.find( matches->subs[ i ] );
    FC_ASSERT( rule != _rules_by_id.end(),
      "stinger: matched subscription id ${i} has no rule", ("i", matches->subs[ i ]) );
    ilog( "stinger: match rule=${r} block=${b} trx_id=${t} trx_in_block=${tib} op_in_trx=${oit} virtual_op=${v} op=${op}",
      ("r", rule->second->name)("b", note.block)("t", note.trx_id)
      ("tib", note.trx_in_block)("oit", note.op_in_trx)("v", note.virtual_op)("op", op_body) );
  }
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
    my->_slots = detail::declare_rule_attributes( my->_matcher.get() );
    for( const auto& rule : my->rules.rules )
    {
      FC_ASSERT( betree_insert( my->_matcher.get(), rule.id, rule.match.c_str() ),
        "stinger: rule \"${n}\" (id ${i}) failed to parse: ${e}",
        ("n", rule.name)("i", rule.id)("e", rule.match) );
      my->_rules_by_id.emplace( rule.id, &rule );
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
