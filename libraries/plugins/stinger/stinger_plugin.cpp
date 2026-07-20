#include <hive/chain/hive_fwd.hpp>

#include <hive/plugins/stinger/stinger_plugin.hpp>

#include <hive/chain/database.hpp>
#include <hive/chain/notifications.hpp>

#include <hive/utilities/signal.hpp>

#define STINGER_RULES_FILE_KEY "stinger-rules-file"

namespace hive { namespace plugins { namespace stinger {

using namespace hive::chain;

namespace detail {

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
  // engine ingestion point: discriminate the operation and feed it as a fact
  // tagged with note.block; ops during reindex are skipped by design
  if( _db.is_replaying_block() )
    return;
}

void stinger_plugin_impl::on_post_apply_block( const block_notification& note )
{
  // block boundary: acts as the engine's clock tick (window expiry, absence rules)
}

void stinger_plugin_impl::on_irreversible_block( uint32_t block_num )
{
  // finalize facts with block_num <= block_num: drop retraction bookkeeping,
  // release deliveries gated on irreversibility
}

void stinger_plugin_impl::on_switch_fork( uint32_t new_head_block_num )
{
  // retract all facts with block_num > new_head_block_num
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
