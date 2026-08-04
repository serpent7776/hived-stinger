#pragma once
#include <hive/chain/hive_fwd.hpp>
#include <hive/plugins/chain/chain_plugin.hpp>

#include <appbase/application.hpp>

namespace hive { namespace plugins { namespace stinger {

#define HIVE_STINGER_PLUGIN_NAME "stinger"

namespace detail { class stinger_plugin_impl; }

/**
 * Real-time trigger/notification (CEP) plugin.
 *
 * Matches each operation against user-registered rules held in a be-tree
 * subscription index as blocks are applied, and emits matches immediately as
 * an ordered event stream alongside block_applied / switch_fork /
 * irreversible lifecycle events. The plugin is stateless; fork handling is
 * left to stream consumers (an at-irreversible delivery mode may be layered
 * on top later).
 */
class stinger_plugin : public appbase::plugin< stinger_plugin >
{
  public:
    stinger_plugin();
    virtual ~stinger_plugin();

    APPBASE_PLUGIN_REQUIRES( (hive::plugins::chain::chain_plugin) )

    static const std::string& name() { static std::string name = HIVE_STINGER_PLUGIN_NAME; return name; }

    virtual void set_program_options( boost::program_options::options_description& cli, boost::program_options::options_description& cfg ) override;
    virtual void plugin_initialize( const boost::program_options::variables_map& options ) override;
    virtual void plugin_startup() override;
    virtual void plugin_shutdown() override;

  private:
    std::unique_ptr< detail::stinger_plugin_impl > my;
};

} } } // hive::plugins::stinger
