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
 * Feeds blockchain operations into a pattern-matching engine and fires
 * user-registered rules as blocks are applied. Fork rollback is handled by
 * retracting facts above the fork point; irreversibility finalizes them.
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
