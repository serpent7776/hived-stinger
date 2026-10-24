#include <hive/plugins/stinger/rules.hpp>

#include <fc/crypto/city.hpp>
#include <fc/exception/exception.hpp>
#include <fc/io/json.hpp>
#include <fc/reflect/variant.hpp>

#include <map>

#include <set>
#include <string>

namespace hive { namespace plugins { namespace stinger {

namespace {

/// fc reflection silently drops unknown keys when filling a struct, which
/// would turn a typo like "exrp" into a rule that matches nothing - so key
/// sets are checked by hand before conversion.
void reject_unknown_keys( const fc::variant_object& obj, const std::set< std::string >& allowed, const std::string& what )
{
  for( const auto& entry : obj )
    FC_ASSERT( allowed.count( entry.key() ) != 0,
      "unknown key \"${k}\" in ${w}", ("k", entry.key())("w", what) );
}

void require_key( const fc::variant_object& obj, const char* key, const std::string& what )
{
  FC_ASSERT( obj.contains( key ), "missing required key \"${k}\" in ${w}", ("k", key)("w", what) );
}

void check_rule_object( const fc::variant& var, size_t index )
{
  const std::string what = "rule #" + std::to_string( index );
  FC_ASSERT( var.is_object(), "${w} is not a JSON object", ("w", what) );
  const auto& rule = var.get_object();
  reject_unknown_keys( rule, { "name", "match", "enrich", "exec", "delivery" }, what );
  require_key( rule, "name", what );
  require_key( rule, "match", what );
  require_key( rule, "exec", what );
  FC_ASSERT( rule[ "exec" ].is_object(), "exec of ${w} is not a JSON object", ("w", what) );
  const auto& exec = rule[ "exec" ].get_object();
  reject_unknown_keys( exec, { "type" }, "exec of " + what );
  require_key( exec, "type", "exec of " + what );
  // checked by hand for the message: fc's enum conversion reports a bad cast
  FC_ASSERT( exec[ "type" ].is_string(), "exec type of ${w} is not a string", ("w", what) );
  const std::string& t = exec[ "type" ].get_string();
  FC_ASSERT( t == "log" || t == "socket",
    "unknown exec type \"${t}\" in ${w} (known: log, socket)", ("t", t)("w", what) );
  if( rule.contains( "enrich" ) )
    FC_ASSERT( rule[ "enrich" ].is_array(), "enrich of ${w} is not a JSON array", ("w", what) );
  if( rule.contains( "delivery" ) )
  {
    // checked by hand for the message: fc's enum conversion reports a bad cast
    FC_ASSERT( rule[ "delivery" ].is_string(), "delivery of ${w} is not a string", ("w", what) );
    const std::string& d = rule[ "delivery" ].get_string();
    FC_ASSERT( d == "irreversible" || d == "head",
      "unknown delivery \"${d}\" in ${w} (known: irreversible, head)", ("d", d)("w", what) );
  }
}

} // anonymous namespace

rules_config load_rules_file( const fc::path& path )
{
  try
  {
    FC_ASSERT( fc::exists( path ) && !fc::is_directory( path ), "rules file does not exist" );

    fc::variant doc = fc::json::from_file( path, fc::json::format_validation_mode::full );
    FC_ASSERT( doc.is_object(), "rules file is not a JSON object" );
    reject_unknown_keys( doc.get_object(), { "rules" }, "rules file" );
    require_key( doc.get_object(), "rules", "rules file" );
    FC_ASSERT( doc.get_object()[ "rules" ].is_array(), "\"rules\" is not a JSON array" );

    const auto& rule_vars = doc.get_object()[ "rules" ].get_array();
    for( size_t i = 0; i < rule_vars.size(); ++i )
      check_rule_object( rule_vars[ i ], i );

    rules_config config = doc.as< rules_config >();

    std::map< uint64_t, std::string > ids;
    std::set< std::string >           names;
    for( size_t i = 0; i < config.rules.size(); ++i )
    {
      auto& rule = config.rules[ i ];
      FC_ASSERT( !rule.name.empty(), "rule #${i} has an empty name", ("i", i) );
      const std::string what = "rule \"" + rule.name + "\"";
      FC_ASSERT( names.insert( rule.name ).second, "duplicate rule name \"${n}\"", ("n", rule.name) );
      rule.id = fc::city_hash64( rule.name.data(), rule.name.size() );
      auto inserted = ids.emplace( rule.id, rule.name );
      FC_ASSERT( inserted.second,
        "hash collision between rule names \"${a}\" and \"${b}\" - rename one of them",
        ("a", inserted.first->second)("b", rule.name) );
      FC_ASSERT( !rule.match.empty(), "${w} has an empty match", ("w", what) );
      FC_ASSERT( rule.enrich.empty(), "${w} uses \"enrich\", which is not implemented yet", ("w", what) );
    }

    return config;
  } FC_CAPTURE_AND_RETHROW( (path.generic_string()) )
}

} } } // hive::plugins::stinger
