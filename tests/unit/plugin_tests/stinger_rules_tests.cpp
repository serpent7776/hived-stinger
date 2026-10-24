#ifdef IS_TEST_NET
#include <boost/test/unit_test.hpp>

#include <hive/plugins/stinger/rules.hpp>

#include <fc/crypto/city.hpp>
#include <fc/exception/exception.hpp>
#include <fc/filesystem.hpp>

#include <fstream>
#include <string>

using hive::plugins::stinger::exec_type;
using hive::plugins::stinger::load_rules_file;
using hive::plugins::stinger::rules_config;

namespace {

/// dump the given JSON text to a file inside dir and load it as a rules file
rules_config load_from_text( const fc::temp_directory& dir, const std::string& text )
{
  fc::path path = dir.path() / "rules.json";
  std::ofstream file( path.generic_string() );
  file << text;
  file.close();
  return load_rules_file( path );
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE( stinger_rules )

BOOST_AUTO_TEST_CASE( valid_rules_file )
{
  fc::temp_directory dir;
  rules_config config = load_from_text( dir, R"(
    {
      "rules": [
        { "name": "large-transfer", "match": "op_type = 2 and amount > 1000000",
          "exec": { "type": "log" } },
        { "name": "sl-market", "match": "op_type = 18 and custom_id = \"sm_market\"",
          "exec": { "type": "log" } }
      ]
    } )" );

  BOOST_REQUIRE_EQUAL( config.rules.size(), 2u );
  // the be-tree subscription id is derived from the rule name
  const std::string name = "large-transfer";
  BOOST_CHECK_EQUAL( config.rules[0].id, fc::city_hash64( name.data(), name.size() ) );
  BOOST_CHECK_EQUAL( config.rules[0].name, name );
  BOOST_CHECK_EQUAL( config.rules[0].match, "op_type = 2 and amount > 1000000" );
  BOOST_CHECK( config.rules[0].exec.type == exec_type::log );
  BOOST_CHECK_EQUAL( config.rules[1].name, "sl-market" );
  BOOST_CHECK( config.rules[1].exec.type == exec_type::log );
}

BOOST_AUTO_TEST_CASE( empty_rules_list_is_valid )
{
  fc::temp_directory dir;
  BOOST_CHECK_EQUAL( load_from_text( dir, R"( { "rules": [] } )" ).rules.size(), 0u );
}

BOOST_AUTO_TEST_CASE( missing_file )
{
  fc::temp_directory dir;
  BOOST_REQUIRE_THROW( load_rules_file( dir.path() / "nonexistent.json" ), fc::exception );
}

BOOST_AUTO_TEST_CASE( malformed_files )
{
  fc::temp_directory dir;
  // not JSON at all
  BOOST_REQUIRE_THROW( load_from_text( dir, "not json" ), fc::exception );
  // top level is not an object
  BOOST_REQUIRE_THROW( load_from_text( dir, R"( [ ] )" ), fc::exception );
  // no "rules" key
  BOOST_REQUIRE_THROW( load_from_text( dir, R"( { } )" ), fc::exception );
  // unknown top-level key
  BOOST_REQUIRE_THROW( load_from_text( dir, R"( { "rules": [], "extra": 1 } )" ), fc::exception );
  // "rules" is not an array
  BOOST_REQUIRE_THROW( load_from_text( dir, R"( { "rules": {} } )" ), fc::exception );
}

BOOST_AUTO_TEST_CASE( malformed_rules )
{
  fc::temp_directory dir;
  // missing match
  BOOST_REQUIRE_THROW( load_from_text( dir,
    R"( { "rules": [ { "name": "r", "exec": { "type": "log" } } ] } )" ), fc::exception );
  // unknown key catches typos fc reflection would silently drop
  BOOST_REQUIRE_THROW( load_from_text( dir,
    R"( { "rules": [ { "name": "r", "exrp": "op_type = 2", "match": "op_type = 2",
                       "exec": { "type": "log" } } ] } )" ), fc::exception );
  // empty name
  BOOST_REQUIRE_THROW( load_from_text( dir,
    R"( { "rules": [ { "name": "", "match": "op_type = 2", "exec": { "type": "log" } } ] } )" ), fc::exception );
  // empty match
  BOOST_REQUIRE_THROW( load_from_text( dir,
    R"( { "rules": [ { "name": "r", "match": "", "exec": { "type": "log" } } ] } )" ), fc::exception );
  // enrich not implemented yet
  BOOST_REQUIRE_THROW( load_from_text( dir,
    R"( { "rules": [ { "name": "r", "match": "op_type = 2", "enrich": [ { "get": "balance" } ],
                       "exec": { "type": "log" } } ] } )" ), fc::exception );
}

BOOST_AUTO_TEST_CASE( duplicate_names )
{
  fc::temp_directory dir;
  BOOST_REQUIRE_THROW( load_from_text( dir, R"(
    { "rules": [
      { "name": "a", "match": "op_type = 2", "exec": { "type": "log" } },
      { "name": "a", "match": "op_type = 3", "exec": { "type": "log" } } ] } )" ), fc::exception );
}

BOOST_AUTO_TEST_CASE( explicit_id_is_rejected )
{
  // ids are derived from names and are not part of the file format
  fc::temp_directory dir;
  BOOST_REQUIRE_THROW( load_from_text( dir,
    R"( { "rules": [ { "id": 1, "name": "r", "match": "op_type = 2", "exec": { "type": "log" } } ] } )" ), fc::exception );
}

BOOST_AUTO_TEST_CASE( exec_types )
{
  fc::temp_directory dir;
  rules_config config = load_from_text( dir, R"(
    { "rules": [
      { "name": "quiet", "match": "op_type = 2", "exec": { "type": "log" } },
      { "name": "wired", "match": "op_type = 2", "exec": { "type": "socket" } } ] } )" );
  BOOST_REQUIRE_EQUAL( config.rules.size(), 2u );
  BOOST_CHECK( config.rules[0].exec.type == exec_type::log );
  BOOST_CHECK( config.rules[1].exec.type == exec_type::socket );
}

BOOST_AUTO_TEST_CASE( malformed_execs )
{
  fc::temp_directory dir;
  // unknown exec type
  BOOST_REQUIRE_THROW( load_from_text( dir,
    R"( { "rules": [ { "name": "r", "match": "op_type = 2", "exec": { "type": "carrier-pigeon" } } ] } )" ), fc::exception );
  // the enumerator's number is not accepted, only its name
  BOOST_REQUIRE_THROW( load_from_text( dir,
    R"( { "rules": [ { "name": "r", "match": "op_type = 2", "exec": { "type": 1 } } ] } )" ), fc::exception );
  // no type at all
  BOOST_REQUIRE_THROW( load_from_text( dir,
    R"( { "rules": [ { "name": "r", "match": "op_type = 2", "exec": { } } ] } )" ), fc::exception );
  // unknown exec key
  BOOST_REQUIRE_THROW( load_from_text( dir,
    R"( { "rules": [ { "name": "r", "match": "op_type = 2",
                       "exec": { "type": "log", "level": "info" } } ] } )" ), fc::exception );
}

BOOST_AUTO_TEST_CASE( delivery_class )
{
  using hive::plugins::stinger::delivery_class;
  fc::temp_directory dir;
  rules_config config = load_from_text( dir, R"(
    { "rules": [
      { "name": "safe", "match": "op_type = 2", "exec": { "type": "log" } },
      { "name": "fast", "match": "op_type = 2", "exec": { "type": "log" }, "delivery": "head" },
      { "name": "explicit", "match": "op_type = 2", "exec": { "type": "log" }, "delivery": "irreversible" } ] } )" );
  BOOST_REQUIRE_EQUAL( config.rules.size(), 3u );
  // irreversible unless asked otherwise
  BOOST_CHECK( config.rules[0].delivery == delivery_class::irreversible );
  BOOST_CHECK( config.rules[1].delivery == delivery_class::head );
  BOOST_CHECK( config.rules[2].delivery == delivery_class::irreversible );

  // unknown class
  BOOST_REQUIRE_THROW( load_from_text( dir,
    R"( { "rules": [ { "name": "r", "match": "op_type = 2", "exec": { "type": "log" }, "delivery": "soon" } ] } )" ), fc::exception );
  // the enumerator's number is not accepted, only its name
  BOOST_REQUIRE_THROW( load_from_text( dir,
    R"( { "rules": [ { "name": "r", "match": "op_type = 2", "exec": { "type": "log" }, "delivery": 1 } ] } )" ), fc::exception );
}

BOOST_AUTO_TEST_SUITE_END()
#endif
