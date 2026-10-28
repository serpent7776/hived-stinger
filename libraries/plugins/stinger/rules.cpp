#include <hive/plugins/stinger/rules.hpp>

#include <hive/protocol/operations.hpp>

#include <fc/crypto/city.hpp>
#include <fc/exception/exception.hpp>
#include <fc/io/json.hpp>
#include <fc/reflect/variant.hpp>

#include <algorithm>
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

struct operation_type_name
{
  std::string& name;
  explicit operation_type_name( std::string& n ) : name( n ) {}
  typedef void result_type;
  template< typename T > void operator()( const T& ) const { name = fc::get_typename< T >::name(); }
};

/// operation name (with and without the `_operation` suffix) -> op.which()
const std::map< std::string, int64_t >& operation_type_ids()
{
  static const std::map< std::string, int64_t > ids = []()
  {
    std::map< std::string, int64_t > result;
    for( int64_t i = 0; i < hive::protocol::operation::count(); ++i )
    {
      std::string full;
      operation_type_name visitor( full );
      hive::protocol::operation op( i, visitor );
      // hive::protocol::transfer_operation -> transfer_operation
      const std::string name = full.substr( full.find_last_of( ':' ) + 1 );
      result.emplace( name, i );
      const std::string suffix = "_operation";
      if( name.size() > suffix.size() && name.compare( name.size() - suffix.size(), suffix.size(), suffix ) == 0 )
        result.emplace( name.substr( 0, name.size() - suffix.size() ), i );
    }
    return result;
  }();
  return ids;
}

// the lexer's classes, spelled out: the C locale functions would follow a
// global locale the lexer does not
bool is_letter( char c ) { return ( c >= 'a' && c <= 'z' ) || ( c >= 'A' && c <= 'Z' ); }
bool is_digit( char c ) { return c >= '0' && c <= '9'; }
bool is_identifier_start( char c ) { return is_letter( c ) || c == '_'; }
bool is_identifier_char( char c ) { return is_letter( c ) || is_digit( c ) || c == '_' || c == '-'; }
bool is_number_char( char c ) { return is_digit( c ) || c == '.'; }

} // anonymous namespace

std::string resolve_operation_names( const std::string& match )
{
  // a scanner shaped like the be-tree lexer: string literals in either quote
  // with backslash escapes, numbers, and identifiers [a-zA-Z_][a-zA-Z0-9_-]*;
  // only an identifier token that names an operation is rewritten
  const auto& ids = operation_type_ids();
  std::string out;
  out.reserve( match.size() );
  size_t i = 0;
  while( i < match.size() )
  {
    const char c = match[ i ];
    size_t j = i + 1;
    if( c == '"' || c == '\'' )
    {
      // an unterminated literal runs to the end; the parser reports it
      while( j < match.size() && match[ j ] != c )
        j += ( match[ j ] == '\\' ) ? 2 : 1;
      j = std::min( j + 1, match.size() );
    }
    else if( is_number_char( c ) )
    {
      while( j < match.size() && is_number_char( match[ j ] ) )
        ++j;
    }
    else if( is_identifier_start( c ) )
    {
      while( j < match.size() && is_identifier_char( match[ j ] ) )
        ++j;
      auto found = ids.find( match.substr( i, j - i ) );
      if( found != ids.end() )
      {
        // the number must stay its own token when re-lexed: `2transfer` is
        // two tokens the parser rejects, and must not come out as `22`
        if( !out.empty() && is_number_char( out.back() ) )
          out += ' ';
        out += std::to_string( found->second );
        if( j < match.size() && is_number_char( match[ j ] ) )
          out += ' ';
        i = j;
        continue;
      }
    }
    out.append( match, i, j - i );
    i = j;
  }
  return out;
}

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
      rule.expr = resolve_operation_names( rule.match );
      FC_ASSERT( rule.enrich.empty(), "${w} uses \"enrich\", which is not implemented yet", ("w", what) );
    }

    return config;
  } FC_CAPTURE_AND_RETHROW( (path.generic_string()) )
}

} } } // hive::plugins::stinger
