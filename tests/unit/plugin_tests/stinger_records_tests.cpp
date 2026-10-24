#ifdef IS_TEST_NET
#include <boost/test/unit_test.hpp>

#include <hive/plugins/stinger/frame.hpp>
#include <hive/plugins/stinger/records.hpp>
#include <hive/plugins/stinger/rules.hpp>

#include <hive/protocol/operations.hpp>

#include <fc/io/json.hpp>
#include <fc/variant_object.hpp>

#include <string>
#include <string_view>
#include <vector>

using namespace hive::plugins::stinger;
using namespace hive::protocol;

namespace {

constexpr size_t TEST_BUDGET = 64 * 1024;
constexpr size_t NO_LIMIT    = 16 * 1024 * 1024;

match_coords sample_coords()
{
  match_coords c;
  c.block_num    = 123456;
  c.trx_in_block = 7;
  c.op_in_trx    = 2;
  c.timestamp    = fc::time_point_sec( 1700000000 );
  c.virtual_op   = false;
  c.trx_id       = fc::ripemd160::hash( std::string( "trx" ) );
  return c;
}

operation sample_transfer()
{
  transfer_operation op;
  op.from   = "alice";
  op.to     = "bob";
  op.amount = asset( 1000, HIVE_SYMBOL );
  op.memo   = "memo";
  return op;
}

/// a custom_json op whose packed size is controlled through the json length
operation custom_json_of_size( size_t json_len )
{
  custom_json_operation op;
  op.id   = "x";
  op.json = std::string( json_len, 'j' );
  return op;
}

/// encode a match frame for `op` with one rule id and decode it back; the
/// buffer must outlive the returned view
match_frame encode( std::vector< std::byte >& buf, const operation& op, size_t budget = TEST_BUDGET )
{
  frame_encoder enc( budget );
  const uint64_t ids[] = { 11, 22 };
  const match_coords c = sample_coords();
  enc.encode_match( c, ids, 2, op );
  buf.assign( enc.data(), enc.data() + enc.size() );
  frame_reader reader( buf.data(), buf.size() );
  return decode_match( reader.next() );
}

/// parse one line back; it must end with exactly one newline
fc::variant_object parse( const std::string& line )
{
  BOOST_REQUIRE( !line.empty() );
  BOOST_REQUIRE_EQUAL( line.back(), '\n' );
  BOOST_REQUIRE_EQUAL( line.find( '\n' ), line.size() - 1 );
  fc::variant v = fc::json::from_string( line, fc::json::format_validation_mode::full );
  BOOST_REQUIRE( v.is_object() );
  return v.get_object();
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE( stinger_records )

BOOST_AUTO_TEST_CASE( hello_carries_the_version_and_the_next_seq )
{
  record_writer w( NO_LIMIT );
  BOOST_CHECK_EQUAL( w.next_seq(), 1u );
  auto rec = parse( w.hello() );
  BOOST_CHECK_EQUAL( rec[ "kind" ].as_string(), "hello" );
  BOOST_CHECK_EQUAL( rec[ "version" ].as_uint64(), record_writer::FORMAT_VERSION );
  BOOST_CHECK_EQUAL( rec[ "next_seq" ].as_uint64(), 1u );
  BOOST_CHECK( !rec.contains( "seq" ) );
  // a greeting does not consume a seq
  BOOST_CHECK_EQUAL( w.next_seq(), 1u );
  w.irreversible( 5 );
  BOOST_CHECK_EQUAL( parse( w.hello() )[ "next_seq" ].as_uint64(), 2u );
}

BOOST_AUTO_TEST_CASE( match_record )
{
  std::vector< std::byte > buf;
  const match_frame m = encode( buf, sample_transfer() );
  record_writer w( NO_LIMIT );
  std::vector< std::string_view > rules{ "big-transfer", "alice" };

  auto rec = parse( w.match( delivery_class::head, rules, m ) );
  BOOST_CHECK_EQUAL( rec[ "kind" ].as_string(), "match" );
  BOOST_CHECK_EQUAL( rec[ "seq" ].as_uint64(), 1u );
  BOOST_CHECK_EQUAL( rec[ "delivery" ].as_string(), "head" );
  BOOST_REQUIRE( rec[ "rules" ].is_array() );
  BOOST_REQUIRE_EQUAL( rec[ "rules" ].get_array().size(), 2u );
  BOOST_CHECK_EQUAL( rec[ "rules" ].get_array()[ 0 ].as_string(), "big-transfer" );
  BOOST_CHECK_EQUAL( rec[ "rules" ].get_array()[ 1 ].as_string(), "alice" );
  BOOST_CHECK_EQUAL( rec[ "block" ].as_uint64(), 123456u );
  BOOST_CHECK_EQUAL( rec[ "timestamp" ].as_string(), "2023-11-14T22:13:20" );
  BOOST_CHECK_EQUAL( rec[ "trx_id" ].as_string(), sample_coords().trx_id.str() );
  BOOST_CHECK_EQUAL( rec[ "trx_in_block" ].as_int64(), 7 );
  BOOST_CHECK_EQUAL( rec[ "op_in_trx" ].as_int64(), 2 );
  BOOST_CHECK_EQUAL( rec[ "virtual_op" ].as_bool(), false );
  BOOST_CHECK( !rec.contains( "op_omitted" ) );

  // the operation in database_api form, not the legacy array
  BOOST_REQUIRE( rec[ "op" ].is_object() );
  const auto& op = rec[ "op" ].get_object();
  BOOST_CHECK_EQUAL( op[ "type" ].as_string(), "transfer_operation" );
  BOOST_REQUIRE( op[ "value" ].is_object() );
  BOOST_CHECK_EQUAL( op[ "value" ].get_object()[ "from" ].as_string(), "alice" );
  BOOST_CHECK_EQUAL( op[ "value" ].get_object()[ "memo" ].as_string(), "memo" );
  BOOST_CHECK( op[ "value" ].get_object()[ "amount" ].is_object() );   // nai form

  // the other class, same frame: a record of its own with the next seq
  rules = { "safe" };
  rec = parse( w.match( delivery_class::irreversible, rules, m ) );
  BOOST_CHECK_EQUAL( rec[ "seq" ].as_uint64(), 2u );
  BOOST_CHECK_EQUAL( rec[ "delivery" ].as_string(), "irreversible" );
  BOOST_CHECK_EQUAL( rec[ "rules" ].get_array().size(), 1u );
  BOOST_CHECK_EQUAL( w.next_seq(), 3u );
}

BOOST_AUTO_TEST_CASE( match_record_renders_in_database_api_form_whatever_the_thread_default )
{
  std::vector< std::byte > buf;
  const match_frame m = encode( buf, sample_transfer() );
  record_writer w( NO_LIMIT );
  std::vector< std::string_view > rules{ "r" };

  serialization_mode_controller::mode_guard guard( transaction_serialization_type::legacy );
  auto rec = parse( w.match( delivery_class::head, rules, m ) );
  BOOST_REQUIRE( rec[ "op" ].is_object() );
  BOOST_CHECK_EQUAL( rec[ "op" ].get_object()[ "type" ].as_string(), "transfer_operation" );
}

BOOST_AUTO_TEST_CASE( body_omitted_by_the_frame )
{
  // a budget the frame's overhead fills: the encoder leaves the body out
  std::vector< std::byte > buf;
  const match_frame m = encode( buf, sample_transfer(), match_frame_overhead( 2 ) + 1 );
  BOOST_REQUIRE( m.body_omitted );

  record_writer w( NO_LIMIT );
  std::vector< std::string_view > rules{ "r" };
  auto rec = parse( w.match( delivery_class::irreversible, rules, m ) );
  BOOST_CHECK( rec[ "op" ].is_null() );
  BOOST_CHECK_EQUAL( rec[ "op_omitted" ].as_string(), "frame budget" );
  BOOST_CHECK_EQUAL( rec[ "block" ].as_uint64(), 123456u );
}

BOOST_AUTO_TEST_CASE( body_omitted_by_the_line_limit )
{
  std::vector< std::byte > buf;
  const match_frame m = encode( buf, custom_json_of_size( 8000 ) );
  BOOST_REQUIRE( !m.body_omitted );
  std::vector< std::string_view > rules{ "r" };

  // fits: the body is there
  {
    record_writer w( 16 * 1024 );
    const std::string& line = w.match( delivery_class::irreversible, rules, m );
    BOOST_CHECK_LE( line.size(), 16u * 1024 );
    auto rec = parse( line );
    BOOST_CHECK( rec[ "op" ].is_object() );
    BOOST_CHECK( !rec.contains( "op_omitted" ) );
  }
  // does not fit: the same record without the body, under the limit
  {
    record_writer w( 4 * 1024 );
    const std::string& line = w.match( delivery_class::irreversible, rules, m );
    BOOST_CHECK_LE( line.size(), 4u * 1024 );
    auto rec = parse( line );
    BOOST_CHECK( rec[ "op" ].is_null() );
    BOOST_CHECK_EQUAL( rec[ "op_omitted" ].as_string(), "line limit" );
    BOOST_CHECK_EQUAL( rec[ "seq" ].as_uint64(), 1u );
    BOOST_CHECK_EQUAL( rec[ "block" ].as_uint64(), 123456u );
    BOOST_CHECK_EQUAL( w.next_seq(), 2u );
  }
}

BOOST_AUTO_TEST_CASE( markers )
{
  record_writer w( NO_LIMIT );
  const block_id_type id = fc::ripemd160::hash( std::string( "block" ) );

  auto rec = parse( w.block_applied( 10, id ) );
  BOOST_CHECK_EQUAL( rec[ "kind" ].as_string(), "block_applied" );
  BOOST_CHECK_EQUAL( rec[ "seq" ].as_uint64(), 1u );
  BOOST_CHECK_EQUAL( rec[ "block" ].as_uint64(), 10u );
  BOOST_CHECK_EQUAL( rec[ "block_id" ].as_string(), id.str() );

  rec = parse( w.irreversible( 9 ) );
  BOOST_CHECK_EQUAL( rec[ "kind" ].as_string(), "irreversible" );
  BOOST_CHECK_EQUAL( rec[ "seq" ].as_uint64(), 2u );
  BOOST_CHECK_EQUAL( rec[ "block" ].as_uint64(), 9u );
  BOOST_CHECK( !rec.contains( "block_id" ) );

  rec = parse( w.switch_fork( 8 ) );
  BOOST_CHECK_EQUAL( rec[ "kind" ].as_string(), "switch_fork" );
  BOOST_CHECK_EQUAL( rec[ "seq" ].as_uint64(), 3u );
  BOOST_CHECK_EQUAL( rec[ "block" ].as_uint64(), 8u );
  BOOST_CHECK_EQUAL( w.next_seq(), 4u );
}

BOOST_AUTO_TEST_CASE( records_are_single_lines )
{
  // a memo with a newline and a quote stays inside the JSON string
  transfer_operation op;
  op.from   = "alice";
  op.to     = "bob";
  op.amount = asset( 1, HIVE_SYMBOL );
  op.memo   = "line one\nline \"two\"";
  std::vector< std::byte > buf;
  const match_frame m = encode( buf, op );
  record_writer w( NO_LIMIT );
  std::vector< std::string_view > rules{ "r" };
  auto rec = parse( w.match( delivery_class::head, rules, m ) );
  BOOST_CHECK_EQUAL( rec[ "op" ].get_object()[ "value" ].get_object()[ "memo" ].as_string(), op.memo );
}

BOOST_AUTO_TEST_SUITE_END()
#endif
