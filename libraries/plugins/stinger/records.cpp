#include <hive/plugins/stinger/records.hpp>

#include <hive/protocol/misc_utilities.hpp>
#include <hive/protocol/operations.hpp>

#include <fc/io/json.hpp>
#include <fc/variant_object.hpp>

namespace hive { namespace plugins { namespace stinger {

namespace {

/// render `obj` as one line into `out`
void render( const fc::mutable_variant_object& obj, std::string& out )
{
  out = fc::json::to_string( fc::variant( obj ) );
  out += '\n';
}

} // anonymous namespace

record_writer::record_writer( size_t line_limit )
  : _line_limit( line_limit )
{
  FC_ASSERT( line_limit > 0, "the line limit must be positive" );
}

const std::string& record_writer::hello( const stream_position& position )
{
  // a stream that has not started stands nowhere: null
  fc::variant head;
  if( position.head_block != 0 )
    head = fc::variant( fc::mutable_variant_object()( "block", position.head_block )( "block_id", position.head_id ) );
  fc::variant irreversible;
  if( position.irreversible )
    irreversible = fc::variant( *position.irreversible );

  fc::mutable_variant_object obj;
  obj( "kind", "hello" )( "seq", last_seq() )( "version", FORMAT_VERSION )
     ( "delivered", fc::mutable_variant_object()( "head", std::move( head ) )( "irreversible", std::move( irreversible ) ) );
  render( obj, _line );
  return _line;
}

const std::string& record_writer::match( delivery_class cls, const std::vector< std::string_view >& rules, const match_frame& m )
{
  // database_api form, whatever the thread's default is
  protocol::serialization_mode_controller::mode_guard guard( protocol::transaction_serialization_type::hf26 );

  fc::variants rule_names;
  rule_names.reserve( rules.size() );
  for( const auto& name : rules )
    rule_names.emplace_back( std::string( name ) );

  fc::mutable_variant_object obj;
  obj( "kind", "match" )
     ( "seq", _next_seq )
     ( "delivery", cls )
     ( "rules", std::move( rule_names ) )
     ( "block", m.coords.block_num )
     ( "timestamp", m.coords.timestamp )
     ( "trx_id", m.coords.trx_id )
     ( "trx_in_block", m.coords.trx_in_block )
     ( "op_in_trx", m.coords.op_in_trx )
     ( "virtual_op", m.coords.virtual_op );

  if( m.body_omitted )
  {
    obj( "op", fc::variant() )( "op_omitted", "frame budget" );
    render( obj, _line );
  }
  else
  {
    fc::variant op;
    fc::to_variant( m.unpack_op(), op );
    obj( "op", std::move( op ) );
    render( obj, _line );
    if( _line.size() > _line_limit )
    {
      // the body is what made it long; the coordinates always fit.
      // set() replaces the key; operator() would append a second one
      obj.set( "op", fc::variant() )( "op_omitted", "line limit" );
      render( obj, _line );
    }
  }
  ++_next_seq;
  return _line;
}

const std::string& record_writer::block_applied( uint32_t block_num, const protocol::block_id_type& block_id )
{
  return marker( "block_applied", block_num, &block_id );
}

const std::string& record_writer::irreversible( uint32_t block_num )
{
  return marker( "irreversible", block_num, nullptr );
}

const std::string& record_writer::switch_fork( uint32_t block_num, const protocol::block_id_type& block_id )
{
  return marker( "switch_fork", block_num, &block_id );
}

const std::string& record_writer::marker( const char* kind, uint32_t block_num, const protocol::block_id_type* block_id )
{
  fc::mutable_variant_object obj;
  obj( "kind", kind )( "seq", _next_seq )( "block", block_num );
  if( block_id )
    obj( "block_id", *block_id );
  render( obj, _line );
  ++_next_seq;
  return _line;
}

} } } // hive::plugins::stinger
