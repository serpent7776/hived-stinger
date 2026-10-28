#pragma once

#include <fc/filesystem.hpp>
#include <fc/reflect/reflect.hpp>
#include <fc/variant.hpp>

#include <string>
#include <vector>

namespace hive { namespace plugins { namespace stinger {

/**
 * Where a rule's matches go. Fixed menu (no user code on the writer thread):
 *   log    - a structured log line per matched rule
 *   socket - a JSON line on the unix socket named by stinger-socket
 *            (records.hpp, socket_sink.hpp); the option is required when a
 *            rule uses it
 *
 * Outbound delivery (HTTP and friends) is deliberately not in the menu: the
 * producer is the writer thread, and retries, timeouts, TLS and per-target
 * backpressure are policy a consumer process handles better on the far side
 * of a local sink.
 */
enum class exec_type : uint8_t
{
  log    = 0,
  socket = 1
};

struct exec_definition
{
  exec_type type = exec_type::log;
};

/**
 * When a rule's matches leave the node.
 *   irreversible - once the block is irreversible: the consumer never sees a
 *                  match that can be undone, about one block after it was
 *                  applied with one-block irreversibility. The default.
 *   head         - as the operation is applied, while its block can still be
 *                  popped by a fork or rolled back on failure. The stream then
 *                  also carries the lifecycle frames the consumer needs to
 *                  retract: block_applied and switch_fork (frame.hpp).
 * The two classes are independent streams; a block's head matches arrive
 * before its irreversible ones, and in a fork the head stream retracts a
 * block the irreversible stream never mentions.
 */
enum class delivery_class : uint8_t
{
  irreversible = 0,
  head         = 1
};

/**
 * One entry of the stinger-rules-file. `match` is a be-tree boolean
 * expression over the attribute vocabulary declared at startup (see
 * declare_rule_attributes in stinger_plugin.cpp); it is compiled when the
 * subscription index is built and a rule that fails to compile aborts
 * startup. An operation may be named in it instead of numbered - see
 * resolve_operation_names. `name` is the rule's public identity - match
 * events carry it, so renaming a rule means a new identity as far as stream
 * consumers are concerned.
 */
struct rule_definition
{
  std::string                 name;
  std::string                 match;
  /// declarative fetch specs executed on match; accepted in the format but
  /// not implemented yet - load_rules_file rejects rules that use it
  std::vector< fc::variant >  enrich;
  exec_definition             exec;
  delivery_class              delivery = delivery_class::irreversible;

  /// be-tree subscription id, derived by load_rules_file as city_hash64 of
  /// `name` - not part of the file format (a cross-name hash collision is
  /// rejected at load)
  uint64_t                    id = 0;
  /// `match` with operation names resolved to op_type numbers - the
  /// expression the be-tree compiles; derived by load_rules_file, not part
  /// of the file format
  std::string                 expr;
};

struct rules_config
{
  std::vector< rule_definition > rules;
};

/**
 * Parse and validate a stinger rules file:
 *
 *   { "rules": [ { "name": "...", "match": "...",
 *                  "exec": { "type": "log" | "socket" },
 *                  "delivery": "irreversible" | "head" }, ... ] }
 *
 * `delivery` is optional and defaults to irreversible.
 *
 * Validation is strict - unknown keys, missing required keys, duplicate
 * names or a malformed exec throw fc::exception with the offending rule
 * named. Expression compilation is not done here (it needs the be-tree
 * attribute declarations); the plugin validates match exprs when it builds
 * the index.
 */
rules_config load_rules_file( const fc::path& path );

/**
 * Resolve operation names in a match expression to their op_type numbers.
 * An identifier outside string literals that names an operation type - as
 * database_api prints it, `transfer_operation`, or without the suffix,
 * `transfer` - is replaced by the operation's index in
 * hive::protocol::operation (virtual operations included), so
 *
 *   op_type = transfer                        -> op_type = 2
 *   op_type in (transfer, escrow_transfer)    -> op_type in (2, 27)
 *
 * Everything else is copied as written, so an identifier that names nothing
 * still reaches the be-tree parser and fails there with the rule named.
 */
std::string resolve_operation_names( const std::string& match );

} } } // hive::plugins::stinger

FC_REFLECT_ENUM( hive::plugins::stinger::delivery_class, (irreversible)(head) )
FC_REFLECT_ENUM( hive::plugins::stinger::exec_type, (log)(socket) )
FC_REFLECT( hive::plugins::stinger::exec_definition, (type) )
FC_REFLECT( hive::plugins::stinger::rule_definition, (name)(match)(enrich)(exec)(delivery) )
FC_REFLECT( hive::plugins::stinger::rules_config, (rules) )
