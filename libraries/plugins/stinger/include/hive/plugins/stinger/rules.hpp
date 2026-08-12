#pragma once

#include <fc/filesystem.hpp>
#include <fc/reflect/reflect.hpp>
#include <fc/variant.hpp>

#include <string>
#include <vector>

namespace hive { namespace plugins { namespace stinger {

/**
 * Delivery target for a rule's matches. Fixed menu (no user code on the
 * writer thread); currently only:
 *   log - structured log line
 *
 * Outbound delivery (HTTP and friends) is deliberately not in the menu: the
 * producer is the writer thread, and retries, timeouts, TLS and per-target
 * backpressure are policy a consumer process handles better on the far side
 * of a local sink.
 */
struct exec_definition
{
  std::string type;
};

/**
 * One entry of the stinger-rules-file. `match` is a be-tree boolean
 * expression over the attribute vocabulary declared at startup (see
 * declare_rule_attributes in stinger_plugin.cpp); it is compiled when the
 * subscription index is built and a rule that fails to compile aborts
 * startup. `name` is the rule's public identity - match events carry it, so
 * renaming a rule means a new identity as far as stream consumers are
 * concerned.
 */
struct rule_definition
{
  std::string                 name;
  std::string                 match;
  /// declarative fetch specs executed on match; accepted in the format but
  /// not implemented yet - load_rules_file rejects rules that use it
  std::vector< fc::variant >  enrich;
  exec_definition             exec;

  /// be-tree subscription id, derived by load_rules_file as city_hash64 of
  /// `name` - not part of the file format (a cross-name hash collision is
  /// rejected at load)
  uint64_t                    id = 0;
};

struct rules_config
{
  std::vector< rule_definition > rules;
};

/**
 * Parse and validate a stinger rules file:
 *
 *   { "rules": [ { "name": "...", "match": "...",
 *                  "exec": { "type": "log" } }, ... ] }
 *
 * Validation is strict - unknown keys, missing required keys, duplicate
 * names or a malformed exec throw fc::exception with the offending rule
 * named. Expression compilation is not done here (it needs the be-tree
 * attribute declarations); the plugin validates match exprs when it builds
 * the index.
 */
rules_config load_rules_file( const fc::path& path );

} } } // hive::plugins::stinger

FC_REFLECT( hive::plugins::stinger::exec_definition, (type) )
FC_REFLECT( hive::plugins::stinger::rule_definition, (name)(match)(enrich)(exec) )
FC_REFLECT( hive::plugins::stinger::rules_config, (rules) )
