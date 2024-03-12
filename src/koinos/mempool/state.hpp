#pragma once

#include <koinos/chain/chain.pb.h>
#include <koinos/state_db/state_db_types.hpp>

namespace koinos::mempool::space {

const chain::object_space& mempool_metadata();
const chain::object_space& pending_transaction();
const chain::object_space& transaction_index();
const chain::object_space& address_resources();
const chain::object_space& account_nonce();

} // namespace koinos::mempool::space
