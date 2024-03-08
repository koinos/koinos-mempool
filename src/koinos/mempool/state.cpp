#include <koinos/mempool/state.hpp>

namespace koinos::mempool::space {

namespace detail {

constexpr uint32_t mempool_metadata_id    = 1;
constexpr uint32_t pending_transaction_id = 2;
constexpr uint32_t transaction_index_id   = 3;
constexpr uint32_t address_resources_id   = 4;
constexpr uint32_t account_nonce_id       = 5;

const chain::object_space make_mempool_metadata()
{
  chain::object_space s;
  s.set_id( mempool_metadata_id );
  return s;
}

const chain::object_space make_pending_transaction()
{
  chain::object_space s;
  s.set_id( pending_transaction_id );
  return s;
}

const chain::object_space make_transaction_index()
{
  chain::object_space s;
  s.set_id( transaction_index_id );
  return s;
}

const chain::object_space make_address_resources()
{
  chain::object_space s;
  s.set_id( address_resources_id );
  return s;
}

const chain::object_space make_account_nonce()
{
  chain::object_space s;
  s.set_id( account_nonce_id );
  return s;
}

} // namespace detail

const chain::object_space& mempool_metadata()
{
  static auto s = detail::make_mempool_metadata();
  return s;
}

const chain::object_space& pending_transaction()
{
  static auto s = detail::make_pending_transaction();
  return s;
}

const chain::object_space& transaction_index()
{
  static auto s = detail::make_transaction_index();
  return s;
}

const chain::object_space& address_resources()
{
  static auto s = detail::make_address_resources();
  return s;
}

const chain::object_space& account_nonce()
{
  static auto s = detail::make_account_nonce();
  return s;
}

} // namespace koinos::mempool::space
