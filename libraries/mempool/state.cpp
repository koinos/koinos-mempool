#include <koinos/mempool/state.hpp>

namespace koinos {

namespace chain {

bool operator<( const object_space& lhs, const object_space& rhs )
{
   if ( lhs.system() < rhs.system() )
   {
      return true;
   }
   else if ( lhs.system() > rhs.system() )
   {
      return false;
   }

   if ( lhs.zone() < rhs.zone() )
   {
      return true;
   }
   else if ( lhs.system() > rhs.system() )
   {
      return false;
   }

   return lhs.id() < rhs.id();
}

} // chain

namespace mempool::space {

namespace detail {

constexpr uint32_t mempool_metadata_id = 1;
constexpr uint32_t pending_transaction_id = 2;
constexpr uint32_t transaction_index_id = 3;
constexpr uint32_t address_resources_id = 3;

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

} // detail

const chain::object_space mempool_metadata()
{
   static auto s = detail::make_mempool_metadata();
   return s;
}

const chain::object_space pending_transaction()
{
   static auto s = detail::make_pending_transaction();
   return s;
}

const chain::object_space transaction_index()
{
   static auto s = detail::make_transaction_index();
   return s;
}

const chain::object_space address_resources()
{
   static auto s = detail::make_address_resources();
   return s;
}

} // mempool::space

} // koinos

