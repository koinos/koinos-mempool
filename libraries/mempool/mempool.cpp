#include <koinos/mempool/mempool.hpp>

#include <chrono>
#include <functional>
#include <tuple>

#include <boost/multiprecision/cpp_int.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>

#include <koinos/chain/value.pb.h>

#include <koinos/util/base58.hpp>
#include <koinos/util/conversion.hpp>
#include <koinos/util/hex.hpp>

namespace koinos::mempool {

namespace detail {

using namespace boost;
using int128_t = boost::multiprecision::int128_t;
using namespace std::chrono_literals;

struct pending_transaction_object
{
   protocol::transaction                 transaction;
   std::chrono::system_clock::time_point time;
   nonce_type                            nonce;
   uint64_t                              disk_storage_used;
   uint64_t                              network_bandwidth_used;
   uint64_t                              compute_bandwidth_used;

   const transaction_id_type& id() const
   {
      return transaction.id();
   }

   const account_type& payer() const
   {
      return transaction.header().payer();
   }

   uint64_t rc_limit() const
   {
      return transaction.header().rc_limit();
   }
};

using pending_transaction_queue = std::list< pending_transaction_object >;
using pending_transaction_iterator = pending_transaction_queue::iterator;

struct pending_transaction_iterator_wrapper
{
   pending_transaction_iterator iterator;

   const transaction_id_type& id() const
   {
      return iterator->id();
   }

   const account_type& payer() const
   {
      return iterator->payer();
   }

   const nonce_type& nonce() const
   {
      return iterator->nonce;
   }

   const std::chrono::system_clock::time_point& time() const
   {
      return iterator->time;
   }
};

struct by_id;
struct by_account_nonce;
struct by_time;

using pending_transaction_index = multi_index_container<
   pending_transaction_iterator_wrapper,
   multi_index::indexed_by<
      multi_index::ordered_unique< multi_index::tag< by_id >,
         multi_index::const_mem_fun< pending_transaction_iterator_wrapper, const transaction_id_type&, &pending_transaction_iterator_wrapper::id >
      >,
      multi_index::ordered_non_unique< multi_index::tag< by_time >,
         multi_index::const_mem_fun< pending_transaction_iterator_wrapper, const std::chrono::system_clock::time_point&, &pending_transaction_iterator_wrapper::time >
      >,
      multi_index::ordered_unique< multi_index::tag< by_account_nonce >,
         multi_index::composite_key<
            pending_transaction_iterator_wrapper,
            multi_index::const_mem_fun< pending_transaction_iterator_wrapper, const account_type&, &pending_transaction_iterator_wrapper::payer >,
            multi_index::const_mem_fun< pending_transaction_iterator_wrapper, const nonce_type&, &pending_transaction_iterator_wrapper::nonce >
         >
      >
   >
>;

struct account_resources_object
{
   account_type                          account;
   uint64_t                              resources;
   uint64_t                              max_resources;
   std::chrono::system_clock::time_point time;
};

struct by_account;

using account_resources_index = multi_index_container<
   account_resources_object,
   multi_index::indexed_by<
      multi_index::ordered_unique< multi_index::tag< by_account >,
         multi_index::member< account_resources_object, account_type, &account_resources_object::account >
      >
   >
>;

class mempool_impl final
{
private:
   account_resources_index          _account_resources_idx;
   mutable std::mutex               _account_resources_mutex;

   pending_transaction_queue        _pending_transactions;
   pending_transaction_index        _pending_transaction_idx;
   mutable std::mutex               _pending_transaction_mutex;

public:
   mempool_impl();
   virtual ~mempool_impl();

   bool has_pending_transaction( const transaction_id_type& id ) const;
   std::vector< rpc::mempool::pending_transaction > get_pending_transactions( std::size_t limit );
   bool check_pending_account_resources(
      const account_type& payer,
      uint64_t max_payer_resources,
      uint64_t trx_resource_limit ) const;
   uint64_t add_pending_transaction(
      const protocol::transaction& transaction,
      std::chrono::system_clock::time_point time,
      uint64_t max_payer_rc,
      uint64_t disk_storaged_used,
      uint64_t network_bandwidth_used,
      uint64_t compute_bandwidth_used );
   std::pair< uint64_t, uint64_t > remove_pending_transactions( const std::vector< transaction_id_type >& ids );
   uint64_t prune( std::chrono::seconds expiration, std::chrono::system_clock::time_point now );
   std::size_t payer_entries_size() const;
   void cleanup_account_resources( const pending_transaction_object& pending_trx );
   std::size_t pending_transaction_count() const;

private:
   bool check_pending_account_resources_lockfree(
         const account_type& payer,
         uint64_t max_payer_rc,
         uint64_t rc_limit
      )const;
};

mempool_impl::mempool_impl() {}
mempool_impl::~mempool_impl() = default;

bool mempool_impl::has_pending_transaction( const transaction_id_type& id ) const
{
   std::lock_guard< std::mutex > guard( _pending_transaction_mutex );
   auto& id_idx = _pending_transaction_idx.get< by_id >();

   auto it = id_idx.find( id );

   return it != id_idx.end();
}

std::vector< rpc::mempool::pending_transaction > mempool_impl::get_pending_transactions( std::size_t limit )
{
   KOINOS_ASSERT( limit <= MAX_PENDING_TRANSACTION_REQUEST, pending_transaction_request_overflow, "Requested too many pending transactions. Max: ${max}", ("max", MAX_PENDING_TRANSACTION_REQUEST) );

   std::lock_guard< std::mutex > guard( _pending_transaction_mutex );

   std::vector< rpc::mempool::pending_transaction > pending_transactions;
   pending_transactions.reserve(limit);

   auto itr = _pending_transactions.begin();

   while ( itr != _pending_transactions.end() && pending_transactions.size() < limit )
   {
      rpc::mempool::pending_transaction ptx;
      *ptx.mutable_transaction() = itr->transaction;
      ptx.set_disk_storage_used( itr->disk_storage_used );
      ptx.set_network_bandwidth_used( itr->network_bandwidth_used );
      ptx.set_compute_bandwidth_used( itr->compute_bandwidth_used );

      pending_transactions.push_back( ptx );
      ++itr;
   }

   return pending_transactions;
}

bool mempool_impl::check_pending_account_resources_lockfree(
   const account_type& payer,
   uint64_t max_payer_resources,
   uint64_t trx_resource_limit ) const
{
   auto& account_idx = _account_resources_idx.get< by_account >();
   auto it = account_idx.find( payer );

   if ( it == account_idx.end() )
   {
      return trx_resource_limit <= max_payer_resources;
   }

   int128_t max_resource_delta = int128_t( max_payer_resources ) - int128_t( it->max_resources );
   int128_t new_resources = int128_t( it->resources ) + max_resource_delta - int128_t( trx_resource_limit );
   return new_resources >= 0;
}

bool mempool_impl::check_pending_account_resources(
   const account_type& payer,
   uint64_t max_payer_rc,
   uint64_t rc_limit ) const
{
   std::lock_guard< std::mutex > guard( _account_resources_mutex );
   return check_pending_account_resources_lockfree( payer, max_payer_rc, rc_limit );
}

uint64_t mempool_impl::add_pending_transaction(
   const protocol::transaction& transaction,
   std::chrono::system_clock::time_point time,
   uint64_t max_payer_rc,
   uint64_t disk_storaged_used,
   uint64_t network_bandwidth_used,
   uint64_t compute_bandwidth_used )
{
   const auto& payer = transaction.header().payer();
   uint64_t rc_limit = transaction.header().rc_limit();
   uint64_t rc_used = rc_limit;

   {
      std::lock_guard< std::mutex > guard( _account_resources_mutex );

      KOINOS_ASSERT(
         check_pending_account_resources_lockfree( payer, max_payer_rc, rc_limit ),
         pending_transaction_exceeds_resources,
         "transaction would exceed maximum resources for account: ${a}", ("a", util::encode_base58( util::converter::as< std::vector< std::byte > >( payer )))
      );

      {
         auto id = util::converter::to< transaction_id_type >( transaction.id() );
         auto nonce_value = util::converter::to< chain::value_type >( transaction.header().nonce() );

         KOINOS_ASSERT(
            nonce_value.has_uint64_value(),
            pending_transaction_insertion_failure,
            "transaction nonce did not contain uint64 value"
         );

         nonce_type nonce = nonce_value.uint64_value();

         std::lock_guard< std::mutex > guard( _pending_transaction_mutex );

         /*
          * We use two synchronized data structures to store transaction ordering
          * The first is a simple FIFO queue of transactions
          * The second is a boost multi index container (BMIC) whose primary responsibility is to re-order transactions with
          * conflicting payers. The BMIC contains a wrapper class which just contains an iterator in to the FIFO queue.
          * The FIFO queue is an std::list which guarantees iterator validity on all modifications, so the BMIC iterators
          * are valid in all cases.
          *
          * When a new transaction is added that has the same payer as another, we want to insert it in the FIFO queue
          * immediately prior to the fist nonce that is after the new transaction. We check for conflicting nonces
          * using lower bound on the BMIC. If we find a transaction with the same payer, then it is the lowest nonce
          * with the same payer. Because lower bound is leq, there is a chance the nonce is the same, in which case we want
          * to assert and fail. Otherwise, insert in the FIFO queue before the found transaction. This will ensure a
          * correct ordering of transactions for an account when pulled from the front of the FIFO queue.
          *
          * If no such transaction was found, there is no conflict, simply add the transaction at the end of the FIFO queue
          */

         const auto& account_nonce_idx = _pending_transaction_idx.get< by_account_nonce >();
         auto account_nonce_iterator = account_nonce_idx.lower_bound( boost::make_tuple( payer, nonce ) );

         pending_transaction_iterator transaction_iterator;
         pending_transaction_object pending_transaction {
            .transaction            = transaction,
            .time                   = time,
            .nonce                  = nonce,
            .disk_storage_used      = disk_storaged_used,
            .network_bandwidth_used = network_bandwidth_used,
            .compute_bandwidth_used = compute_bandwidth_used
         };

         if ( account_nonce_iterator != account_nonce_idx.end() && account_nonce_iterator->payer() == payer )
         {
            KOINOS_ASSERT(
               account_nonce_iterator->nonce() != nonce,
               pending_transaction_insertion_failure,
               "transaction account nonce conflicts with existing transaction in mempool - account: ${a}, nonce: ${n}",
               ("a", util::to_base58( payer ) )( "n", nonce )
            );

            transaction_iterator = _pending_transactions.emplace( account_nonce_iterator->iterator, std::move( pending_transaction ) );
         }
         else
         {
            _pending_transactions.push_back( std::move( pending_transaction ) );
            transaction_iterator = --_pending_transactions.end();
         }

         KOINOS_ASSERT( transaction_iterator != _pending_transactions.end(), pending_transaction_insertion_failure, "failed to insert transaction with id: ${id}", ("id", id) );

         auto rval = _pending_transaction_idx.emplace( pending_transaction_iterator_wrapper{ transaction_iterator } );
         if ( !rval.second )
         {
            _pending_transactions.erase( transaction_iterator );
            KOINOS_ASSERT( false, pending_transaction_insertion_failure, "failed to insert transaction with id: ${id}", ("id", id) );
         }
      }

      auto& account_idx = _account_resources_idx.get< by_account >();
      auto it = account_idx.find( payer );

      if ( it == account_idx.end() )
      {
         _account_resources_idx.insert( account_resources_object {
            .account       = payer,
            .resources     = max_payer_rc - rc_limit,
            .max_resources = max_payer_rc,
            .time          = time
         } );
      }
      else
      {
         int128_t max_resource_delta = int128_t( max_payer_rc ) - int128_t( it->max_resources );
         int128_t new_resources = int128_t( it->resources ) + max_resource_delta - int128_t( rc_limit );

         account_idx.modify( it, [&]( account_resources_object& aro )
         {
            aro.max_resources = max_payer_rc;
            aro.resources     = new_resources.convert_to< uint64_t >();
            aro.time          = time;
         } );

         rc_used = max_payer_rc - it->resources;
      }
   }

   LOG(debug) << "Transaction added to mempool: " << util::to_hex( transaction.id() );
   return rc_used;
}

std::pair< uint64_t, uint64_t > mempool_impl::remove_pending_transactions( const std::vector< transaction_id_type >& ids )
{
   std::lock_guard< std::mutex > account_guard( _account_resources_mutex );
   std::lock_guard< std::mutex > trx_guard( _pending_transaction_mutex );

   std::size_t count = 0;
   for ( const auto& id : ids )
   {
      auto& id_idx = _pending_transaction_idx.get< by_id >();

      auto itr = id_idx.find( id );
      if ( itr != id_idx.end() )
      {
         LOG(debug) << "Removing included transaction from mempool: " << util::to_hex( itr->iterator->transaction.id() );
         cleanup_account_resources( *(itr->iterator) );
         _pending_transactions.erase( itr->iterator );
         id_idx.erase( itr );
         count++;
      }
   }

   return std::make_pair( count, _pending_transactions.size() );
}

uint64_t mempool_impl::prune( std::chrono::seconds expiration, std::chrono::system_clock::time_point now )
{
   std::lock_guard< std::mutex > account_guard( _account_resources_mutex );
   std::lock_guard< std::mutex > trx_guard( _pending_transaction_mutex );

   auto& by_time_idx = _pending_transaction_idx.get< by_time >();
   auto itr = by_time_idx.begin();

   std::size_t count = 0;
   while ( itr != by_time_idx.end() && itr->time() + expiration <= now )
   {
      LOG(debug) << "Pruning transaction from mempool: " << util::to_hex( itr->id() );
      cleanup_account_resources( *(itr->iterator) );
      _pending_transactions.erase( itr->iterator );
      by_time_idx.erase( itr );
      itr = by_time_idx.begin();
      count++;
   }

   return count;
}

std::size_t mempool_impl::payer_entries_size() const
{
   std::lock_guard< std::mutex > guard( _account_resources_mutex );
   return _account_resources_idx.size();
}

void mempool_impl::cleanup_account_resources( const pending_transaction_object& pending_trx )
{
   auto itr = _account_resources_idx.find( pending_trx.payer() );
   if ( itr != _account_resources_idx.end() )
   {
      if ( itr->resources + pending_trx.rc_limit() >= itr->max_resources )
      {
         _account_resources_idx.erase( itr );
      }
      else
      {
         _account_resources_idx.modify( itr, [&]( account_resources_object& aro )
         {
            aro.resources += pending_trx.rc_limit();
         } );
      }
   }
}

std::size_t mempool_impl::pending_transaction_count() const
{
   std::lock_guard< std::mutex > lock_guard( _pending_transaction_mutex );
   return _pending_transactions.size();
}

} // detail

mempool::mempool() : _my( std::make_unique< detail::mempool_impl >() ) {}
mempool::~mempool() = default;

bool mempool::has_pending_transaction( const transaction_id_type& id ) const
{
   return _my->has_pending_transaction( id );
}

std::vector< rpc::mempool::pending_transaction > mempool::get_pending_transactions( std::size_t limit )
{
   return _my->get_pending_transactions( limit );
}

bool mempool::check_pending_account_resources(
   const account_type& payer,
   uint64_t max_payer_resources,
   uint64_t trx_resource_limit ) const
{
   return _my->check_pending_account_resources( payer, max_payer_resources, trx_resource_limit );
}

uint64_t mempool::add_pending_transaction(
   const protocol::transaction& transaction,
   std::chrono::system_clock::time_point time,
   uint64_t max_payer_rc,
   uint64_t disk_storaged_used,
   uint64_t network_bandwidth_used,
   uint64_t compute_bandwidth_used )
{
   return _my->add_pending_transaction( transaction, time, max_payer_rc, disk_storaged_used, network_bandwidth_used, compute_bandwidth_used );
}

std::pair< uint64_t, uint64_t > mempool::remove_pending_transactions( const std::vector< transaction_id_type >& ids )
{
   return _my->remove_pending_transactions( ids );
}

uint64_t mempool::prune( std::chrono::seconds expiration, std::chrono::system_clock::time_point now )
{
   return _my->prune( expiration, now );
}

std::size_t mempool::payer_entries_size() const
{
   return _my->payer_entries_size();
}

std::size_t mempool::pending_transaction_count() const
{
   return _my->pending_transaction_count();
}

} // koinos::mempool
