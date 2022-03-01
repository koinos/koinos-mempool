#include <boost/test/unit_test.hpp>

#include <koinos/mempool/mempool.hpp>

#include <koinos/crypto/elliptic.hpp>
#include <koinos/crypto/multihash.hpp>
#include <koinos/protocol/protocol.pb.h>
#include <koinos/util/conversion.hpp>

#include <memory>

using namespace koinos;

struct mempool_fixture
{
   mempool_fixture()
   {
      std::string seed1 = "alpha bravo charlie delta";
      _key1 = crypto::private_key::regenerate( crypto::hash( crypto::multicodec::sha2_256, seed1 ) );

      std::string seed2 = "echo foxtrot golf hotel";
      _key2 = crypto::private_key::regenerate( crypto::hash( crypto::multicodec::sha2_256, seed2 ) );

      std::string seed3 = "india juliet kilo lima";
      _key3 = crypto::private_key::regenerate( crypto::hash( crypto::multicodec::sha2_256, seed3 ) );
   }

   crypto::multihash sign( crypto::private_key& key, protocol::transaction& t )
   {
      auto digest = crypto::hash( crypto::multicodec::sha2_256, t.header() );
      auto signature = key.sign_compact( digest );
      std::string sig_str;
      sig_str.resize( signature.size() );
      std::transform( signature.begin(), signature.end(), sig_str.begin(), []( std::byte b ) { return char( b ); } );
      t.add_signatures( std::move( sig_str ) );
      return digest;
   }

   crypto::private_key _key1;
   crypto::private_key _key2;
   crypto::private_key _key3;
};

BOOST_FIXTURE_TEST_SUITE( mempool_tests, mempool_fixture )

BOOST_AUTO_TEST_CASE( mempool_basic_test )
{
   mempool::mempool mempool;

   protocol::transaction t1;
   t1.mutable_header()->set_rc_limit( 10 );
   t1.set_id( util::converter::as< std::string >( sign( _key1, t1 ) ) );

   BOOST_TEST_MESSAGE( "adding pending transaction" );
   auto payer = _key1.get_public_key().to_address_bytes();
   uint64_t max_payer_resources = 1000000000000ull;
   auto trx_resource_limit = t1.header().rc_limit();
   mempool.add_pending_transaction( t1, 1, payer, max_payer_resources, trx_resource_limit, 1, 2, 3 );

   BOOST_TEST_MESSAGE( "adding duplicate pending transaction" );
   BOOST_REQUIRE_THROW( mempool.add_pending_transaction( t1, 2, payer, max_payer_resources, trx_resource_limit, 1, 2, 3 ), mempool::pending_transaction_insertion_failure );

   BOOST_TEST_MESSAGE( "checking payer was not charged for failed pending transaction" );
   BOOST_REQUIRE( mempool.check_pending_account_resources( payer, max_payer_resources, max_payer_resources - trx_resource_limit ) );

   BOOST_TEST_MESSAGE( "checking pending transaction list" );
   {
      auto pending_txs = mempool.get_pending_transactions();
      BOOST_TEST_MESSAGE( "checking pending transactions size" );
      BOOST_REQUIRE_EQUAL( pending_txs.size(), 1 );
      BOOST_TEST_MESSAGE( "checking pending transaction id" );
      BOOST_REQUIRE_EQUAL( util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, pending_txs[0].transaction().header() ) ), t1.id() );
      BOOST_REQUIRE_EQUAL( pending_txs[0].disk_storage_used(), 1 );
      BOOST_REQUIRE_EQUAL( pending_txs[0].network_bandwidth_used(), 2 );
      BOOST_REQUIRE_EQUAL( pending_txs[0].compute_bandwidth_used(), 3 );
   }

   BOOST_TEST_MESSAGE( "pending transaction existence check" );
   BOOST_REQUIRE_EQUAL( mempool.has_pending_transaction( util::converter::to< crypto::multihash >( t1.id() ) ), true );

   protocol::transaction t2;
   t2.mutable_header()->set_rc_limit( 1000000000000 );
   t2.set_id( util::converter::as< std::string >( sign( _key1, t2 ) ) );

   BOOST_TEST_MESSAGE( "adding pending transaction that exceeds accout resources" );
   payer = _key1.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   trx_resource_limit = t2.header().rc_limit();
   BOOST_REQUIRE_THROW( mempool.add_pending_transaction( t2, 3, payer, max_payer_resources, trx_resource_limit, 1, 1, 1 ), mempool::pending_transaction_exceeds_resources );

   BOOST_TEST_MESSAGE( "removing pending transactions" );
   mempool.remove_pending_transactions( std::vector< crypto::multihash >{ util::converter::to< crypto::multihash >( t1.id() ) } );

   BOOST_TEST_MESSAGE( "checking pending transaction list" );
   {
      auto pending_txs = mempool.get_pending_transactions();
      BOOST_TEST_MESSAGE( "checking pending transactions size" );
      BOOST_REQUIRE_EQUAL( pending_txs.size(), 0 );
   }

   BOOST_TEST_MESSAGE( "pending transaction existence check" );
   BOOST_REQUIRE_EQUAL( mempool.has_pending_transaction( util::converter::to< crypto::multihash >( t1.id() ) ), false );
}

BOOST_AUTO_TEST_CASE( pending_transaction_pagination )
{
   mempool::mempool mempool;
   protocol::transaction trx;
   mempool::account_type payer;
   uint64_t max_payer_resources;
   uint64_t trx_resource_limit;

   for( uint64_t i = 0; i < MAX_PENDING_TRANSACTION_REQUEST + 1; i++ )
   {
      trx.mutable_header()->set_rc_limit( 10 * i );
      trx.set_id( util::converter::as< std::string >( sign( _key1, trx ) ) );

      payer = _key1.get_public_key().to_address_bytes();
      max_payer_resources = 1000000000000;
      trx_resource_limit = trx.header().rc_limit();
      mempool.add_pending_transaction( trx, i, payer, max_payer_resources, trx_resource_limit, 1, 1, 1 );
   }

   BOOST_REQUIRE_THROW( mempool.get_pending_transactions( MAX_PENDING_TRANSACTION_REQUEST + 1 ), mempool::pending_transaction_request_overflow );

   auto pending_trxs = mempool.get_pending_transactions( MAX_PENDING_TRANSACTION_REQUEST );
   BOOST_REQUIRE( pending_trxs.size() == MAX_PENDING_TRANSACTION_REQUEST );
   for( uint64_t i = 0; i < pending_trxs.size(); i++ )
   {
      BOOST_CHECK_EQUAL( pending_trxs[i].transaction().header().rc_limit(), 10 * i );
   }
}

BOOST_AUTO_TEST_CASE( pending_transaction_pruning )
{
   // Add payerA transactions to blocks 1 and 2
   // Add payerB transaction to block 1
   // Add payerC transaction to block 2
   // Prune 1, payerA trx2 and payerC trx exist
   // Prune 2, no trx exist
   mempool::mempool mempool;
   protocol::transaction trx;
   mempool::account_type payer;
   uint64_t max_payer_resources;
   uint64_t trx_resource_limit;

   trx.mutable_header()->set_rc_limit( 1 );
   trx.set_id( util::converter::as< std::string >( sign( _key1, trx ) ) );
   payer = _key1.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   trx_resource_limit = trx.header().rc_limit();
   mempool.add_pending_transaction( trx, 1, payer, max_payer_resources, trx_resource_limit, 1, 1, 1 );

   trx.mutable_header()->set_rc_limit( 2 );
   trx.set_id( util::converter::as< std::string >( sign( _key2, trx ) ) );
   payer = _key2.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   trx_resource_limit = trx_resource_limit = trx.header().rc_limit();
   mempool.add_pending_transaction( trx, 1, payer, max_payer_resources, trx_resource_limit, 1, 1, 1 );

   trx.mutable_header()->set_rc_limit( 3 );
   trx.set_id( util::converter::as< std::string >( sign( _key1, trx ) ) );
   payer = _key1.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   trx_resource_limit = trx_resource_limit = trx.header().rc_limit();
   mempool.add_pending_transaction( trx, 2, payer, max_payer_resources, trx_resource_limit, 1, 1, 1 );

   trx.mutable_header()->set_rc_limit( 4 );
   trx.set_id( util::converter::as< std::string >( sign( _key3, trx ) ) );
   payer = _key3.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   trx_resource_limit = trx_resource_limit = trx.header().rc_limit();
   mempool.add_pending_transaction( trx, 2, payer, max_payer_resources, trx_resource_limit, 1, 1, 1 );

   auto pending_trxs = mempool.get_pending_transactions();
   BOOST_CHECK_EQUAL( mempool.payer_entries_size(), 3 );
   BOOST_REQUIRE_EQUAL( pending_trxs.size(), 4 );
   for ( size_t i = 0; i < pending_trxs.size(); i++ )
   {
      BOOST_REQUIRE_EQUAL( pending_trxs[i].transaction().header().rc_limit(), i + 1 );
   }

   mempool.prune( 1 );
   pending_trxs = mempool.get_pending_transactions();
   BOOST_CHECK_EQUAL( mempool.payer_entries_size(), 2 );
   BOOST_REQUIRE_EQUAL( pending_trxs.size(), 2 );
   for ( size_t i = 0; i < pending_trxs.size(); i++ )
   {
      BOOST_REQUIRE_EQUAL( pending_trxs[i].transaction().header().rc_limit(), i + 3 );
   }

   mempool.prune( 2 );
   pending_trxs = mempool.get_pending_transactions();
   BOOST_CHECK_EQUAL( mempool.payer_entries_size(), 0 );
   BOOST_REQUIRE_EQUAL( pending_trxs.size(), 0 );
}

BOOST_AUTO_TEST_CASE( pending_transaction_dynamic_max_resources )
{
   mempool::mempool mempool;
   protocol::transaction trx;
   mempool::account_type payer;
   uint64_t max_payer_resources;
   uint64_t trx_resource_limit;

   BOOST_TEST_MESSAGE( "gain max account resources" );

   trx.mutable_header()->set_rc_limit( 1000000000000 );
   trx.set_id( util::converter::as< std::string >( sign( _key1, trx ) ) );

   payer = _key1.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   trx_resource_limit = trx.header().rc_limit();

   mempool.add_pending_transaction( trx, 1, payer, max_payer_resources, trx_resource_limit, 1, 1, 1 );

   for ( unsigned int i = 2; i < 10; i++ )
   {
      max_payer_resources = max_payer_resources + i * 10;
      trx_resource_limit = i * 10;

      trx.mutable_header()->set_rc_limit( trx_resource_limit );
      trx.set_id( util::converter::as< std::string >( sign( _key1, trx ) ) );

      mempool.add_pending_transaction( trx, i, payer, max_payer_resources, trx_resource_limit, 1, 1, 1 );
   }

   max_payer_resources = max_payer_resources + 99;
   trx_resource_limit = 100;

   trx.mutable_header()->set_rc_limit( trx_resource_limit );
   trx.set_id( util::converter::as< std::string >( sign( _key1, trx ) ) );

   BOOST_REQUIRE_THROW(
      mempool.add_pending_transaction( trx, 10, payer, max_payer_resources, trx_resource_limit, 1, 1, 1 ),
      mempool::pending_transaction_exceeds_resources
   );

   BOOST_TEST_MESSAGE( "lose max account resources" );

   trx.mutable_header()->set_rc_limit( 999999999980 );
   trx.set_id( util::converter::as< std::string >( sign( _key2, trx ) ) );

   payer = _key2.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   trx_resource_limit = trx.header().rc_limit();

   mempool.add_pending_transaction( trx, 1, payer, max_payer_resources, trx_resource_limit, 1, 1, 1 );

   max_payer_resources = 999999999990;
   trx_resource_limit = 10;

   trx.mutable_header()->set_rc_limit( trx_resource_limit );
   trx.set_id( util::converter::as< std::string >( sign( _key2, trx ) ) );

   mempool.add_pending_transaction( trx, 2, payer, max_payer_resources, trx_resource_limit, 1, 1, 1 );

   trx_resource_limit = 1;

   trx.mutable_header()->set_rc_limit( trx_resource_limit );
   trx.set_id( util::converter::as< std::string >( sign( _key2, trx ) ) );

   BOOST_REQUIRE_THROW(
      mempool.add_pending_transaction( trx, 3, payer, max_payer_resources, trx_resource_limit, 1, 1, 1 ),
      mempool::pending_transaction_exceeds_resources
   );
}

BOOST_AUTO_TEST_SUITE_END()
