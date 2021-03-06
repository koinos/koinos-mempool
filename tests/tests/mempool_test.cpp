#include <boost/test/unit_test.hpp>

#include <koinos/mempool/mempool.hpp>

#include <koinos/chain/value.pb.h>
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

   mempool::transaction_id_type sign( crypto::private_key& key, protocol::transaction& t )
   {
      auto digest = crypto::hash( crypto::multicodec::sha2_256, t.header() );
      auto signature = key.sign_compact( digest );
      std::string sig_str;
      sig_str.resize( signature.size() );
      std::transform( signature.begin(), signature.end(), sig_str.begin(), []( std::byte b ) { return char( b ); } );
      t.add_signatures( std::move( sig_str ) );
      return util::converter::as< std::string >( digest );
   }

   crypto::private_key _key1;
   crypto::private_key _key2;
   crypto::private_key _key3;
};

BOOST_FIXTURE_TEST_SUITE( mempool_tests, mempool_fixture )

BOOST_AUTO_TEST_CASE( mempool_basic_test )
{
   mempool::mempool mempool;

   auto payer = _key1.get_public_key().to_address_bytes();
   uint64_t max_payer_resources = 1000000000000ull;
   chain::value_type nonce_value;
   nonce_value.set_uint64_value( 1 );

   protocol::transaction t1;
   t1.mutable_header()->set_rc_limit( 10 );
   t1.mutable_header()->set_payer( payer );
   t1.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   t1.set_id( sign( _key1, t1 ) );

   BOOST_TEST_MESSAGE( "adding pending transaction" );
   auto trx_resource_limit = t1.header().rc_limit();
   auto rc_used = mempool.add_pending_transaction( t1, 1, max_payer_resources, 1, 2, 3 );
   BOOST_CHECK_EQUAL( t1.header().rc_limit(), rc_used );

   BOOST_TEST_MESSAGE( "adding duplicate pending transaction" );
   BOOST_REQUIRE_THROW( mempool.add_pending_transaction( t1, 2, max_payer_resources, 1, 2, 3 ), mempool::pending_transaction_insertion_failure );

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
   BOOST_REQUIRE( mempool.has_pending_transaction( t1.id() ) );

   payer = _key1.get_public_key().to_address_bytes();
   nonce_value.set_uint64_value( 2 );

   protocol::transaction t2;
   t2.mutable_header()->set_rc_limit( 1000000000000 );
   t2.mutable_header()->set_payer( payer );
   t2.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   t2.set_id( sign( _key1, t2 ) );

   BOOST_TEST_MESSAGE( "adding pending transaction that exceeds accout resources" );
   max_payer_resources = 1000000000000;
   trx_resource_limit = t2.header().rc_limit();
   BOOST_REQUIRE_THROW( mempool.add_pending_transaction( t2, 3, max_payer_resources, 1, 1, 1 ), mempool::pending_transaction_exceeds_resources );

   BOOST_TEST_MESSAGE( "removing pending transactions" );
   mempool.remove_pending_transactions( std::vector< mempool::transaction_id_type >{ t1.id() } );

   BOOST_TEST_MESSAGE( "checking pending transaction list" );
   {
      auto pending_txs = mempool.get_pending_transactions();
      BOOST_TEST_MESSAGE( "checking pending transactions size" );
      BOOST_REQUIRE_EQUAL( pending_txs.size(), 0 );
   }

   BOOST_TEST_MESSAGE( "pending transaction existence check" );
   BOOST_REQUIRE( !mempool.has_pending_transaction( t1.id() ) );
}

BOOST_AUTO_TEST_CASE( pending_transaction_pagination )
{
   mempool::mempool mempool;
   protocol::transaction trx;
   mempool::account_type payer = _key1.get_public_key().to_address_bytes();;
   uint64_t max_payer_resources = 1000000000000;
   uint64_t trx_resource_limit;
   chain::value_type nonce_value;
   uint64_t rc_used = 0;

   for( uint64_t i = 0; i < MAX_PENDING_TRANSACTION_REQUEST + 1; i++ )
   {
      nonce_value.set_uint64_value( i + 1 );

      trx.mutable_header()->set_rc_limit( 10 * i );
      trx.mutable_header()->set_payer( payer );
      trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
      trx.set_id( sign( _key1, trx ) );

      rc_used += trx.header().rc_limit();

      auto rc = mempool.add_pending_transaction( trx, i, max_payer_resources, 1, 1, 1 );
      BOOST_CHECK_EQUAL( rc, rc_used );
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
   chain::value_type nonce_value;


   payer = _key1.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   nonce_value.set_uint64_value( 1 );
   trx.mutable_header()->set_rc_limit( 1 );
   trx.mutable_header()->set_payer( payer );
   trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   trx.set_id( sign( _key1, trx ) );

   mempool.add_pending_transaction( trx, 1, max_payer_resources, 1, 1, 1 );

   payer = _key2.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   nonce_value.set_uint64_value( 2 );
   trx.mutable_header()->set_rc_limit( 2 );
   trx.mutable_header()->set_payer( payer );
   trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   trx.set_id( sign( _key2, trx ) );

   mempool.add_pending_transaction( trx, 1, max_payer_resources, 1, 1, 1 );

   payer = _key1.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   nonce_value.set_uint64_value( 3 );
   trx.mutable_header()->set_rc_limit( 3 );
   trx.mutable_header()->set_payer( payer );
   trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   trx.set_id( sign( _key1, trx ) );

   mempool.add_pending_transaction( trx, 2, max_payer_resources, 1, 1, 1 );

   payer = _key3.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   nonce_value.set_uint64_value( 4 );
   trx.mutable_header()->set_rc_limit( 4 );
   trx.mutable_header()->set_payer( payer );
   trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   trx.set_id( sign( _key3, trx ) );

   mempool.add_pending_transaction( trx, 2, max_payer_resources, 1, 1, 1 );

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
   chain::value_type nonce_value;

   BOOST_TEST_MESSAGE( "gain max account resources" );

   payer = _key1.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   nonce_value.set_uint64_value( 1 );
   trx.mutable_header()->set_rc_limit( 1000000000000 );
   trx.mutable_header()->set_payer( payer );
   trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   trx.set_id( sign( _key1, trx ) );

   mempool.add_pending_transaction( trx, 1, max_payer_resources, 1, 1, 1 );

   for ( unsigned int i = 2; i < 10; i++ )
   {
      max_payer_resources = max_payer_resources + i * 10;
      trx_resource_limit = i * 10;
      nonce_value.set_uint64_value( i );

      trx.mutable_header()->set_rc_limit( trx_resource_limit );
      trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
      trx.set_id( sign( _key1, trx ) );

      mempool.add_pending_transaction( trx, i, max_payer_resources, 1, 1, 1 );
   }

   max_payer_resources = max_payer_resources + 99;
   trx_resource_limit = 100;
   nonce_value.set_uint64_value( 10 );

   trx.mutable_header()->set_rc_limit( trx_resource_limit );
   trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   trx.set_id( sign( _key1, trx ) );

   BOOST_REQUIRE_THROW(
      mempool.add_pending_transaction( trx, 10, max_payer_resources, 1, 1, 1 ),
      mempool::pending_transaction_exceeds_resources
   );

   BOOST_TEST_MESSAGE( "lose max account resources" );

   payer = _key2.get_public_key().to_address_bytes();
   max_payer_resources = 1000000000000;
   nonce_value.set_uint64_value( 11 );

   trx.mutable_header()->set_rc_limit( 999999999980 );
   trx.mutable_header()->set_payer( payer );
   trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   trx.set_id( sign( _key2, trx ) );

   mempool.add_pending_transaction( trx, 1, max_payer_resources, 1, 1, 1 );

   max_payer_resources = 999999999990;
   trx_resource_limit = 10;
   nonce_value.set_uint64_value( 12 );

   trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   trx.mutable_header()->set_rc_limit( trx_resource_limit );
   trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   trx.set_id( sign( _key2, trx ) );

   mempool.add_pending_transaction( trx, 2, max_payer_resources, 1, 1, 1 );

   trx_resource_limit = 1;
   nonce_value.set_uint64_value( 12 );

   trx.mutable_header()->set_rc_limit( trx_resource_limit );
   trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   trx.set_id( sign( _key2, trx ) );

   BOOST_REQUIRE_THROW(
      mempool.add_pending_transaction( trx, 3, max_payer_resources, 1, 1, 1 ),
      mempool::pending_transaction_exceeds_resources
   );
}

BOOST_AUTO_TEST_CASE( mempool_nonce_test )
{
   mempool::mempool mempool;

   auto payer = _key1.get_public_key().to_address_bytes();
   uint64_t max_payer_resources = 1000000000000ull;
   chain::value_type nonce_value;

   BOOST_TEST_MESSAGE( "adding pending transactions" );

   protocol::transaction t1;
   nonce_value.set_uint64_value( 2 );
   t1.mutable_header()->set_rc_limit( 10 );
   t1.mutable_header()->set_payer( payer );
   t1.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   t1.set_id( sign( _key1, t1 ) );
   mempool.add_pending_transaction( t1, 1, max_payer_resources, 1, 2, 3 );

   protocol::transaction t2;
   payer = _key2.get_public_key().to_address_bytes();
   nonce_value.set_uint64_value( 2 );
   t2.mutable_header()->set_rc_limit( 10 );
   t2.mutable_header()->set_payer( payer );
   t2.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   t2.set_id( sign( _key2, t2 ) );
   mempool.add_pending_transaction( t2, 2, max_payer_resources, 1, 2, 3 );

   protocol::transaction t3;
   nonce_value.set_uint64_value( 3 );
   payer = _key1.get_public_key().to_address_bytes();
   t3.mutable_header()->set_rc_limit( 10 );
   t3.mutable_header()->set_payer( payer );
   t3.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   t3.set_id( sign( _key1, t3 ) );
   mempool.add_pending_transaction( t3, 3, max_payer_resources, 1, 2, 3 );

   protocol::transaction t4;
   nonce_value.set_uint64_value( 1 );
   t4.mutable_header()->set_rc_limit( 10 );
   t4.mutable_header()->set_payer( payer );
   t4.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   t4.set_id( sign( _key1, t4 ) );
   mempool.add_pending_transaction( t4, 4, max_payer_resources, 1, 2, 3 );

   protocol::transaction t5;
   payer = _key2.get_public_key().to_address_bytes();
   nonce_value.set_uint64_value( 1 );
   t5.mutable_header()->set_rc_limit( 10 );
   t5.mutable_header()->set_payer( payer );
   t5.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
   t5.set_id( sign( _key2, t5 ) );
   mempool.add_pending_transaction( t5, 5, max_payer_resources, 1, 2, 3 );

   BOOST_TEST_MESSAGE( "checking pending transaction list" );
   // Order should be t4, t1, t5, t2, t3
   // key1, key1, key2, key2, key1
   auto pending_txs = mempool.get_pending_transactions();
   BOOST_TEST_MESSAGE( "checking pending transactions size" );
   BOOST_REQUIRE_EQUAL( pending_txs.size(), 5 );
   BOOST_TEST_MESSAGE( "checking pending transaction ids" );
   BOOST_REQUIRE( pending_txs[0].transaction().id() == t4.id() ); // Height 1
   BOOST_REQUIRE( pending_txs[1].transaction().id() == t1.id() ); // Height 1
   BOOST_REQUIRE( pending_txs[2].transaction().id() == t5.id() ); // Height 2
   BOOST_REQUIRE( pending_txs[3].transaction().id() == t2.id() ); // Height 2
   BOOST_REQUIRE( pending_txs[4].transaction().id() == t3.id() ); // Height 5

   mempool.prune( 1 ); // Removes t4 and t1

   pending_txs = mempool.get_pending_transactions();
   BOOST_TEST_MESSAGE( "checking pending transactions size" );
   BOOST_REQUIRE_EQUAL( pending_txs.size(), 3 );
   BOOST_TEST_MESSAGE( "checking pending transaction ids" );
   BOOST_REQUIRE( pending_txs[0].transaction().id() == t5.id() ); // Height 2
   BOOST_REQUIRE( pending_txs[1].transaction().id() == t2.id() ); // Height 2
   BOOST_REQUIRE( pending_txs[2].transaction().id() == t3.id() ); // Height 5

   mempool.prune( 2 ); // Removed t5 and t2

   pending_txs = mempool.get_pending_transactions();
   BOOST_TEST_MESSAGE( "checking pending transactions size" );
   BOOST_REQUIRE_EQUAL( pending_txs.size(), 1 );
   BOOST_TEST_MESSAGE( "checking pending transaction ids" );
   BOOST_REQUIRE( pending_txs[0].transaction().id() == t3.id() ); // Height 5
}

BOOST_AUTO_TEST_SUITE_END()
