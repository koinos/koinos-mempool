#include <atomic>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/program_options.hpp>

#include <yaml-cpp/yaml.h>

#include <koinos/broadcast/broadcast.pb.h>
#include <koinos/exception.hpp>
#include <koinos/mempool/mempool.hpp>
#include <koinos/mq/request_handler.hpp>
#include <koinos/rpc/mempool/mempool_rpc.pb.h>
#include <koinos/util/conversion.hpp>
#include <koinos/util/options.hpp>
#include <koinos/util/random.hpp>
#include <koinos/util/services.hpp>

#define HELP_OPTION        "help"
#define BASEDIR_OPTION     "basedir"
#define AMQP_OPTION        "amqp"
#define AMQP_DEFAULT       "amqp://guest:guest@localhost:5672/"
#define LOG_LEVEL_OPTION   "log-level"
#define LOG_LEVEL_DEFAULT  "info"
#define INSTANCE_ID_OPTION "instance-id"
#define JOBS_OPTION        "jobs"
#define JOBS_DEFAULT       uint64_t( 2 )

KOINOS_DECLARE_EXCEPTION( service_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( invalid_argument, service_exception );

using namespace boost;
using namespace koinos;

namespace constants {
   // If a transaction has not been included in ~1 hour, discard it
   constexpr uint64_t trx_expiration_delta = 360;
}

int main( int argc, char** argv )
{
   std::atomic< bool > stopped = false;
   int retcode = EXIT_SUCCESS;
   std::vector< std::thread > threads;

   boost::asio::io_context main_ioc, server_ioc;
   auto request_handler = koinos::mq::request_handler( server_ioc );

   try
   {
      program_options::options_description options;
      options.add_options()
         (HELP_OPTION       ",h", "Print this help message and exit")
         (BASEDIR_OPTION    ",d", program_options::value< std::string >()->default_value( util::get_default_base_directory().string() ), "Koinos base directory")
         (AMQP_OPTION       ",a", program_options::value< std::string >(), "AMQP server URL")
         (LOG_LEVEL_OPTION  ",l", program_options::value< std::string >(), "The log filtering level")
         (INSTANCE_ID_OPTION",i", program_options::value< std::string >(), "An ID that uniquely identifies the instance")
         (JOBS_OPTION       ",j", program_options::value< uint64_t >(), "The number of worker jobs");

      program_options::variables_map args;
      program_options::store( program_options::parse_command_line( argc, argv, options ), args );

      if ( args.count( HELP_OPTION ) )
      {
         std::cout << options << std::endl;
         return EXIT_SUCCESS;
      }

      auto basedir = std::filesystem::path{ args[ BASEDIR_OPTION ].as< std::string >() };
      if ( basedir.is_relative() )
         basedir = std::filesystem::current_path() / basedir;

      YAML::Node config;
      YAML::Node global_config;
      YAML::Node mempool_config;

      auto yaml_config = basedir / "config.yml";
      if ( !std::filesystem::exists( yaml_config ) )
      {
         yaml_config = basedir / "config.yaml";
      }

      if ( std::filesystem::exists( yaml_config ) )
      {
         config = YAML::LoadFile( yaml_config );
         global_config = config[ "global" ];
         mempool_config = config[ util::service::mempool ];
      }

      auto amqp_url    = util::get_option< std::string >( AMQP_OPTION, AMQP_DEFAULT, args, mempool_config, global_config );
      auto log_level   = util::get_option< std::string >( LOG_LEVEL_OPTION, LOG_LEVEL_DEFAULT, args, mempool_config, global_config );
      auto instance_id = util::get_option< std::string >( INSTANCE_ID_OPTION, util::random_alphanumeric( 5 ), args, mempool_config, global_config );
      auto jobs        = util::get_option< uint64_t >( JOBS_OPTION, std::max( JOBS_DEFAULT, uint64_t( std::thread::hardware_concurrency() ) ), args, mempool_config, global_config );

      koinos::initialize_logging( util::service::mempool, instance_id, log_level, basedir / util::service::mempool / "logs" );

      KOINOS_ASSERT( jobs > 1, invalid_argument, "jobs must be greater than 1" );

      if ( config.IsNull() )
      {
         LOG(warning) << "Could not find config (config.yml or config.yaml expected), using default values";
      }

      LOG(info) << "Starting mempool...";
      LOG(info) << "Number of jobs: " << jobs;

      boost::asio::signal_set signals( server_ioc );
      signals.add( SIGINT );
      signals.add( SIGTERM );
#if defined( SIGQUIT )
      signals.add( SIGQUIT );
#endif

      signals.async_wait( [&]( const boost::system::error_code& err, int num )
      {
         LOG(info) << "Caught signal, shutting down...";
         stopped = true;
         main_ioc.stop();
      } );

      for ( std::size_t i = 0; i < jobs; i++ )
         threads.emplace_back( [&]() { server_ioc.run(); } );

      koinos::mempool::mempool mempool;

      request_handler.add_rpc_handler(
         util::service::mempool,
         [&]( const std::string& msg ) -> std::string
         {
            koinos::rpc::mempool::mempool_request args;
            koinos::rpc::mempool::mempool_response resp;

            if ( args.ParseFromString( msg ) )
            {
               try {
                  switch( args.request_case() )
                  {
                     case rpc::mempool::mempool_request::RequestCase::kCheckPendingAccountResources:
                     {
                        const auto& p = args.check_pending_account_resources();
                        resp.mutable_check_pending_account_resources()->set_success(
                           mempool.check_pending_account_resources(
                              p.payer(),
                              p.max_payer_rc(),
                              p.rc_limit()
                           )
                        );

                        break;
                     }
                     case rpc::mempool::mempool_request::RequestCase::kGetPendingTransactions:
                     {
                        const auto& p = args.get_pending_transactions();
                        auto transactions = mempool.get_pending_transactions( p.limit() );
                        auto pending_trxs = resp.mutable_get_pending_transactions();
                        for( const auto& trx : transactions )
                        {
                           pending_trxs->add_pending_transactions()->CopyFrom( trx );
                        }

                        break;
                     }
                     case rpc::mempool::mempool_request::RequestCase::kReserved:
                        resp.mutable_reserved();
                        break;
                     default:
                        resp.mutable_error()->set_message( "Error: attempted to call unknown rpc" );
                  }
               }
               catch( const koinos::exception& e )
               {
                  auto error = resp.mutable_error();
                  error->set_message( e.what() );
                  error->set_data( e.get_stacktrace() );
               }
               catch( std::exception& e )
               {
                  resp.mutable_error()->set_message( e.what() );
               }
               catch( ... )
               {
                  LOG(error) << "Unexpected error while handling rpc: " << args.ShortDebugString();
                  resp.mutable_error()->set_message( "Unexpected error while handling rpc" );
               }
            }
            else
            {
               LOG(warning) << "Received bad message";
               resp.mutable_error()->set_message( "Received bad message" );
            }

            std::stringstream out;
            resp.SerializeToOstream( &out );
            return out.str();
         }
      );

      request_handler.add_broadcast_handler(
         "koinos.transaction.accept",
         [&]( const std::string& msg )
         {
            koinos::broadcast::transaction_accepted trx_accept;

            if ( !trx_accept.ParseFromString( msg ) )
            {
               LOG(warning) << "Could not parse transaction accepted broadcast";
               return;
            }

            try
            {
               mempool.add_pending_transaction(
                  trx_accept.transaction(),
                  trx_accept.height(),
                  trx_accept.receipt().payer(),
                  trx_accept.receipt().max_payer_rc(),
                  trx_accept.receipt().rc_limit(),
                  trx_accept.receipt().disk_storage_used(),
                  trx_accept.receipt().network_bandwidth_used(),
                  trx_accept.receipt().compute_bandwidth_used()
               );
            }
            catch ( const std::exception& e )
            {
               LOG(info) << "Could not add pending transaction: " << e.what();
            }
         }
      );

      request_handler.add_broadcast_handler(
         "koinos.transaction.fail",
         [&]( const std::string& msg )
         {
            koinos::broadcast::transaction_failed trx_fail;

            if ( !trx_fail.ParseFromString( msg ) )
            {
               LOG(warning) << "Could not parse transaction failure broadcast";
               return;
            }

            mempool.remove_pending_transactions( std::vector< crypto::multihash >{ util::converter::to< crypto::multihash >( trx_fail.id() ) } );
         }
      );

      request_handler.add_broadcast_handler(
         "koinos.block.accept",
         [&]( const std::string& msg )
         {
            koinos::broadcast::block_accepted block_accept;

            if ( !block_accept.ParseFromString( msg ) )
            {
               LOG(warning) << "Could not parse block accepted broadcast";
               return;
            }

            try
            {
               std::vector< crypto::multihash > ids;
               const auto& block = block_accept.block();
               for ( int i = 0; i < block.transactions_size(); ++i )
               {
                  ids.emplace_back( util::converter::to< crypto::multihash >( block.transactions( i ).id() ) );
               }

               mempool.remove_pending_transactions( ids );

               if ( block.header().height() > constants::trx_expiration_delta )
               {
                  mempool.prune( block.header().height() - constants::trx_expiration_delta );
               }
            }
            catch ( const std::exception& e )
            {
               LOG(info) << "Could not remove pending transaction: " << e.what();
            }

            if ( block_accept.live() )
               LOG(info) << mempool.pending_transaction_count() << " pending transaction(s) exist in the pool";
         }
      );

      LOG(info) << "Connecting AMQP request handler...";
      request_handler.connect( amqp_url );
      LOG(info) << "Established connection to AMQP";

      auto work = asio::make_work_guard( main_ioc );
      main_ioc.run();
   }
   catch ( const invalid_argument& e )
   {
      LOG(error) << "Invalid argument: " << e.what();
      retcode = EXIT_FAILURE;
   }
   catch ( const koinos::exception& e )
   {
      if ( !stopped )
      {
         LOG(fatal) << "An unexpected error has occurred: " << e.what();
         retcode = EXIT_FAILURE;
      }
   }
   catch ( const boost::exception& e )
   {
      LOG(fatal) << "An unexpected error has occurred: " << boost::diagnostic_information( e );
      retcode = EXIT_FAILURE;
   }
   catch ( const std::exception& e )
   {
      LOG(fatal) << "An unexpected error has occurred: " << e.what();
      retcode = EXIT_FAILURE;
   }
   catch ( ... )
   {
      LOG(fatal) << "An unexpected error has occurred";
      retcode = EXIT_FAILURE;
   }

   for ( auto& t : threads )
      t.join();

   LOG(info) << "Shut down gracefully";

   return retcode;
}
