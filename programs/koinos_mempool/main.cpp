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
#include <koinos/util.hpp>

#define HELP_OPTION        "help"
#define BASEDIR_OPTION     "basedir"
#define AMQP_OPTION        "amqp"
#define AMQP_DEFAULT       "amqp://guest:guest@localhost:5672/"
#define LOG_LEVEL_OPTION   "log-level"
#define LOG_LEVEL_DEFAULT  "info"
#define INSTANCE_ID_OPTION "instance-id"

using namespace boost;
using namespace koinos;

// If a transaction has not been included in ~1 hour, discard it
#define TRX_EXPIRATION_DELTA uint64_t(360)

constexpr uint32_t MAX_AMQP_CONNECT_SLEEP_MS = 30000;

template< typename T >
T get_option(
   std::string key,
   T default_value,
   const program_options::variables_map& cli_args,
   const YAML::Node& service_config = YAML::Node(),
   const YAML::Node& global_config = YAML::Node() )
{
   if ( cli_args.count( key ) )
      return cli_args[ key ].as< T >();

   if ( service_config && service_config[ key ] )
      return service_config[ key ].as< T >();

   if ( global_config && global_config[ key ] )
      return global_config[ key ].as< T >();

   return std::move( default_value );
}

int main( int argc, char** argv )
{
   try
   {
      program_options::options_description options;
      options.add_options()
         (HELP_OPTION       ",h", "Print this help message and exit")
         (BASEDIR_OPTION    ",d", program_options::value< std::string >()->default_value( get_default_base_directory().string() ), "Koinos base directory")
         (AMQP_OPTION       ",a", program_options::value< std::string >(), "AMQP server URL")
         (LOG_LEVEL_OPTION  ",l", program_options::value< std::string >(), "The log filtering level")
         (INSTANCE_ID_OPTION",i", program_options::value< std::string >(), "An ID that uniquely identifies the instance");

      program_options::variables_map args;
      program_options::store( program_options::parse_command_line( argc, argv, options ), args );

      if( args.count( HELP_OPTION ) )
      {
         std::cout << options << std::endl;
         return EXIT_FAILURE;
      }

      auto basedir = std::filesystem::path{ args[ BASEDIR_OPTION ].as< std::string >() };
      if( basedir.is_relative() )
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
         mempool_config = config[ service::mempool ];
      }

      auto amqp_url    = get_option< std::string >( AMQP_OPTION, AMQP_DEFAULT, args, mempool_config, global_config );
      auto log_level   = get_option< std::string >( LOG_LEVEL_OPTION, LOG_LEVEL_DEFAULT, args, mempool_config, global_config );
      auto instance_id = get_option< std::string >( INSTANCE_ID_OPTION, random_alphanumeric( 5 ), args, mempool_config, global_config );

      koinos::initialize_logging( service::mempool, instance_id, log_level, basedir / service::mempool );

      if ( config.IsNull() )
      {
         LOG(warning) << "Could not find config (config.yml or config.yaml expected), using default values";
      }

      LOG(info) << "Starting mempool...";

      auto request_handler = koinos::mq::request_handler();

      koinos::mempool::mempool mempool;

      request_handler.add_rpc_handler(
         koinos::service::mempool,
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
                              p.max_payer_resources(),
                              p.trx_resource_limit()
                           )
                        );

                        break;
                     }
                     case rpc::mempool::mempool_request::RequestCase::kGetPendingTransactions:
                     {
                        const auto& p = args.get_pending_transactions();
                        auto transactions = mempool.get_pending_transactions( p.limit() );
                        for( const auto& trx : transactions )
                        {
                           *(resp.mutable_get_pending_transactions()->add_transactions()) = trx;
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

            mempool.add_pending_transaction(
               trx_accept.transaction(),
               trx_accept.height(),
               trx_accept.payer(),
               trx_accept.max_payer_resources(),
               trx_accept.trx_resource_limit()
            );
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

            const auto& block = block_accept.block();
            for ( int i = 0; i < block.transactions_size(); ++i )
            {
               mempool.remove_pending_transaction( crypto::multihash::from( block.transactions( i ).id() ) );
            }

            if( block.header().height() > TRX_EXPIRATION_DELTA )
            {
               mempool.prune( block.header().height() - TRX_EXPIRATION_DELTA );
            }
         }
      );

      LOG(info) << "Connecting AMQP request handler...";
      auto ec = request_handler.connect( amqp_url );
      if ( ec != mq::error_code::success )
      {
         LOG(info) << "Could not connect to AMQP server" ;
         exit( EXIT_FAILURE );
      }
      LOG(info) << "Established connection to AMQP";

      request_handler.start();

      boost::asio::io_service io_service;
      boost::asio::signal_set signals( io_service, SIGINT, SIGTERM );

      signals.async_wait( [&]( const boost::system::error_code& err, int num )
      {
         LOG(info) << "Caught signal, shutting down...";
         request_handler.stop();
      } );

      io_service.run();
   }
   catch ( const boost::exception& e )
   {
      LOG(fatal) << boost::diagnostic_information( e ) << std::endl;
   }
   catch ( const std::exception& e )
   {
      LOG(fatal) << e.what() << std::endl;
   }
   catch ( ... )
   {
      LOG(fatal) << "Unknown exception" << std::endl;
   }

   return EXIT_FAILURE;
}
