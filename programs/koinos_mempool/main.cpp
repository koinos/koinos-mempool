#include <csignal>
#include <filesystem>
#include <iostream>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/program_options.hpp>

#include <koinos/exception.hpp>
#include <koinos/mempool/mempool.hpp>
#include <koinos/mq/request_handler.hpp>
#include <koinos/pack/rt/binary.hpp>
#include <koinos/util.hpp>

#define HELP_OPTION        "help"
#define AMQP_OPTION        "amqp"
#define BASEDIR_OPTION     "basedir"
#define LOG_FILTER_OPTION  "log-filter"
#define INSTANCE_ID_OPTION "instance-id"
using namespace boost;
using namespace koinos;

// If a transaction has not been included in ~1 hour, discard it
#define TRX_EXPIRATION_DELTA koinos::block_height_type(360)

constexpr uint32_t MAX_AMQP_CONNECT_SLEEP_MS = 30000;

int main( int argc, char** argv )
{
   try
   {
      program_options::options_description options;
      options.add_options()
         (HELP_OPTION   ",h", "Print this help message and exit")
         (AMQP_OPTION   ",a", program_options::value< std::string >()->default_value( "amqp://guest:guest@localhost:5672/" ), "AMQP server URL")
         (BASEDIR_OPTION",d", program_options::value< std::string >()->default_value( get_default_base_directory().string() ), "Koinos base directory")
         (LOG_FILTER_OPTION ",l", program_options::value< std::string >()->default_value( "info" ), "The log filtering level")
         (INSTANCE_ID_OPTION",i", program_options::value< std::string >()->default_value( random_alphanumeric( 5 ) ), "An ID that uniquely identifies the instance");

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

      auto instance_id = args[ INSTANCE_ID_OPTION ].as< std::string >();
      auto level       = args[ LOG_FILTER_OPTION ].as< std::string >();

      koinos::initialize_logging( service::mempool, instance_id, level, basedir / service::mempool );

      LOG(info) << "Starting mempool...";

      auto request_handler = koinos::mq::request_handler();
      auto amqp_url = args[ AMQP_OPTION ].as< std::string >();
      uint32_t amqp_sleep_ms = 1000;

      LOG(info) << "Connecting AMQP request handler...";
      while ( true )
      {
         auto ec = request_handler.connect( amqp_url );
         if ( ec == mq::error_code::success )
         {
            LOG(info) << "Connected request handler to AMQP server";
            break;
         }
         else
         {
            LOG(info) << "Failed, trying again in " << amqp_sleep_ms << " ms" ;
            std::this_thread::sleep_for( std::chrono::milliseconds( amqp_sleep_ms ) );
            amqp_sleep_ms = std::min( amqp_sleep_ms * 2, MAX_AMQP_CONNECT_SLEEP_MS );
         }
      }

      koinos::mempool::mempool mempool;

      request_handler.add_rpc_handler(
         koinos::service::mempool,
         [&]( const std::string& msg ) -> std::string
         {
            pack::json j;
            koinos::rpc::mempool::mempool_rpc_response resp;

            try
            {
               j = pack::json::parse( msg );
               koinos::rpc::mempool::mempool_rpc_request args;
               koinos::pack::from_json( j, args );

               std::visit(
                  koinos::overloaded {
                     [&]( const koinos::rpc::mempool::check_pending_account_resources_request& p )
                     {
                        resp = koinos::rpc::mempool::check_pending_account_resources_response {
                           .success = mempool.check_pending_account_resources(
                              p.payer,
                              p.max_payer_resources,
                              p.trx_resource_limit
                           )
                        };
                     },
                     [&]( const koinos::rpc::mempool::get_pending_transactions_request& p )
                     {
                        resp = koinos::rpc::mempool::get_pending_transactions_response {
                           .transactions = mempool.get_pending_transactions( p.limit )
                        };
                     },
                     [&]( const auto& )
                     {
                        resp = koinos::rpc::mempool::mempool_error_response {
                           .error_text = "Error: attempted to call unknown rpc"
                        };
                     }
                  },
                  args
               );
            }
            catch( const koinos::exception& e )
            {
               LOG(warning) << "Received bad message";
               LOG(warning) << " -> " << e.get_message();
               LOG(warning) << " -> " << e.get_stacktrace();

               resp = koinos::rpc::mempool::mempool_error_response {
                  .error_text = e.get_message(),
                  .error_data = e.get_stacktrace()
               };
            }

            j.clear();
            koinos::pack::to_json( j, resp );
            return j.dump();
         }
      );

      request_handler.add_broadcast_handler(
         "koinos.transaction.accept",
         [&]( const std::string& msg )
         {
            try
            {
               koinos::broadcast::transaction_accepted trx_accept;
               koinos::pack::from_json( pack::json::parse( msg ), trx_accept );
               mempool.add_pending_transaction(
                  trx_accept.transaction,
                  trx_accept.height,
                  trx_accept.payer,
                  trx_accept.max_payer_resources,
                  trx_accept.trx_resource_limit
               );
            }
            catch( const std::exception& e )
            {
               LOG(warning) << "Exception when handling transaction accepted broadcast: " << e.what();
            }
         }
      );

      request_handler.add_broadcast_handler(
         "koinos.block.accept",
         [&]( const std::string& msg )
         {
            try
            {
               koinos::broadcast::block_accepted block_accept;
               koinos::pack::from_json( nlohmann::json::parse( msg ), block_accept );
               for( const auto& trx : block_accept.block.transactions )
               {
                  mempool.remove_pending_transaction( trx.id );
               }

               if( block_accept.block.header.height > TRX_EXPIRATION_DELTA )
               {
                  mempool.prune( koinos::block_height_type( block_accept.block.header.height - TRX_EXPIRATION_DELTA ) );
               }
            }
            catch( const std::exception& e )
            {
               LOG(warning) << "Exception when handling block accepted broadcast: " << e.what();
            }
         }
      );

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
