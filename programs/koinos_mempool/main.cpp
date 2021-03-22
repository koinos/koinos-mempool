
#include <koinos/mempool/mempool.hpp>

#include <boost/asio.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <koinos/exception.hpp>

#include <koinos/mq/client.hpp>
#include <koinos/mq/request_handler.hpp>
#include <koinos/pack/rt/binary.hpp>

#include <csignal>
#include <iostream>
#include <mutex>

namespace bpo = boost::program_options;
namespace bfs = boost::filesystem;

int main( int argc, char** argv )
{
   try
   {
      bpo::options_description options;
      options.add_options()
         ("help,h", "Print this help message and exit.")
         ("amqp,a", bpo::value< std::string >()->default_value( "amqp://guest:guest@localhost:5672/" ), "AMQP server URL")
         ("log-dir,l", bpo::value< bfs::path >(), "Directory to store rotating logs");

      bpo::variables_map args;
      bpo::store( bpo::parse_command_line( argc, argv, options ), args );

      if( args.count( "help" ) )
      {
         std::cout << options << "\n";
         return EXIT_FAILURE;
      }

      if( args.count("log-dir") )
      {
         auto log_dir = args["log-dir"].as< bfs::path >();
         if( log_dir.is_relative() )
            log_dir = bfs::current_path() / log_dir;

         koinos::initialize_logging( log_dir, "koinos_mempool_%3N.log" );
      }

      auto amqp_url = args.at( "amqp" ).as< std::string >();
      auto client = koinos::mq::client();
      auto ec = client.connect( amqp_url );
      if ( ec != koinos::mq::error_code::success )
      {
         LOG(error) << "Unable to connect amqp client";
         return EXIT_FAILURE;
      }

      koinos::mempool::mempool mempool;

      auto request_handler = koinos::mq::request_handler();
      request_handler.add_msg_handler(
         "koinos_rpc",
         "koinos_rpc_mempool",
         true,
         []( const std::string& content_type ){ return content_type == "application/json"; },
         [&]( const std::string& msg ) -> std::string
         {
            auto j = nlohmann::json::parse( msg );
            koinos::rpc::mempool::mempool_rpc_request args;
            koinos::pack::from_json( j, args );

            koinos::rpc::mempool::mempool_rpc_response resp;
            try
            {
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

      request_handler.add_msg_handler(
         "koinos_event",
         "koinos.transaction.accept",
         false,
         []( const std::string& content_type ) { return content_type == "application/json"; },
         [&]( const std::string& msg )
         {
            try
            {
               koinos::broadcast::transaction_accepted trx_accept;
               koinos::pack::from_json( nlohmann::json::parse( msg ), trx_accept );
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

      // TODO: handle block broadcasts in #7


      LOG(info) << "Starting mempool...";

      ec = request_handler.connect( amqp_url );
      if ( ec != koinos::mq::error_code::success )
      {
         LOG(error) << "Unable to connect amqp request handler";
         return EXIT_FAILURE;
      }

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
      LOG(fatal) << "unknown exception" << std::endl;
   }

   return EXIT_FAILURE;
}
