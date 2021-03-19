
#include <koinos/mempool/mempool.hpp>

#include <boost/asio.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <koinos/exception.hpp>

#include <koinos/mq/client.hpp>
#include <koinos/mq/request_handler.hpp>

#include <csignal>
#include <iostream>

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

      auto request_handler = koinos::mq::request_handler();
      // TODO: handle requests in #4, #5, #6, #7

      koinos::mempool::mempool mempool;
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
