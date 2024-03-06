#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/system_timer.hpp>
#include <boost/program_options.hpp>

#include <yaml-cpp/yaml.h>

#include <koinos/broadcast/broadcast.pb.h>
#include <koinos/exception.hpp>
#include <koinos/mempool/mempool.hpp>
#include <koinos/mq/client.hpp>
#include <koinos/mq/request_handler.hpp>
#include <koinos/rpc/mempool/mempool_rpc.pb.h>
#include <koinos/util/conversion.hpp>
#include <koinos/util/options.hpp>
#include <koinos/util/random.hpp>
#include <koinos/util/services.hpp>

#include "git_version.h"

#define FIFO_ALGORITHM                 "fifo"
#define BLOCK_TIME_ALGORITHM           "block-time"
#define POB_ALGORITHM                  "pob"

#define HELP_OPTION                    "help"
#define VERSION_OPTION                 "version"
#define BASEDIR_OPTION                 "basedir"
#define AMQP_OPTION                    "amqp"
#define AMQP_DEFAULT                   "amqp://guest:guest@localhost:5672/"
#define LOG_LEVEL_OPTION               "log-level"
#define LOG_LEVEL_DEFAULT              "info"
#define LOG_DIR_OPTION                 "log-dir"
#define LOG_DIR_DEFAULT                ""
#define LOG_COLOR_OPTION               "log-color"
#define LOG_COLOR_DEFAULT              true
#define LOG_DATETIME_OPTION            "log-datetime"
#define LOG_DATETIME_DEFAULT           true
#define INSTANCE_ID_OPTION             "instance-id"
#define JOBS_OPTION                    "jobs"
#define JOBS_DEFAULT                   uint64_t( 2 )
#define TRANSACTION_EXPIRATION_OPTION  "transaction-expiration"
#define TRANSACTION_EXPIRATION_DEFAULT 120
#define FORK_ALGORITHM_OPTION          "fork-algorithm"
#define FORK_ALGORITHM_DEFAULT         FIFO_ALGORITHM

KOINOS_DECLARE_EXCEPTION( service_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( invalid_argument, service_exception );

using namespace boost;
using namespace koinos;

const std::string& version_string();
using timer_func_type = std::function< void( const boost::system::error_code&, std::shared_ptr< koinos::mempool::mempool >, std::chrono::seconds ) >;

int main( int argc, char** argv )
{
   std::atomic< bool > stopped = false;
   int retcode = EXIT_SUCCESS;
   std::vector< std::thread > threads;
   std::atomic< uint64_t > recently_added_count = 0;

   boost::asio::io_context main_ioc, server_ioc, client_ioc;
   auto request_handler = koinos::mq::request_handler( server_ioc );
   auto client = koinos::mq::client( client_ioc );
   auto timer = boost::asio::system_timer( server_ioc );

   timer_func_type timer_func = [&]( const boost::system::error_code& ec, std::shared_ptr< koinos::mempool::mempool > mpool, std::chrono::seconds exp_time )
   {
      static uint64_t pruned_count = 0;
      static auto last_message = std::chrono::system_clock::now();

      if ( ec == boost::asio::error::operation_aborted )
         return;

      pruned_count += mpool->prune( exp_time );

      auto now = std::chrono::system_clock::now();
      if ( now - last_message >= 1min )
      {
         LOG(info) << "Recently added " << recently_added_count << " transaction(s)";

         if ( pruned_count )
            LOG(info) << "Pruned " << pruned_count << " transaction(s) from mempool";

         recently_added_count = 0;
         pruned_count = 0;
         last_message = now;
      }

      timer.expires_after( 1s );
      timer.async_wait( boost::bind( timer_func, boost::asio::placeholders::error, mpool, exp_time ) );
   };

   try
   {
      program_options::options_description options;
      options.add_options()
         (HELP_OPTION                  ",h", "Print this help message and exit")
         (VERSION_OPTION               ",v", "Print version string and exit")
         (BASEDIR_OPTION               ",d", program_options::value< std::string >()->default_value( util::get_default_base_directory().string() ), "Koinos base directory")
         (AMQP_OPTION                  ",a", program_options::value< std::string >(), "AMQP server URL")
         (LOG_LEVEL_OPTION             ",l", program_options::value< std::string >(), "The log filtering level")
         (INSTANCE_ID_OPTION           ",i", program_options::value< std::string >(), "An ID that uniquely identifies the instance")
         (JOBS_OPTION                  ",j", program_options::value< uint64_t >(), "The number of worker jobs")
         (TRANSACTION_EXPIRATION_OPTION",e", program_options::value< uint64_t >(), "The number of seconds a transaction should expire in")
         (FORK_ALGORITHM_OPTION        ",f", program_options::value< std::string >(), "The fork resolution algorithm to use. Can be 'fifo', 'pob', or 'block-time'. (Default: 'fifo')")
         (LOG_DIR_OPTION                   , program_options::value< std::string >(), "The logging directory")
         (LOG_COLOR_OPTION                 , program_options::value< bool >(), "Log color toggle")
         (LOG_DATETIME_OPTION              , program_options::value< bool >(), "Log datetime on console toggle");

      program_options::variables_map args;
      program_options::store( program_options::parse_command_line( argc, argv, options ), args );

      if ( args.count( HELP_OPTION ) )
      {
         std::cout << options << std::endl;
         return EXIT_SUCCESS;
      }

      if ( args.count( VERSION_OPTION ) )
      {
         const auto& v_str = version_string();
         std::cout.write( v_str.c_str(), v_str.size() );
         std::cout << std::endl;
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

      auto amqp_url           = util::get_option< std::string >( AMQP_OPTION, AMQP_DEFAULT, args, mempool_config, global_config );
      auto log_level          = util::get_option< std::string >( LOG_LEVEL_OPTION, LOG_LEVEL_DEFAULT, args, mempool_config, global_config );
      auto log_dir            = util::get_option< std::string >( LOG_DIR_OPTION, LOG_DIR_DEFAULT, args, mempool_config, global_config );
      auto log_color          = util::get_option< bool >( LOG_COLOR_OPTION, LOG_COLOR_DEFAULT, args, mempool_config, global_config );
      auto log_datetime       = util::get_option< bool >( LOG_DATETIME_OPTION, LOG_DATETIME_DEFAULT, args, mempool_config, global_config );
      auto instance_id        = util::get_option< std::string >( INSTANCE_ID_OPTION, util::random_alphanumeric( 5 ), args, mempool_config, global_config );
      auto jobs               = util::get_option< uint64_t >( JOBS_OPTION, std::max( JOBS_DEFAULT, uint64_t( std::thread::hardware_concurrency() ) ), args, mempool_config, global_config );
      auto tx_expiration      = std::chrono::seconds( util::get_option< uint64_t >( TRANSACTION_EXPIRATION_OPTION, TRANSACTION_EXPIRATION_DEFAULT, args, mempool_config, global_config ) );
      auto fork_algorithm_opt = util::get_option< std::string >( FORK_ALGORITHM_OPTION, FORK_ALGORITHM_DEFAULT, args, mempool_config, global_config );

      std::optional< std::filesystem::path > logdir_path;
      if ( !log_dir.empty() )
      {
         logdir_path = std::make_optional< std::filesystem::path >( log_dir );
         if ( logdir_path->is_relative() )
            logdir_path = basedir / util::service::mempool / *logdir_path;
      }

      koinos::initialize_logging( util::service::mempool, instance_id, log_level, logdir_path, log_color, log_datetime );

      LOG(info) << version_string();

      KOINOS_ASSERT( jobs > 1, invalid_argument, "jobs must be greater than 1" );

      if ( config.IsNull() )
      {
         LOG(warning) << "Could not find config (config.yml or config.yaml expected), using default values";
      }

      state_db::fork_resolution_algorithm fork_algorithm;
      if ( fork_algorithm_opt == FIFO_ALGORITHM )
      {
         LOG(info) << "Using fork resolution algorithm: " << FIFO_ALGORITHM;
         fork_algorithm = state_db::fork_resolution_algorithm::fifo;
      }
      else if ( fork_algorithm_opt == BLOCK_TIME_ALGORITHM )
      {
         LOG(info) << "Using fork resolution algorithm: " << BLOCK_TIME_ALGORITHM;
         fork_algorithm = state_db::fork_resolution_algorithm::block_time;
      }
      else if ( fork_algorithm_opt == POB_ALGORITHM )
      {
         LOG(info) << "Using fork resolution algorithm: " << POB_ALGORITHM;
         fork_algorithm = state_db::fork_resolution_algorithm::pob;
      }
      else
      {
         KOINOS_THROW( invalid_argument, "${a} is not a valid fork algorithm", ("a", fork_algorithm_opt) );
      }

      LOG(info) << "Starting mempool...";
      LOG(info) << "Number of jobs: " << jobs;
      LOG(info) << "Transaction expiration: " << tx_expiration.count() << "s";

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

      threads.emplace_back( [&]() { client_ioc.run(); } );
      threads.emplace_back( [&]() { client_ioc.run(); } );

      for ( std::size_t i = 0; i < jobs; i++ )
         threads.emplace_back( [&]() { server_ioc.run(); } );

      std::shared_ptr< koinos::mempool::mempool > mempool = std::make_shared< koinos::mempool::mempool >( fork_algorithm );

      request_handler.add_broadcast_handler(
         "koinos.block.irreversible",
         [&]( const std::string& msg )
         {
            koinos::broadcast::block_irreversible block_irr;

            if ( !block_irr.ParseFromString( msg ) )
            {
               LOG(warning) << "Could not parse block irreversible broadcast";
               return;
            }

            try
            {
               mempool->handle_irreversibility( block_irr );
            }
            catch ( const koinos::exception& e )
            {
               LOG(info) << "Could not process block irreversibility: " << e.what();
            }
            catch ( const std::exception& e )
            {
               LOG(info) << "Could not process block irreversibility: " << e.what();
            }
         }
      );

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
                           mempool->check_pending_account_resources(
                              p.payer(),
                              p.max_payer_rc(),
                              p.rc_limit(),
                              p.has_block_id() ? util::converter::to< crypto::multihash >( p.block_id() ) : std::optional< crypto::multihash >{}
                           )
                        );

                        break;
                     }
                     case rpc::mempool::mempool_request::RequestCase::kGetPendingTransactions:
                     {
                        const auto& p = args.get_pending_transactions();
                        auto transactions = mempool->get_pending_transactions(
                           p.limit(),
                           p.has_block_id() ? util::converter::to< crypto::multihash >( p.block_id() ) : std::optional< crypto::multihash >{}
                        );
                        auto pending_trxs = resp.mutable_get_pending_transactions();
                        for ( const auto& trx : transactions )
                        {
                           pending_trxs->add_pending_transactions()->CopyFrom( trx );
                        }

                        break;
                     }
                     case rpc::mempool::mempool_request::RequestCase::kCheckAccountNonce:
                     {
                        const auto& p = args.check_account_nonce();
                        resp.mutable_check_account_nonce()->set_success(
                           mempool->check_account_nonce(
                              p.payee(),
                              p.nonce(),
                              p.has_block_id() ? util::converter::to< crypto::multihash >( p.block_id() ) : std::optional< crypto::multihash >{}
                           )
                        );

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
               auto rc_used = mempool->add_pending_transaction(
                  trx_accept.transaction(),
                  std::chrono::system_clock::now(),
                  trx_accept.receipt().max_payer_rc(),
                  trx_accept.receipt().disk_storage_used(),
                  trx_accept.receipt().network_bandwidth_used(),
                  trx_accept.receipt().compute_bandwidth_used()
               );

               broadcast::mempool_accepted accepted_broadcast;
               accepted_broadcast.mutable_transaction()->CopyFrom( trx_accept.transaction() );
               accepted_broadcast.mutable_receipt()->CopyFrom( trx_accept.receipt() );
               accepted_broadcast.set_height( trx_accept.height() );
               accepted_broadcast.set_pending_rc_used( rc_used );

               client.broadcast( "koinos.mempool.accept", util::converter::as< std::string >( accepted_broadcast ) );

               recently_added_count++;
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

            mempool->remove_pending_transactions( std::vector< mempool::transaction_id_type >{ trx_fail.id() } );
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
               mempool->handle_block( block_accept );
            }
            catch ( const std::exception& e )
            {
               LOG(warning) << "Could not process block accepted: " << e.what();
            }
         }
      );

      timer.expires_after( 1s );
      timer.async_wait( boost::bind( timer_func, boost::asio::placeholders::error, mempool, tx_expiration ) );

      LOG(info) << "Connecting AMQP client...";
      client.connect( amqp_url );
      LOG(info) << "Established AMQP client connection to the server";

      LOG(info) << "Connecting AMQP request handler...";
      request_handler.connect( amqp_url );
      LOG(info) << "Established request handler connection to the AMQP server";

      LOG(info) << "Listening for requests over AMQP";
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

   timer.cancel();

   for ( auto& t : threads )
      t.join();

   LOG(info) << "Shut down gracefully";

   return retcode;
}

const std::string& version_string()
{
   static std::string v_str = "Koinos Mempool v";
   v_str += std::to_string( KOINOS_MAJOR_VERSION ) + "." + std::to_string( KOINOS_MINOR_VERSION ) + "." + std::to_string( KOINOS_PATCH_VERSION );
   v_str += " (" + std::string( KOINOS_GIT_HASH ) + ")";
   return v_str;
}

