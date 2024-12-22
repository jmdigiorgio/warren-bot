import os
import time
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from account_fetcher import get_account_data
from logger_config import get_logger

# Load environment variables (only in development)
if os.path.exists('.env'):
    load_dotenv()

# Get module logger
logger = get_logger('account_publisher')

# Initialize Supabase clients
try:
    SUPABASE_URL = os.environ['SUPABASE_URL']
    SUPABASE_SERVICE_ROLE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
    SUPABASE_ANON_KEY = os.environ['NEXT_PUBLIC_SUPABASE_ANON_KEY']
except KeyError as e:
    logger.error(f"Missing required environment variable: {e}")
    raise

# Client for reading clock data (public access)
supabase_reader = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
# Client for writing account data (authenticated access)
supabase_writer = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def get_market_status():
    """Get the current market status from the clock_snapshot table"""
    try:
        # Get the latest clock snapshot using anon key
        logger.info("Fetching market status from Supabase")
        response = supabase_reader.table('clock_snapshot').select('*').order('created_at', desc=True).limit(1).execute()
        if response.data:
            return response.data[0]
        else:
            logger.error("No clock data found in database")
            return None
    except Exception as e:
        logger.error(f"Error fetching market status from database: {str(e)}")
        return None

def publish_account_data(force_open=False, test_mode=False):
    """
    Fetch and publish account data to Supabase
    
    Args:
        force_open (bool): If True, treat market as open regardless of actual state
        test_mode (bool): If True, run once and exit
    """
    try:
        while True:
            clock_data = get_market_status()
            
            if not clock_data:
                logger.error("Could not determine market status, waiting 60 seconds before retry")
                if not test_mode:
                    time.sleep(60)
                continue
            
            if force_open or clock_data['is_open']:
                # Fetch and publish account data
                logger.info("Fetching account data from Alpaca API")
                account_data = get_account_data()
                account_data['alpaca_id'] = account_data.pop('id')
                
                logger.info("Publishing account data to Supabase")
                data = supabase_writer.table('account_snapshot').insert(account_data).execute()
                logger.info("Account data updated")
                
                if test_mode:
                    logger.info("Test mode - Exiting after one successful publish")
                    return account_data
            else:
                if test_mode:
                    logger.info("Test mode - Exiting as market is closed")
                    return None
            
            if not test_mode:
                time.sleep(60)  # Sleep for 1 minute before next check
                    
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Account data publisher')
    parser.add_argument('--force-open', action='store_true', help='Force market to be treated as open')
    parser.add_argument('--test', action='store_true', help='Run once and exit')
    args = parser.parse_args()
    
    publish_account_data(force_open=args.force_open, test_mode=args.test) 