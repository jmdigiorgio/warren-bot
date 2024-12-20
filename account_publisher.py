import os
import logging
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv
from supabase import create_client, Client
from account_fetcher import get_account_data
from clock_fetcher import get_clock_data

# Load environment variables (only in development)
if os.path.exists('.env'):
    load_dotenv()

# Configure logging
logger = logging.getLogger()
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt='%(asctime)s %(levelname)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# Initialize Supabase client
try:
    SUPABASE_URL = os.environ['SUPABASE_URL']
    SUPABASE_SERVICE_ROLE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
except KeyError as e:
    logger.error(f"Missing required environment variable: {e}")
    raise

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def publish_account_data(force_open=False, test_mode=False):
    """
    Fetch and publish account data to Supabase
    
    Args:
        force_open (bool): If True, treat market as open regardless of actual state
        test_mode (bool): If True, run once and exit
    """
    try:
        logger.info("Account publisher: Checking market status...")
        clock_data = get_clock_data(force_open=force_open)
        
        if clock_data['is_open']:
            logger.info("Account publisher: Market is open, fetching account data")
            account_data = get_account_data()
            
            # Map Alpaca's id to alpaca_id
            account_data['alpaca_id'] = account_data.pop('id')
            
            # Publish to Supabase
            data = supabase.table('account_snapshot').insert(account_data).execute()
            logger.info("Account publisher: Successfully published account data to database")
            
            if test_mode:
                logger.info("Account publisher: Test mode - Exiting after one successful publish")
                return account_data
        else:
            logger.info("Account publisher: Market is closed, skipping account data fetch")
            if test_mode:
                logger.info("Account publisher: Test mode - Exiting as market is closed")
                return None
                
    except Exception as e:
        logger.error(f"Account publisher error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Account data publisher')
    parser.add_argument('--force-open', action='store_true', help='Force market to be treated as open')
    parser.add_argument('--test', action='store_true', help='Run once and exit')
    args = parser.parse_args()
    
    publish_account_data(force_open=args.force_open, test_mode=args.test) 