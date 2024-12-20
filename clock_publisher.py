import os
import logging
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv
from supabase import create_client, Client
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

def publish_clock_data(force_open=False, test_mode=False):
    """
    Fetch and publish clock data to Supabase
    
    Args:
        force_open (bool): If True, treat market as open regardless of actual state
        test_mode (bool): If True, run once and exit
    """
    try:
        logger.info("Clock publisher: Checking market status...")
        # Get clock data
        clock_data = get_clock_data(force_open=force_open)
        
        if clock_data['is_open']:
            # Publish to Supabase
            data = supabase.table('clock_snapshot').insert(clock_data).execute()
            logger.info("Clock publisher: Successfully published clock data to database")
            
            if test_mode:
                logger.info("Clock publisher: Test mode - Exiting after one successful publish")
                return clock_data
        else:
            logger.info("Clock publisher: Market is closed, skipping clock data publish")
            if test_mode:
                logger.info("Clock publisher: Test mode - Exiting as market is closed")
                return None
                
    except Exception as e:
        logger.error(f"Clock publisher error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Clock data publisher')
    parser.add_argument('--force-open', action='store_true', help='Force market to be treated as open')
    parser.add_argument('--test', action='store_true', help='Run once and exit')
    args = parser.parse_args()
    
    publish_clock_data(force_open=args.force_open, test_mode=args.test) 