import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
from clock_fetcher import get_clock_data
from logger_config import get_logger

# Load environment variables (only in development)
if os.path.exists('.env'):
    load_dotenv()

# Get module logger
logger = get_logger('clock_publisher')

# Initialize Supabase client
try:
    SUPABASE_URL = os.environ['SUPABASE_URL']
    SUPABASE_SERVICE_ROLE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
except KeyError as e:
    logger.error(f"Missing required environment variable: {e}")
    raise

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def format_time_remaining(seconds):
    """Format seconds into hours, minutes, seconds string"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours} hours, {minutes} minutes, and {seconds} seconds"

def calculate_sleep_time(next_time_str):
    """Calculate seconds until the next market event (open)"""
    next_time = datetime.fromisoformat(next_time_str.replace('Z', '+00:00'))
    now = datetime.now(next_time.tzinfo)
    sleep_seconds = (next_time - now).total_seconds()
    return max(0, sleep_seconds)  # Ensure we don't return negative sleep time

def cleanup_old_snapshots():
    """Delete all but the most recent clock snapshot when market is closed"""
    try:
        logger.info("Cleaning up old clock snapshots")
        # Get the latest snapshot ID
        response = supabase.table('clock_snapshot').select('id').order('created_at', desc=True).limit(1).execute()
        if response.data:
            latest_id = response.data[0]['id']
            # Delete all snapshots except the latest
            supabase.table('clock_snapshot').delete().neq('id', latest_id).execute()
            logger.info("Successfully cleaned up old clock snapshots")
    except Exception as e:
        logger.error(f"Error cleaning up old clock snapshots: {str(e)}")

def publish_clock_data(force_open=False, test_mode=False):
    """
    Fetch and publish clock data to Supabase
    
    Args:
        force_open (bool): If True, treat market as open regardless of actual state
        test_mode (bool): If True, run once and exit
    """
    try:
        while True:
            # Get clock data and publish to database
            logger.info("Fetching clock data from Alpaca API")
            clock_data = get_clock_data(force_open=force_open)
            
            logger.info("Publishing clock data to Supabase")
            data = supabase.table('clock_snapshot').insert(clock_data).execute()
            
            if clock_data['is_open']:
                # Calculate and log time until market close
                time_to_close = calculate_sleep_time(clock_data['next_close'])
                logger.info(f"Market will close in {format_time_remaining(time_to_close)}")
                
                if test_mode:
                    logger.info("Test mode - Exiting after successful publish")
                    return clock_data
                    
                # When market is open, check every minute for unexpected closures
                time.sleep(60)
            else:
                # Clean up old snapshots when market is closed
                cleanup_old_snapshots()
                
                # When market is closed, calculate sleep time until next open
                total_sleep_time = calculate_sleep_time(clock_data['next_open'])
                logger.info(f"Market is closed. Next open in {format_time_remaining(total_sleep_time)}")
                
                if test_mode:
                    logger.info("Test mode - Exiting as market is closed")
                    return clock_data
                
                # Sleep until just before market opens, logging countdown each minute
                while total_sleep_time > 0:
                    if total_sleep_time > 60:
                        time.sleep(60)
                        total_sleep_time -= 60
                        logger.info(f"Market opens in {format_time_remaining(total_sleep_time)}")
                    else:
                        time.sleep(total_sleep_time)
                        break
                    
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Clock data publisher')
    parser.add_argument('--force-open', action='store_true', help='Force market to be treated as open')
    parser.add_argument('--test', action='store_true', help='Run once and exit')
    args = parser.parse_args()
    
    publish_clock_data(force_open=args.force_open, test_mode=args.test) 