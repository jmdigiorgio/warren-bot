import os
import time
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from positions_fetcher import get_positions_data
from logger_config import get_logger
import logging

# Load environment variables (only in development)
if os.path.exists('.env'):
    load_dotenv()

# Get module logger
logger = get_logger('positions_publisher')

# Disable HTTP request logging
logging.getLogger('httpx').setLevel(logging.WARNING)

# Initialize Supabase clients
try:
    SUPABASE_URL = os.environ['SUPABASE_URL']
    SUPABASE_SERVICE_ROLE_KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
    SUPABASE_ANON_KEY = os.environ['NEXT_PUBLIC_SUPABASE_ANON_KEY']
except KeyError as e:
    logger.error(f"[POSITIONS] Missing required environment variable: {e}")
    raise

# Client for reading clock data (public access)
supabase_reader = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
# Client for writing positions data (authenticated access)
supabase_writer = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def get_market_status():
    """Get the current market status from the clock_snapshot table"""
    try:
        # Get the latest clock snapshot using anon key
        logger.info("[POSITIONS] Fetching market status from Supabase")
        response = supabase_reader.table('clock_snapshot').select('*').order('created_at', desc=True).limit(1).execute()
        if response.data:
            return response.data[0]
        else:
            logger.error("[POSITIONS] No clock data found in database")
            return None
    except Exception as e:
        logger.error(f"[POSITIONS] Error fetching market status from database: {str(e)}")
        return None

def cleanup_old_snapshots():
    """Delete all but the most recent positions snapshot when market is closed"""
    try:
        logger.info("[POSITIONS] Cleaning up old positions snapshots")
        # Get the latest snapshot ID
        response = supabase_writer.table('positions_snapshot').select('id').order('created_at', desc=True).limit(1).execute()
        if response.data:
            latest_id = response.data[0]['id']
            # Delete all snapshots except the latest
            supabase_writer.table('positions_snapshot').delete().neq('id', latest_id).execute()
            logger.info("[POSITIONS] Successfully cleaned up old positions snapshots")
    except Exception as e:
        logger.error(f"[POSITIONS] Error cleaning up old positions snapshots: {str(e)}")

def publish_positions_data(force_open=False):
    """
    Fetch and publish positions data to Supabase
    
    Args:
        force_open (bool): If True, treat market as open regardless of actual state
    """
    try:
        while True:
            clock_data = get_market_status()
            
            if not clock_data:
                logger.error("[POSITIONS] Could not determine market status, waiting 60 seconds before retry")
                time.sleep(60)
                continue
            
            if force_open or clock_data['is_open']:
                # Fetch and publish positions data
                logger.info("[POSITIONS] Fetching positions data from Alpaca API")
                positions_data = get_positions_data()
                
                logger.info("[POSITIONS] Publishing positions data to Supabase")
                try:
                    # Delete existing positions before inserting new ones
                    supabase_writer.table('positions_snapshot').delete().neq('id', 0).execute()
                    # Insert new positions data
                    data = supabase_writer.table('positions_snapshot').insert(positions_data).execute()
                    logger.info("[POSITIONS] Positions data inserted successfully", extra={'inserted_data': data.data})
                except Exception as e:
                    logger.error(f"[POSITIONS] Failed to insert positions data: {str(e)}", 
                               extra={'positions_data': positions_data}, 
                               exc_info=True)
                    raise
                
                # When market is open, check every minute
                time.sleep(60)
            else:
                # Clean up old snapshots when market is closed
                cleanup_old_snapshots()
                
                # When market is closed, sleep until next check
                time.sleep(60)

    except Exception as e:
        logger.error(f"[POSITIONS] Error in publish_positions_data: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    import sys
    force_open = '--force-open' in sys.argv
    
    if force_open:
        logger.info("[POSITIONS] Market will be treated as open")
        
    publish_positions_data(force_open=force_open) 