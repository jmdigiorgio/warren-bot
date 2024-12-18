import os
import time
import signal
import logging
import threading
from datetime import datetime
from typing import Dict, Optional

import schedule
import uvicorn
from fastapi import FastAPI
from pythonjsonlogger import jsonlogger
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AccountStatus
from supabase import create_client, Client

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

# Initialize FastAPI app
app = FastAPI()

# Initialize clients
try:
    ALPACA_API_KEY = os.environ['ALPACA_API_KEY']
    ALPACA_SECRET_KEY = os.environ['ALPACA_SECRET_KEY']
    SUPABASE_URL = os.environ['SUPABASE_URL']
    SUPABASE_KEY = os.environ['SUPABASE_KEY']
except KeyError as e:
    logger.error(f"Missing required environment variable: {e}")
    raise

# Get configurable settings
CHECK_INTERVAL_MINUTES = int(os.environ.get('CHECK_INTERVAL_MINUTES', '1'))

# Initialize trading client and validate paper trading access
trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)

def validate_alpaca_access():
    """Validate Alpaca API access and ensure it's a paper trading account"""
    try:
        account = trading_client.get_account()
        if not account.status == AccountStatus.ACTIVE:
            raise ValueError(f"Account status is {account.status}, expected ACTIVE")
        
        # Verify it's a paper account (account number starts with PA)
        if not account.account_number.startswith('PA'):
            logger.error(f"Account number format: {account.account_number}")
            raise ValueError("Not a paper trading account! Please use paper trading credentials")
        
        logger.info("Successfully validated Alpaca paper trading access", 
                   extra={
                       'account_status': account.status,
                       'account_number': account.account_number
                   })
    except Exception as e:
        logger.error(f"Failed to validate Alpaca access: {str(e)}")
        raise

# Validate access on startup
validate_alpaca_access()

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Global flag for graceful shutdown
is_running = True

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def get_account_balance() -> Optional[Dict]:
    """Fetch current account balance from Alpaca with retry logic"""
    try:
        account = trading_client.get_account()
        return {
            'cash': str(float(account.cash)),  # Convert to string for JSON
            'portfolio_value': str(float(account.portfolio_value)),  # Convert to string for JSON
            'created_at': datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Error fetching Alpaca balance: {str(e)}", exc_info=True)
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def store_balance(balance_data: Dict) -> None:
    """Store balance data in Supabase with retry logic"""
    try:
        supabase.table('overview_snapshot').insert(balance_data).execute()
        logger.info("Stored balance data", extra={'balance_data': balance_data})
    except Exception as e:
        logger.error(f"Error storing balance in Supabase: {str(e)}", exc_info=True)
        raise

def check_and_store_balance() -> None:
    """Main function to check balance and store it"""
    try:
        balance = get_account_balance()
        if balance:
            store_balance(balance)
    except Exception as e:
        logger.error(f"Failed to complete balance check and store: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint for Railway"""
    try:
        # Quick check if we can connect to Alpaca
        trading_client.get_account()
        return {"status": "healthy"}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {"status": "unhealthy", "error": str(e)}

def run_scheduler() -> None:
    """Run the scheduler in a separate thread"""
    logger.info(f"Starting scheduler with {CHECK_INTERVAL_MINUTES} minute interval")
    schedule.every(CHECK_INTERVAL_MINUTES).minutes.do(check_and_store_balance)
    
    # Run initial check
    check_and_store_balance()
    
    while is_running:
        schedule.run_pending()
        time.sleep(1)

def handle_shutdown(signum, frame) -> None:
    """Handle graceful shutdown"""
    global is_running
    logger.info("Received shutdown signal, stopping scheduler...")
    is_running = False

def main() -> None:
    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    # Start scheduler in a separate thread
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.start()
    
    # Start FastAPI server
    port = int(os.environ.get('PORT', '8000'))
    logger.info(f"Starting FastAPI server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
    
    # Wait for scheduler to finish
    scheduler_thread.join()
    logger.info("Shutdown complete")

if __name__ == "__main__":
    main() 