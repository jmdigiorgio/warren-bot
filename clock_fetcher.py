import os
import logging
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient

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

# Initialize clients
try:
    ALPACA_API_KEY = os.environ['ALPACA_API_KEY']
    ALPACA_SECRET_KEY = os.environ['ALPACA_SECRET_KEY']
except KeyError as e:
    logger.error(f"Missing required environment variable: {e}")
    raise

# Initialize trading client
trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)

def get_clock_data(force_open=False):
    """
    Fetch current market clock data from Alpaca
    
    Args:
        force_open (bool): If True, always return market as open (for testing)
    """
    try:
        clock = trading_client.get_clock()
        clock_data = {
            'is_open': True if force_open else clock.is_open,  # Force open if requested
            'next_open': clock.next_open.isoformat() if clock.next_open else None,
            'next_close': clock.next_close.isoformat() if clock.next_close else None,
            'timestamp': clock.timestamp.isoformat() if clock.timestamp else None
        }
        
        status = "open (forced)" if force_open and not clock.is_open else "open" if clock.is_open else "closed"
        logger.info(f"Market is currently {status}", extra={'clock_data': clock_data})
        
        return clock_data
    except Exception as e:
        logger.error(f"Error fetching clock data: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    # When run directly, show both normal and forced states
    print("\nNormal market state:")
    print(get_clock_data())
    print("\nForced open state:")
    print(get_clock_data(force_open=True)) 