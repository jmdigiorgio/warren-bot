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

def get_positions_data():
    """Fetch positions data from Alpaca"""
    try:
        positions = trading_client.get_all_positions()
        positions_data = []
        
        for position in positions:
            position_data = {
                'asset_id': str(position.asset_id),
                'symbol': position.symbol,
                'exchange': position.exchange,
                'asset_class': position.asset_class,
                'asset_marginable': position.asset_marginable,
                'qty': position.qty,
                'avg_entry_price': position.avg_entry_price,
                'side': position.side,
                'market_value': position.market_value,
                'cost_basis': position.cost_basis,
                'unrealized_pl': position.unrealized_pl,
                'unrealized_plpc': position.unrealized_plpc,
                'unrealized_intraday_pl': position.unrealized_intraday_pl,
                'unrealized_intraday_plpc': position.unrealized_intraday_plpc,
                'current_price': position.current_price,
                'lastday_price': position.lastday_price,
                'change_today': position.change_today,
                'qty_available': position.qty_available
            }
            positions_data.append(position_data)
            
        return positions_data
    except Exception as e:
        logger.error(f"Error fetching Alpaca positions data: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    get_positions_data() 