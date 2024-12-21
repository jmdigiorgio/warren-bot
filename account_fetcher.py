import os
import logging
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AccountStatus

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

def get_account_data():
    """Fetch and print account data from Alpaca"""
    try:
        account = trading_client.get_account()
        account_data = {
            'id': str(account.id),
            'account_number': account.account_number,
            'status': account.status,
            'currency': account.currency,
            'cash': account.cash,
            'portfolio_value': account.portfolio_value,
            'pattern_day_trader': account.pattern_day_trader,
            'trading_blocked': account.trading_blocked,
            'transfers_blocked': account.transfers_blocked,
            'account_blocked': account.account_blocked,
            'created_at': account.created_at.isoformat() if account.created_at else None,
            'trade_suspended_by_user': account.trade_suspended_by_user,
            'multiplier': account.multiplier,
            'shorting_enabled': account.shorting_enabled,
            'equity': account.equity,
            'last_equity': account.last_equity,
            'long_market_value': account.long_market_value,
            'short_market_value': account.short_market_value,
            'initial_margin': account.initial_margin,
            'maintenance_margin': account.maintenance_margin,
            'last_maintenance_margin': account.last_maintenance_margin,
            'daytrade_count': account.daytrade_count,
            'buying_power': account.buying_power,
            'daytrading_buying_power': account.daytrading_buying_power,
            'regt_buying_power': account.regt_buying_power,
            'non_marginable_buying_power': account.non_marginable_buying_power
        }
        return account_data
    except Exception as e:
        logger.error(f"Error fetching Alpaca account data: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    get_account_data() 