import os
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, List
from dataclasses import dataclass
from pathlib import Path
import credentials
import db_utils

from binance_sdk_spot.spot import Spot, ConfigurationRestAPI, SPOT_REST_API_PROD_URL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class CoinConfig:
    """Configuration for a cryptocurrency pair."""
    symbol: str
    column_name: str


class CryptoPriceTracker:
    """
    Production-grade cryptocurrency price tracker that fetches prices
    and stores them in a structured DataFrame format.
    """
    
    # Mapping of trading pairs to their DataFrame column names
    COIN_MAPPINGS: Dict[str, str] = {
        'BTCUSDT': 'benchmark_btc',
        'ETHUSDT': 'benchmark_eth',
        # 'BNBUSDT': 'benchmark_bnb',
        'SOLUSDT': 'benchmark_sol',
    }
    
    def __init__(self, api_key: str = None, api_secret: str = None):
        """
        Initialize the price tracker.
        
        Args:
            api_key: Binance API key (defaults to credentials.API_KEY)
            api_secret: Binance API secret (defaults to credentials.API_SECRET)
        """
        self.api_key = api_key or credentials.API_KEY
        self.api_secret = api_secret or credentials.API_SECRET
        
        # Initialize Binance client
        configuration_rest_api = ConfigurationRestAPI(
            api_key=self.api_key,
            api_secret=self.api_secret,
            base_path=SPOT_REST_API_PROD_URL,
            timeout=5000
        )
        self.client = Spot(config_rest_api=configuration_rest_api)
        
        # Initialize DataFrame
        self.df = pd.DataFrame(columns=['timestamp', 'pm', 'balance'])
        
        logger.info("CryptoPriceTracker initialized successfully")
    
    def get_ticker_price(self, symbol: str) -> Optional[float]:
        """
        Fetch the latest price for a given symbol.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            
        Returns:
            Current price as float, or None if error occurs
        """
        try:
            response = self.client.rest_api.ticker_price(symbol=symbol)
            
            rate_limits = response.rate_limits
            logger.debug(f"ticker_price({symbol}) rate limits: {rate_limits}")
            
            data = response.data()
            price = float(data.actual_instance.price)
            
            logger.info(f"Successfully fetched {symbol} price: {price}")
            return price
            
        except Exception as e:
            logger.error(f"Error fetching ticker price for {symbol}: {e}")
            return None
    
    def get_current_timestamp(self) -> datetime:
        """
        Get current timestamp with seconds set to 0.
        
        Returns:
            datetime object with seconds replaced by 0
        """
        now = datetime.now()
        return now.replace(second=0, microsecond=0)
    
    def add_price_record(self, symbol: str) -> bool:
        """
        Fetch latest price for a symbol and add it to the DataFrame.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            
        Returns:
            True if successful, False otherwise
        """
        if symbol not in self.COIN_MAPPINGS:
            logger.error(f"Symbol {symbol} not found in COIN_MAPPINGS")
            return False
        
        price = self.get_ticker_price(symbol)
        if price is None:
            return False
        
        timestamp = self.get_current_timestamp()
        pm = self.COIN_MAPPINGS[symbol]
        
        new_record = pd.DataFrame([{
            'timestamp': timestamp,
            'pm': pm,
            'balance': price
        }])
        
        self.df = pd.concat([self.df, new_record], ignore_index=True)
        
        logger.info(f"Added record: {timestamp} | {pm} | {price}")
        return True
    
    def add_multiple_prices(self, symbols: Optional[List[str]] = None) -> int:
        """
        Fetch and add prices for multiple symbols.
        
        Args:
            symbols: List of symbols to fetch. If None, fetches all configured symbols.
            
        Returns:
            Number of successfully added records
        """
        if symbols is None:
            symbols = list(self.COIN_MAPPINGS.keys())
        
        success_count = 0
        for symbol in symbols:
            if self.add_price_record(symbol):
                success_count += 1
        
        logger.info(f"Successfully added {success_count}/{len(symbols)} records")
        return success_count
    
    def get_dataframe(self) -> pd.DataFrame:
        """
        Get the current DataFrame.
        
        Returns:
            DataFrame containing all price records
        """
        return self.df.copy()
    
    def get_latest_price(self, pm: str) -> Optional[float]:
        """
        Get the most recent price for a given pm (benchmark).
        
        Args:
            pm: Performance metric name (e.g., 'benchmark_btc')
            
        Returns:
            Latest price or None if not found
        """
        filtered = self.df[self.df['pm'] == pm]
        if filtered.empty:
            return None
        return filtered.iloc[-1]['balance']
    
    def display_summary(self):
        """Display a summary of the current data."""
        if self.df.empty:
            logger.info("No data available")
            return
        
        print("\n" + "="*60)
        print("CRYPTO PRICE TRACKER SUMMARY")
        print("="*60)
        print(f"Total Records: {len(self.df)}")
        # print(f"Date Range: {self.df['timestamp'].min()} to {self.df['timestamp'].max()}")
        print("\nLatest Prices:")
        print("-"*60)
        
        for pm in self.df['pm'].unique():
            latest_price = self.get_latest_price(pm)
            print(f"{pm:20s}: ${latest_price:,.2f}")
        
        print("="*60 + "\n")

    def save_to_db(self):
        try:
            db_utils.df_to_table(table_name='balance_all_consolidated', df=self.df)
        except Exception as e:
            logger.error(f"Error saving to DB: {e}")

    def update_nav(self):
        try:
            query = 'select * from shares_table;'
            shares = db_utils.get_db_table(query=query)
            shares['timestamp'] = pd.to_datetime(shares['timestamp'])

            latest_shares = shares.sort_values(by='timestamp', ascending=False).drop_duplicates(subset='pm')

            result_df = pd.merge(self.df, latest_shares[['pm','shares']], on='pm', how='left')
            result_df['nav'] = result_df.apply(
                lambda row: 0 if pd.isna(row['shares']) or row['shares'] == 0 else row['balance'] / row['shares'],
                axis=1
            )
            print(result_df)

            db_utils.df_to_table(table_name='nav_table', df=result_df)

        except Exception as e:
            logger.error(f"Error udpaing nav_table: {e}")


def main():
    """Main execution function."""
    try:
        # Initialize tracker
        tracker = CryptoPriceTracker()
        
        # Fetch prices for all configured coins
        tracker.add_multiple_prices()
        
        # Display the DataFrame
        print("\nDataFrame Contents:")
        print(tracker.get_dataframe())
        
        # Display summary
        tracker.display_summary()
        
        tracker.save_to_db()
        tracker.update_nav()

        return tracker
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise


if __name__ == "__main__":
    tracker = main()