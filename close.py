import pandas as pd
import os 
import sys
sys.path.append(r"C:\Users\aryan\AppData\Roaming\Python\Python311\site-packages")
import re
from tvDatafeed import TvDatafeed, Interval

username = "rialto364@gmail.com"
password = "TradingView@@123"

# Login to tvDatafeed (ensure you are logged in with TV credentials)
tv = TvDatafeed(username,password)

def clean_symbol(symbol):
    return re.sub(r'[^\w]', '_', symbol)


def fetch_weekly_data_tv(universe, output_prefix, bars=500):
    # Load stock symbols based on the selected universe
    universe_files = {
        'Small Cap': 'ind_niftysmallcap100list.csv',
        'Large Cap': 'ind_nifty50list.csv',
        'Mid Cap': 'ind_niftymidcap100list.csv',
        'Micro Cap': 'ind_niftymicrocap250_list.csv',
        'All Cap': 'nifty500.csv'
    }

    if universe not in universe_files:
        raise ValueError("Invalid universe selected. Choose from: Small Cap, Large Cap, Mid Cap, Micro Cap, All Cap.")

    symbols_df = pd.read_csv(universe_files[universe])
    symbols = symbols_df['Symbol'].tolist()  # Assuming 'Symbol' column exists

    # Initialize empty DataFrames
    open_df, high_df, low_df, close_df, volume_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    for symbol in symbols:
        symbol=clean_symbol(symbol)

        try:
            # Fetch historical weekly data
            data = tv.get_hist(symbol=symbol, exchange='NSE', interval=Interval.in_weekly, n_bars=bars)
            df=pd.DataFrame(data)

            if df.empty:
                print(f"No data for {symbol}")
                continue

            # Add data to respective DataFrames
            open_df[symbol] = data['open']
            high_df[symbol] = data['high']
            low_df[symbol] = data['low']
            close_df[symbol] = data['close']
            volume_df[symbol] = data['volume']

        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            continue

    # Set Date as index
    for df in [open_df, high_df, low_df, close_df, volume_df]:
        df.index.name = 'Date'

    # Save CSV files
    open_df.to_csv(f"{output_prefix}_open.csv")
    high_df.to_csv(f"{output_prefix}_high.csv")
    low_df.to_csv(f"{output_prefix}_low.csv")
    close_df.to_csv(f"{output_prefix}_close.csv")
    volume_df.to_csv(f"{output_prefix}_volume.csv")

    print("CSV files saved successfully!")

# Example usage
fetch_weekly_data_tv(universe='All Cap', output_prefix='weekly')
# new feature