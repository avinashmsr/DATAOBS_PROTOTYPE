from datetime import datetime, timedelta
from dagster import op, In, Out, graph
from typing import List
import yfinance as yf
import pandas as pd


# Calculate start and end dates for the download

ticker = "NFLX" # Download the data for Netflix ticker
time_interval = "1m"
time_period = 1 # last 1 day(s) of ticker data
end_date = datetime.now().date()
start_date = end_date - timedelta(days=time_period)
data = yf.Ticker(ticker)
#print(data.info)

ticker_data = data.history(start=start_date, end=end_date, interval=time_interval)

ticker_data['Ticker'] = ticker

print("Number of rows = ", ticker_data.shape[0])
print(ticker_data.to_string())
#sorted_df = ticker_data.sort_values(by='Datetime')
#print(sorted_df)
#print(sorted_df.head())
#print(sorted_df.tail())
