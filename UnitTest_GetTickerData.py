from datetime import datetime, timedelta
from dagster import op, In, Out, graph
from typing import List
import yfinance as yf
import pandas as pd


# Calculate start and end dates for the download

ticker = "NFLX" # Download the data for Netflix ticker
#time_interval = "1m"
time_period = 180 # last 6 months / 180 days of ticker data
end_date = datetime.now().date()
start_date = end_date - timedelta(days=time_period)
data = yf.Ticker(ticker)
#print(data.info)

ticker_data = data.history(start=start_date, end=end_date)
print(ticker_data.to_string())