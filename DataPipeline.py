from datetime import datetime, timedelta
from dagster import op, In, Out, graph
from typing import List
import yfinance as yf
import pandas as pd

@op(ins={"ticker": In(dagster_type=str)},
    out=Out(pd.DataFrame))
def get_ticker_data(context, ticker: str) -> pd.DataFrame:
    # Calculate start and end dates for the download
    time_period = 180 # this is the number of days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=time_period)
    
    # Fetching the data for Netflix ticker
    ticker_data = yf.Ticker(ticker)
    
    # Filtering the last 6 months / 180 days of ticker data
    ticker_data_filtered = ticker_data.history(start=start_date, end=end_date)

    return ticker_data_filtered

@op(out=Out(str))
def get_netflix() -> str:
    return "NFLX"

@op(out=Out(str))
def get_disney() -> str:
    return "DIS"

@op(ins={"data": In(dagster_type=pd.DataFrame)},          
    out=Out(pd.DataFrame)) 
def validate_data(context, data: pd.DataFrame) -> pd.DataFrame: 
    if data.empty: 
        context.log.error(f"Data invalid for ticker: {data.iloc[0]['Ticker']}")     
        return data 
    else: 
        context.log.info(f"Data valid for ticker: {data.iloc[0]['Ticker']}") 
        return data
    
@op(ins={"data": In(dagster_type=pd.DataFrame)},
    out=Out(pd.DataFrame))
def clean_data(context, data: pd.DataFrame) -> pd.DataFrame:
    # Remove Adj Close columns from Data
    data.drop("Adj Close", axis=1, inplace=True)
    
    # Return the updated DataFrame
    return data