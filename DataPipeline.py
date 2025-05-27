from datetime import datetime, timedelta
from dagster import op, In, Out, graph
from typing import List
import yfinance as yf
import pandas as pd

@op(ins={"ticker": In(dagster_type=str)},
    out=Out(pd.DataFrame))
def download_data(context, ticker: str) -> pd.DataFrame:
    # Calculate start and end dates for the download
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=1)
    
    # Download the data for Netflix ticker
    data = yf.download(ticker, start=start_date, end=end_date, interval="1m")

    # Filter to yesterday's data only
    yesterday = (datetime.now() - timedelta(days=1)).date()
    data = data.loc[data.index.date == yesterday]

    # Add column for ticker symbol
    data['Ticker'] = ticker

    return data

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