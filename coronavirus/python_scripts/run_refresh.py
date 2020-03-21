from datetime import datetime
import os
import logging
import pandas as pd
from pycountry import countries

from hopkins_cleaner import HopkinsDataFull

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Adding a stream handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

LOGGER.addHandler(ch)

def test_us_province_counts(df):
    """
    Make sure all province and states have same number
    of records
    """
    us = df.loc[df['country_or_region'] == 'US']

    grp = us.groupby(['province_or_state'])['daily_new_cases'].size().reset_index()

    # Make sure all state names have same number of records
    assert grp['daily_new_cases'].min() == grp['daily_new_cases'].max()

def test_daily_vs_total_logic(df):
    max_date = df['date'].max()
    yesterday = df.loc[df['date'] == max_date]
    
    LOGGER.info(f"Total Cases: {df['daily_new_cases'].sum()}")
    LOGGER.info(f"Total Recoveries: {df['daily_new_recoveries'].sum()}")
    LOGGER.info(f"Total Deaths: {df['daily_new_deaths'].sum()}")                
                
    assert yesterday['running_total_cases'].sum() == df['daily_new_cases'].sum()
    assert yesterday['running_total_recoveries'].sum() == df['daily_new_recoveries'].sum()
    assert yesterday['running_total_deaths'].sum() == df['daily_new_deaths'].sum()

def log_quick_readout_last_5_days(df): 
    global_df = df.groupby(['date'])[['daily_new_cases','daily_new_recoveries','daily_new_deaths']].sum().reset_index()
    LOGGER.info("Global recent results")
    LOGGER.info(f"\n{global_df.sort_values('date').tail(5)}")

    us = df.loc[df['country_or_region'] == 'US']
    us_grp = us.groupby(['date'])[['daily_new_cases','daily_new_recoveries','daily_new_deaths']].sum().reset_index()
    LOGGER.info("US recent results")
    LOGGER.info(f"\n{us_grp.sort_values('date').tail(5)}")   

LOGGER.info("START")

# Execute main logic
self = HopkinsDataFull()
self.run()
df = self.data

# Execute test cases
test_us_province_counts(df)
test_daily_vs_total_logic(df)
log_quick_readout_last_5_days(df)

# Save CSV
df.to_csv('../output_data/HOPKINS_CLEANED.csv', index=False)
