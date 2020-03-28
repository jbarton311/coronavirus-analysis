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

def test_US_metrics(df):
    """
    Make sure all province and states have same number
    of records
    """
    # Limit to only most recent day by source
    df_max_date = df.groupby(['data_source'])['date'].max().reset_index()
    latest_df = df.merge(df_max_date,
            how='inner',
            on=['data_source','date'])

    us = df.loc[df['country_or_region'] == 'US']
    latest_us = latest_df.loc[latest_df['country_or_region'] == 'US']

    LOGGER.info(f"US Total Cases: {latest_us['running_total_cases'].sum()}")
    LOGGER.info(f"US Total Deaths: {latest_us['running_total_deaths'].sum()}")

    assert latest_us['running_total_cases'].sum() == us['daily_new_cases'].sum()
    assert latest_us['running_total_deaths'].sum() == us['daily_new_deaths'].sum()


def log_quick_readout_last_5_days(df): 
    global_df = df.groupby(['date'])[['daily_new_cases','daily_new_deaths']].sum().reset_index()
    LOGGER.info("Global recent results")
    LOGGER.info(f"\n{global_df.sort_values('date').tail(5)}")

    us = df.loc[df['country_or_region'] == 'US']
    us_grp = us.groupby(['date'])[['daily_new_cases','daily_new_deaths']].sum().reset_index()
    LOGGER.info("US recent results")
    LOGGER.info(f"\n{us_grp.sort_values('date').tail(5)}")   

LOGGER.info("START")

# Execute main logic
self = HopkinsDataFull()
self.run()
df = self.data

# Execute test cases
test_US_metrics(df)
log_quick_readout_last_5_days(df)

# Save CSV
df.to_csv('../output_data/HOPKINS_CLEANED.csv', index=False)
