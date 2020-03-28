from datetime import datetime
import os
import pandas as pd
import utils


class JHUDataGlobal():
    def __init__(self, dataset_name, url='not needed'):
        self.key_col = "state_and_country"
        self.url = url
        self.dataset_name = dataset_name
        self.col_confirmed = f"running_total_{self.dataset_name}"

        self.data = pd.DataFrame()
        self.JH_raw = pd.DataFrame()

    def read_initial_data(self):
        """Read data from URL"""
        JH_df = pd.read_csv(self.url)
        JH_df.columns = JH_df.columns.str.lower().str.replace(" ", "_")
        JH_df = JH_df.rename(
            columns={
                "province/state": "province_or_state",
                "country/region": "country_or_region",
            }
        )

        self.JH_raw = JH_df

        self.data = JH_df

    def stack_initial_dataset(self):
        """
        Want the data in a stacked format
        """
        # Stack the dataset
        df = self.data.copy()

        index_cols = [
            "province_or_state",
            "country_or_region",
            "lat",
            "long",
        ]
        df = df.set_index(index_cols).stack().reset_index(name=self.col_confirmed)
        df = df.rename(columns={"level_4": "date"})
        df["date"] = pd.to_datetime(df["date"])
        df['data_source'] = 'JHU'

        self.country_name_cleanup(df)
        self.data = df

    def country_name_cleanup(self, df):
        df.loc[df['country_or_region'] == 'Korea, South', 'country_or_region'] = 'South Korea'
        return df
    
    def clean_data(self):
        """
        Some clean up we need to do AND add a RANK field
        """
        df = self.data.copy()
        # Fill blanks and concat country and state
        df["province_or_state"] = df["province_or_state"].fillna("Not Provided")
        df["country_or_region"] = df["country_or_region"].fillna("Not Provided")

        df["state_and_country"] = (
            df["province_or_state"] + "-" + df["country_or_region"]
        )

        self.data = df
    
    def run(self):
        self.read_initial_data()
        self.stack_initial_dataset()
        self.clean_data()        
        
        

class JHUCountryCases():
    def __init__(self):
        url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
        self.dataset_name = 'cases'
        data_obj = JHUDataGlobal(url=url,
                        dataset_name=self.dataset_name)
        data_obj.run()

        enhanced = utils.AddDailyFields(data_obj=data_obj)
        enhanced.create_daily_new_col()
        self.data = enhanced.data
        
class JHUCountryDeaths():
    def __init__(self):
        url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
        self.dataset_name = 'deaths'
        data_obj = JHUDataGlobal(url=url,
                        dataset_name=self.dataset_name)
        data_obj.run()

        enhanced = utils.AddDailyFields(data_obj=data_obj)
        enhanced.create_daily_new_col()
        self.data = enhanced.data   