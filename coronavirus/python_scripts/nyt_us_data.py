from datetime import datetime
import os
import pandas as pd


class NYTDataUS():
    """
    Class when run will pull coronavirus data from NYT repo
    and get it into appropriate format to combine with
    data from JHU
    """
    def __init__(self, dataset_name='cases'):
        self.dataset_name = dataset_name
        self.data = pd.DataFrame()
        
    def read_data(self):
        self.data = pd.read_csv('https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv',
                parse_dates=['date'],
                dtype={'fips':str})
        

    def initial_clean(self):
        df = self.data.copy()
        df['country_or_region'] = 'US'
        df = df.drop('fips', axis=1)

        df.rename(columns={'state':'province_or_state',
                          'cases':'running_total_cases',
                          'deaths':'running_total_deaths'},
                 inplace=True)

        # Clean up DC for later REF mapping
        df.loc[df['province_or_state'] == 'District of Columbia', 'province_or_state'] = 'Washington DC'
        
        self.data = df

    def grab_lat_long_from_ref(self):
        
        df = self.data.copy()
        dirname = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        ref_path = os.path.join(dirname, 'ref_data','ref_table_us_states.csv')

        ref_table = pd.read_csv(ref_path)
        ref_table.columns = ref_table.columns.str.lower().str.replace(" ", "_")

        df = df.merge(ref_table,
                how='left',
                left_on='province_or_state',
                right_on='state_name')

        df.drop(['state_name','state_code'], 
                axis=1,
               inplace=True)

        df = df.rename(columns={'latitude':'lat',
                          'longitude':'long'})
        
        self.data = df
        
    def keep_specific_metric(self):
        
        if self.dataset_name == 'cases':
            self.data = self.data.drop('running_total_deaths', axis=1)
        elif self.dataset_name == 'deaths':
            self.data = self.data.drop('running_total_cases', axis=1)
        else:
            print("No valid data name given for NYT data")
    
    def run(self):
        self.read_data()
        self.initial_clean()
        self.grab_lat_long_from_ref()
        self.keep_specific_metric()
