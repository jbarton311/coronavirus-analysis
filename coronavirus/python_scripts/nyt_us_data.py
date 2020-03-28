from datetime import datetime
import os
import pandas as pd
import utils


class NYTDataStateLevel():
    """
    Class when run will pull coronavirus data from NYT repo
    and get it into appropriate format to combine with
    data from JHU
    """
    def __init__(self, dataset_name='cases'):
        self.dataset_name = dataset_name
        self.data = pd.DataFrame()
        self.key_col = 'state_and_county'
        
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

        df[self.key_col] = df['province_or_state'] + "-" + df['county']
         
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

class NYTDataCountyLevel(NYTDataStateLevel):
    """
    Pull COUNTY level data from NYT
    """
    def read_data(self):
        nyt = pd.read_csv('https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv')
        nyt['fips'] = nyt['fips'].fillna('-999').astype(int).astype(str)
        nyt['date'] = pd.to_datetime(nyt['date'])
        self.data = nyt
        
    def initial_clean(self):
        self.data['country_or_region'] = 'US'
        self.data['data_source'] = 'NYT'

        self.data.rename(columns={'state':'province_or_state',
                                  'cases':'running_total_cases',
                                  'deaths':'running_total_deaths'},
                         inplace=True)
        self.data['state_and_county'] = self.data['province_or_state'] + "-" + self.data['county']
        # Clean up DC for later REF mapping
        self.data.loc[self.data['province_or_state'] == 'District of Columbia', 
                                'province_or_state'] = 'Washington DC'

    def grab_lat_long_from_ref(self):
        FIPS = utils.load_FIPS_data()

        self.data = self.data.merge(FIPS[['fips','lat','long']],
                 how='left',
                 on='fips',
                 )

class NYTCountyCases():
    def __init__(self):
        self.key_col = 'state_and_county'
        self.dataset_name='cases'
        nyt = NYTDataCountyLevel(dataset_name=self.dataset_name)
        nyt.run()

        enhanced = utils.AddDailyFields(data_obj=nyt)
        enhanced.create_daily_new_col()
        self.data = enhanced.data
        
class NYTCountyDeaths():
    def __init__(self):
        self.key_col = 'state_and_county'
        self.dataset_name='deaths'
        nyt = NYTDataCountyLevel(dataset_name=self.dataset_name)
        nyt.run()
    
        enhanced = utils.AddDailyFields(data_obj=nyt)
        enhanced.create_daily_new_col()
        self.data = enhanced.data