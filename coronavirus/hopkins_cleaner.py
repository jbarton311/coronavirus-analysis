import pandas as pd
from datetime import datetime
from us_data_cleaner import USDataCleanUp

pd.options.display.max_rows=500

class HopkinsDataCleaner(object):
    """
    Accepts a raw URL from Hopkins GitHub and formats the data 
    to be fed into Power BI
    """
    def __init__(self, url, dataset_name):
        self.url = url
        self.dataset_name = dataset_name
        self.col_confirmed = f"confirmed_{self.dataset_name}"
        self.col_confirmed_prev_day = f"{self.col_confirmed}_prev_day"
        self.col_daily_diff = f"daily_diff_{self.dataset_name}"
        
    def read_initial_data(self):
        """Read data from URL"""
        # Read and clean up column headers
        JH_df = pd.read_csv(self.url)
        JH_df.columns = JH_df.columns.str.lower().str.replace(' ','_')
        JH_df = JH_df.rename(columns={'province/state':'province_state',
                               'country/region':'country_region'})
        self.data = JH_df
        
    def stack_initial_dataset(self):
        """
        Want the data in a stacked format
        """
        # Stack the dataset
        df = self.data.copy()
        
        index_cols = ['province_state', 'country_region', 'lat', 'long',]
        df = df.set_index(index_cols).stack().reset_index(name=self.col_confirmed)
        df = df.rename(columns={'level_4':'date'})
        df['date'] = pd.to_datetime(df['date'])

        self.data = df
    
    def handle_US_bad_data(self):
        """
        Pass data to a separate class that can
        clean up the bad US data
        """
        # Clean up column values
        # This class here will handle cleaning up the US porition of the data
        US_cleaner = USDataCleanUp(df=self.data,
                                   key_col=self.col_confirmed)
        US_cleaner.run()
        self.data = US_cleaner.data.copy()
        
    def clean_mid(self):
        """
        Some clean up we need to do AND add a RANK field
        """
        df = self.data.copy()
        # Fill blanks and concat country and state
        df['province_state'] = df['province_state'].fillna('Not Provided')
        df['country_region'] = df['country_region'].fillna('Not Provided')

        # Rank order dates by state/country
        df['state_and_country'] = df['province_state'] + "-" +  df['country_region']
        df['rank'] = df.groupby(['state_and_country'])['date'].rank(ascending=True)
        
        self.data = df

    def create_daily_diff_col(self):
        """
        Here we create a daily difference column
        """
        df = self.data.copy()
        
        # Prep dataset 1-day delta to get day-over-day change
        df['previous_days_date'] = df['date'] + pd.np.timedelta64(1, 'D')
        previous_df = df.copy()
        previous_df = previous_df[['province_state','country_region','previous_days_date',self.col_confirmed]]
        previous_df = previous_df.rename(columns={self.col_confirmed:self.col_confirmed_prev_day})

        # Join dataset to itself with a one day offset to get daily diff
        df = df.merge(previous_df,
                how='left',
                left_on=['province_state','country_region','date'],
                right_on=['province_state','country_region','previous_days_date'])
        
        df = df.drop(['previous_days_date_x','previous_days_date_y'], axis=1)      

        df[self.col_confirmed_prev_day] = df[self.col_confirmed_prev_day].fillna(0)
        df[self.col_daily_diff] = df[self.col_confirmed] - df[self.col_confirmed_prev_day]
        
        self.data = df
        
    def run(self):
        """
        Main run function to execute logic
        """
        self.read_initial_data()
        self.stack_initial_dataset()
        self.handle_US_bad_data()
        self.clean_mid()
        self.create_daily_diff_col()


