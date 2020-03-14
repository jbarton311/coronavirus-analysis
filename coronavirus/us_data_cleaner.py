import pandas as pd
from datetime import datetime

us_state_abbrev = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Northern Mariana Islands':'MP',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Palau': 'PW',
    'Pennsylvania': 'PA',
    'Puerto Rico': 'PR',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virgin Islands': 'VI',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY',
}

class USDataCleanUp():
    """
    Take in a dataset, clean up the US data that has changed format,
    and then aggregate back into main dataset
    """
    def __init__(self, df, key_col):
        self.original_df = df
        #self.cleaned_us_data = pd.DataFrame()
        self.data = pd.DataFrame()
        self.key_col = key_col

    def setup_US_data(self):
        """
        Add a city and state column for old historical data
        Then split between old data format and new
        """
        df = self.original_df.copy()
        
        # Limit to US only
        df = df.loc[df['country_region'] =='US']
        
        # Split province and state by comma into 2 columns (city and state)
        df = pd.concat([df, df['province_state'].str.split(', ', expand=True)], axis=1)
        df = df.rename(columns={0:'city',
                  1:'state'})
        
        # Add a state abbreviation for new US data 
        df['US_new_data_state_abbrev'] = df['province_state'].map(us_state_abbrev)
        
        # The Diamond/Grand princess are in here but NOT states
        df['state_cleaned'] = df['US_new_data_state_abbrev'].combine_first(df['state']).combine_first(df['province_state'])

        # DC comes thru as DC and D.C.
        df['state_cleaned'] = df.state_cleaned.str.replace('.','')
        
        self.JEFF_TEST_DELETE = df
        # Split between old and new
        self.US_old = df.loc[df['date'] <= datetime(2020, 3, 9)]
        self.US_new = df.loc[df['date'] > datetime(2020, 3, 9)]
        
        
    def handle_old_data(self):
        """
        Logic to aggregate old data
        """
        old_df = self.US_old.groupby(['state_cleaned','date'])[self.key_col].sum().reset_index()
        return old_df
    
    def handle_new_data(self):
        """
        Logic to aggregate new data
        """
        new_df = self.US_new.groupby(['state_cleaned','date'])[self.key_col].sum().reset_index()
        #new_df = self.US_new[['state_cleaned','date','confirmed_cases']]
        return new_df
    
    def combine_US_data(self):
        """
        Combine old and new US data
        """
        df = pd.concat([self.handle_old_data(),
                                self.handle_new_data()])
        df['country_region'] = 'US'
        df = df.rename(columns={'state_cleaned':'province_state'})
        
        return df
        
    def prepare_final_cleaned_data(self):
        df1 = self.original_df
        df2 = self.combine_US_data()

        # Remove old US data
        df1 = df1.loc[df1['country_region'] != 'US']

        # Combine old data with cleaned US
        self.data = pd.concat([df1,df2],
                               axis=0)        

    def run(self):
        self.setup_US_data()
        self.combine_US_data()   
        self.prepare_final_cleaned_data()