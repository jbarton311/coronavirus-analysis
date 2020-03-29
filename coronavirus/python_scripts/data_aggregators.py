"""
Classes created here will retrieve data from
Johns Hopkins GitHub repo that contains
data on the Coronavirus
"""
# pylint: disable=invalid-name, line-too-long
import os
import logging
import pandas as pd
from pycountry import countries
from nyt_us_data import NYTCountyCases, NYTCountyDeaths
from jhu_global_data import JHUCountryCases, JHUCountryDeaths
import utils


LOGGER = logging.getLogger()

class GlobalDataJHU():
    def __init__(self):
        self.cases = JHUCountryCases().data
        self.deaths = JHUCountryDeaths().data
    
    def initial_merge(self):
        deaths = self.deaths[["running_total_deaths",
                                     "date",
                                     "state_and_country",
                                     "running_total_deaths_prev_day",
                                     "daily_new_deaths",
                                    ]]
        
        df = self.cases.merge(deaths, 
                     how="left", 
                     on=["date", "state_and_country"]
                    )

        self.df_original = df
        self.data = df                    
    
    def zero_day_field_creator(self,
                            key_col, 
                            new_col_name,
                            min_case_value):
        """
        Create a rank column for each COUNTY that tracks
        the first date a case was seen and then increments
        from there
        """
        print("Zero day for states")
        df = self.data

        cases_only = df.loc[df["running_total_cases"] >= min_case_value]
        states = cases_only.groupby([key_col,'date'])['daily_new_cases'].sum().reset_index()
        states[new_col_name] = states.groupby([key_col])["date"].rank(ascending=True)

        states = states[[key_col, new_col_name, 'date']]
        df = df.merge(states,
                how='left',
                on=[key_col,'date']) 

        self.data = df

    def zero_day_adds(self):
        self.zero_day_field_creator(key_col='state_and_country',
                                    new_col_name='first_case_state_rank',
                                    min_case_value=5)
        
        self.zero_day_field_creator(key_col='state_and_country',
                                    new_col_name='hundred_case_state_rank',
                                    min_case_value=100)
        
        self.zero_day_field_creator(key_col='country_or_region',
                                    new_col_name='first_case_country_rank',
                                    min_case_value=5)
        
        self.zero_day_field_creator(key_col='country_or_region',
                                    new_col_name='hundred_case_country_rank',
                                    min_case_value=100)     
        
    def add_in_country_codes(self):
        """
        Add in 2 and 3 letter country codes using
        pycountry module
        """
        print("Adding in country codes")
        df = self.data.copy()
        JH_countries = df[['country_or_region']].copy().drop_duplicates()

        JH_countries['country_code_2'] = ''
        JH_countries['country_code_3'] = ''

        for index, row in JH_countries.iterrows():
            country = row[0]
            #print(country)
            try:
                alpha_2 = countries.search_fuzzy(country)[0].alpha_2
                alpha_3 = countries.search_fuzzy(country)[0].alpha_3
                row['country_code_2'] = alpha_2
                row['country_code_3'] = alpha_3
            except LookupError:
                pass
            
        df = df.merge(JH_countries,
            how='left',
            on='country_or_region')
        
        self.data = df   

    def add_country_population(self):
        """
        Add in country population from ref table
        """
        dirname = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        ref_path = os.path.join(dirname, 'ref_data','ref_country_population.csv')
        ref = pd.read_csv(ref_path)
        ref = ref[['country_code_3','country_population_2018']].drop_duplicates()

        self.data = self.data.merge(ref,
                how='left',
                on='country_code_3')  

    def add_country_median_age(self):
        """
        Add in country median age from ref utils
        """        
        df = self.data.copy()
        
        directory = os.path.dirname(os.path.dirname(__file__))
        filepath = os.path.join(directory, 'ref_data','ref_median_age_country.csv')
        df_age = pd.read_csv(filepath)

        df = df.merge(df_age[['country_code_3','median_years']],
                how='left',
                on='country_code_3')
        df = df.rename(columns={'median_years':'country_median_age'})
        
        self.data = df                           

    def add_country_daily_new_agg(self):
        df = self.data
        agg_country_new = df.groupby(['country_or_region','date'])['running_total_cases'].sum().reset_index(name='country_running_agg')

        df = df.merge(agg_country_new,
                how='left',
                on=['country_or_region','date'])

        self.data = df

    def order_cols(self):
        self.data = self.data[['country_or_region',
                'province_or_state',
                'state_and_country',
                'date',
                'daily_new_cases',
                'running_total_cases',
                'running_total_cases_prev_day',
                'daily_new_deaths',
                'running_total_deaths',
                'running_total_deaths_prev_day',
                'data_source',
                'lat',
                'long',
                'first_case_state_rank',
                'first_case_country_rank',
                'hundred_case_state_rank',
                'hundred_case_country_rank',                
                'country_code_2',
                'country_code_3',
                'country_population_2018',
                'country_median_age',
                'country_running_agg',
                ]]

    def run(self):
        """
        Main run function to execute logic
        """
        self.initial_merge()
        self.zero_day_adds()
        self.add_in_country_codes()
        self.add_country_population()
        self.add_country_median_age()
        self.add_country_daily_new_agg()
        self.order_cols()  


class USDataNYT(GlobalDataJHU):
    def __init__(self):
        self.cases = NYTCountyCases().data
        self.deaths = NYTCountyDeaths().data

    def zero_day_adds(self):
        self.zero_day_field_creator(key_col='province_or_state',
                                    new_col_name='first_case_state_rank',
                                    min_case_value=5)
        
        self.zero_day_field_creator(key_col='province_or_state',
                                    new_col_name='hundred_case_state_rank',
                                    min_case_value=100)
        
        self.zero_day_field_creator(key_col='county',
                                    new_col_name='first_case_county_rank',
                                    min_case_value=5)
        
        self.zero_day_field_creator(key_col='county',
                                    new_col_name='hundred_case_county_rank',
                                    min_case_value=100)   
    
    def initial_merge(self):
        deaths = self.deaths[["running_total_deaths",
                                     "date",
                                     "state_and_county",
                                     "running_total_deaths_prev_day",
                                     "daily_new_deaths",
                                    ]]
        
        df = self.cases.merge(deaths, 
                     how="left", 
                     on=["date", "state_and_county"]
                    )    
        
        self.df_original = df
        self.data = df
        
    def order_cols(self):        
               self.data = self.data[['country_or_region',
                'province_or_state',
                'county',
                'state_and_county',
                'date',
                'daily_new_cases',
                'running_total_cases',
                'running_total_cases_prev_day',
                'daily_new_deaths',
                'running_total_deaths',
                'running_total_deaths_prev_day',
                'data_source',
                'fips',
                'lat',
                'long',
                'first_case_state_rank',
                'first_case_county_rank',
                'hundred_case_state_rank',
                'hundred_case_county_rank',                
                'country_code_2',
                'country_code_3',
                'country_population_2018',
                'us_state_pop_2019_estimate',
                'country_median_age',
                'country_running_agg',
                'state_code',
                ]]     
    
    def add_US_state_population(self):
        state_pop = utils.pull_US_state_population_data()

        self.data = self.data.merge(state_pop,
                how='left',
                left_on='province_or_state',
                right_on='state')   

    def add_US_state_codes(self):
        ref = utils.load_ref_US_state()

        self.data = self.data.merge(ref[['state_code','state_name']].drop_duplicates('state_name'),
                                    how='left',
                                    left_on='province_or_state',
                                    right_on='state_name')     
        
        self.data.drop('state_name', 
                       axis=1,
                       inplace=True)                                


    def run(self):
        """
        Main run function to execute logic
        """
        self.initial_merge()
        self.zero_day_adds()
        self.add_in_country_codes()
        self.add_country_population()
        self.add_country_median_age()
        self.add_country_daily_new_agg()
        self.add_US_state_population()
        self.add_US_state_codes()
        self.order_cols()  
        
                