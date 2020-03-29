import os
import pandas as pd
from pycountry import countries


class AddDailyFields():
    """
    Takes the daily running total columns and 
    adds in daily new columns and PREV day
    """
    def __init__(self, data_obj):

        self.data = data_obj.data
        self.dataset_name = data_obj.dataset_name
        self.key_col = data_obj.key_col
        
        self.col_confirmed = f"running_total_{self.dataset_name}"
        self.col_confirmed_prev_day = f"{self.col_confirmed}_prev_day"
        self.col_daily_new = f"daily_new_{self.dataset_name}"

        
    def create_daily_new_col(self):
        """
        Here we create a daily new column
        """
        df = self.data

        # Prep dataset 1-day delta to get day-over-day change
        df["previous_days_date"] = df["date"] + pd.np.timedelta64(1, "D")
        previous_df = df.copy()
        previous_df = previous_df[[
                self.key_col,
                "previous_days_date",
                self.col_confirmed,
                ]]
        
        previous_df = previous_df.rename(
            columns={self.col_confirmed: self.col_confirmed_prev_day}
        )

        # Join dataset to itself with a one day offset to get daily diff
        df = df.merge(
            previous_df,
            how="left",
            left_on=[self.key_col, "date"],
            right_on=[self.key_col, "previous_days_date"],
        )

        df = df.drop(["previous_days_date_x", "previous_days_date_y"], axis=1)

        df[self.col_confirmed_prev_day] = df[self.col_confirmed_prev_day].fillna(0)
        df[self.col_daily_new] = (
            df[self.col_confirmed] - df[self.col_confirmed_prev_day]
        )

        self.data = df


def pull_median_country_age():
    """
    Pull median age by country data from Wikipedia
    """
    age_wiki = pd.read_html('https://en.wikipedia.org/wiki/List_of_countries_by_median_age')
    df = age_wiki[0]
    df.columns = df.columns.str.lower()
    df.columns = ['country','rank','median_years','male_years','female_years']
    
    # Add country codes
    df['country_code_2'] = df.apply(pandas_add_cc_2, axis=1)
    df['country_code_3'] = df.apply(pandas_add_cc_3, axis=1)

    df.loc[df['country'] == 'Virgin Islands', 'country_code_2'] = 'VG'
    df.loc[df['country'] == 'Virgin Islands', 'country_code_3'] = 'VGB'

    df.loc[df['country'] == 'Curacao', 'country_code_2'] = 'CW'
    df.loc[df['country'] == 'Curacao', 'country_code_3'] = 'CUW'

    df.loc[df['country'] == 'Sint Maarten', 'country_code_2'] = 'SX'
    df.loc[df['country'] == 'Sint Maarten', 'country_code_3'] = 'SXM'

    df.loc[df['country'] == 'Kosovo', 'country_code_2'] = 'XK'
    df.loc[df['country'] == 'Kosovo', 'country_code_3'] = 'XKX'

    df.loc[df['country'] == 'Niger', 'country_code_2'] = 'NE'
    df.loc[df['country'] == 'Niger', 'country_code_3'] = 'NER'

    df = df.drop_duplicates('country_code_3')

    directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    filename = os.path.join(directory, 'ref_data', 'ref_median_age_country.csv')
    
    df.to_csv(filename, index=False)

def pull_US_state_population_data():
    """
    Scrapes a Wikipedia page to pull US state population data
    """
    wiki = pd.read_html('https://simple.wikipedia.org/wiki/List_of_U.S._states_by_population')
    df = wiki[0]
    df.columns = df.columns.str.lower().str.replace(' ','_')
    df = df[['state','population_estimate,_july_1,_2019[2]']]
    df.columns = ['state','us_state_pop_2019_estimate']

    df.loc[df['state'] == 'District of Columbia', 'state'] = 'Washington DC'
    df.loc[df['state'] == 'U.S. Virgin Islands', 'state'] = 'Virgin Islands'

    directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    filename = os.path.join(directory, 'ref_data', 'ref_us_state_population.csv')

    df.to_csv(filename, index=False)

    return df

def create_FIPS_ref_data():
    """
    Load up a FIPS ref table from GitHub
    """
    FIPS = pd.read_json('https://raw.githubusercontent.com/josh-byster/fips_lat_long/master/fips_map.json')
    FIPS = FIPS.transpose().reset_index()
    FIPS = FIPS.rename(columns={'index':'fips'})
    FIPS['fips'] = FIPS['fips'].astype(str)

    directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    filename = os.path.join(directory, 'ref_data', 'FIPS_ref_data.csv')

    FIPS.to_csv(filename, index=False)


def load_FIPS_data():
    """ Load FIPS ref table """
    directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    filename = os.path.join(directory, 'ref_data', 'FIPS_ref_data.csv')
    df = pd.read_csv(filename)    
    df['fips'] = df['fips'].astype(str)
    return df

def load_ref_US_state():
    """ Load US state code ref table """
    directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    filename = os.path.join(directory, 'ref_data', 'ref_table_us_states.csv')
    df = pd.read_csv(filename)    
    df.columns = df.columns.str.lower().str.replace(' ','_')
    return df

def load_ref_US_county_info():
    """ Load US county info ref table 
    Unique to the county, state, latitude, and longitude
    """
    directory = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    filename = os.path.join(directory, 'ref_data', 'county_to_zip_ref.csv')
    df = pd.read_csv(filename)    
    df.columns = df.columns.str.lower().str.replace(' ','_')
    return df    

def pandas_add_cc_2(row):
    """Pandas function for pulling 2-letter country code"""
    country = row['country']
    #print(country)
    try:
        alpha_2 = countries.search_fuzzy(country)[0].alpha_2     
        return alpha_2
    except LookupError:
        print("no dice")
        pass 

def pandas_add_cc_3(row):
    """Pandas function for pulling 3-letter country code"""
    country = row['country']
    #print(country)
    try:
        alpha_3 = countries.search_fuzzy(country)[0].alpha_3     
        return alpha_3
    except LookupError:
        print("no dice")
        pass        