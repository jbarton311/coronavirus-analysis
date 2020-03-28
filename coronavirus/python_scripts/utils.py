import os
import pandas as pd
from pycountry import countries


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