"""
Classes created here will retrieve data from
Johns Hopkins GitHub repo that contains
data on the Coronavirus
"""
# pylint: disable=invalid-name, line-too-long
import os
import pandas as pd
from pycountry import countries
from nyt_us_data import NYTDataUS
import utils


class HopkinsDataCleaner:
    """
    Accepts a raw URL from Hopkins GitHub and formats the data
    to be fed into Power BI
    """
    def __init__(self, url, dataset_name):
        self.url = url
        self.dataset_name = dataset_name
        self.col_confirmed = f"running_total_{self.dataset_name}"
        self.col_confirmed_prev_day = f"{self.col_confirmed}_prev_day"
        self.col_daily_new = f"daily_new_{self.dataset_name}"
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

        self.country_name_cleanup(df)
        self.data = df

    def country_name_cleanup(self, df):
        df.loc[df['country_or_region'] == 'Korea, South', 'country_or_region'] = 'South Korea'
        return df


    def handle_US_NYT_data(self):
        """
        US data is being sourced from NYT
        This will combine all JHU data (excluding US)
        with US data from NYT
        """

        nyt = NYTDataUS(dataset_name=self.dataset_name)
        nyt.run()


        # Remove US from JHU data
        self.data = self.data.loc[self.data['country_or_region'] != 'US']
        # Add in data source flags
        self.data['data_source'] = 'JHU'
        nyt.data['data_source'] = 'NYT'
        self.data = pd.concat([self.data, nyt.data], 
                             axis=0)

    def clean_mid(self):
        """
        Some clean up we need to do AND add a RANK field
        """
        df = self.data.copy()
        # Fill blanks and concat country and state
        df["province_or_state"] = df["province_or_state"].fillna("Not Provided")
        df["country_or_region"] = df["country_or_region"].fillna("Not Provided")

        # Rank order dates by state/country
        df["state_and_country"] = (
            df["province_or_state"] + "-" + df["country_or_region"]
        )
        # df['rank'] = df.groupby(['state_and_country'])['date'].rank(ascending=True)

        self.data = df

    def create_daily_new_col(self):
        """
        Here we create a daily new column
        """
        df = self.data.copy()

        # Prep dataset 1-day delta to get day-over-day change
        df["previous_days_date"] = df["date"] + pd.np.timedelta64(1, "D")
        previous_df = df.copy()
        previous_df = previous_df[
            [
                "province_or_state",
                "country_or_region",
                "previous_days_date",
                self.col_confirmed,
            ]
        ]
        previous_df = previous_df.rename(
            columns={self.col_confirmed: self.col_confirmed_prev_day}
        )

        # Join dataset to itself with a one day offset to get daily diff
        df = df.merge(
            previous_df,
            how="left",
            left_on=["province_or_state", "country_or_region", "date"],
            right_on=["province_or_state", "country_or_region", "previous_days_date"],
        )

        df = df.drop(["previous_days_date_x", "previous_days_date_y"], axis=1)

        df[self.col_confirmed_prev_day] = df[self.col_confirmed_prev_day].fillna(0)
        df[self.col_daily_new] = (
            df[self.col_confirmed] - df[self.col_confirmed_prev_day]
        )

        self.data = df

    def clean_final(self):
        """ Final data clean """
        self.data = self.data.rename(columns={"lat": "latitude", 
                                              "long": "longitude"})

    def run(self):
        """
        Main run function to execute logic
        """
        self.read_initial_data()
        self.stack_initial_dataset()
        self.handle_US_NYT_data()
        self.clean_mid()
        self.create_daily_new_col()
        self.clean_final()


class HopkinsDataFull:
    """
    Aggregates the 3 different datasets from the
    JHU GitHub repo
    """
    def __init__(self):
        url_confirmed = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
        #url_recovered = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv"
        url_deaths = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"

        self.confirmed = HopkinsDataCleaner(url=url_confirmed, dataset_name="cases")
        #self.recovered = HopkinsDataCleaner(url=url_recovered, dataset_name="recoveries")
        self.deaths = HopkinsDataCleaner(url=url_deaths, dataset_name="deaths")
        self.data = pd.DataFrame()

    def execute_clean_classes(self):
        """ Run all 3 different data classes """
        self.confirmed.run()
        #self.recovered.run()
        self.deaths.run()

    def initial_merge(self):
        """
        Combine cases, recovered, and deaths
        into a single dataset
        """

        """
        df_recovered = self.recovered.data[["running_total_recoveries",
                                            "date",
                                            "state_and_country",
                                            "running_total_recoveries_prev_day",
                                            "daily_new_recoveries",
                                          ]]
        """
        df_deaths = self.deaths.data[["running_total_deaths",
                                     "date",
                                     "state_and_country",
                                     "running_total_deaths_prev_day",
                                     "daily_new_deaths",
                                    ]]
        
        #df_final = self.confirmed.data.merge(df_recovered, how="left", on=["date", "state_and_country"])

        df_final = self.confirmed.data.merge(df_deaths, 
                                             how="left", 
                                             on=["date", "state_and_country"]
                                            )

        self.data = df_final

    def fix_latitude_longitude_us(self):
        """
        This will ensure that the new US data has proper
        longitude and latitude for the states
        """
        df = self.data.copy()
        dirname = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        ref_path = os.path.join(dirname, 'ref_data','ref_table_us_states.csv')

        ref_table = pd.read_csv(ref_path)
        ref_table.columns = ref_table.columns.str.lower().str.replace(" ", "_")

        # Join long/lat ref data to main data
        df = df.merge(
            ref_table,
            how="left",
            left_on="province_or_state",
            right_on="state_code",
            suffixes=("_old", "_ref"),
        )

        # COALESCE all long/lat fields
        df["new_lat"] = df["latitude_old"].combine_first(df["latitude_ref"])
        df["new_long"] = df["longitude_old"].combine_first(df["longitude_ref"])

        # Clean up columns
        df = df.drop(
            labels=[
                "longitude_old",
                "longitude_ref",
                "latitude_old",
                "latitude_ref",
                "state_code",
            ],
            axis=1,
        )
        df = df.rename(columns={"new_long": "longitude", "new_lat": "latitude"})

        self.data = df

    def zero_day_case_state(self):
        """
        Create a rank column for each STATE that tracks
        the first date a case was seen and then increments
        from there
        """
        df = self.data.copy()

        cases_only = df.loc[df["running_total_cases"] >= 5]
        cases_only["first_case_state_rank"] = cases_only.groupby(["state_and_country"])[
            "date"
        ].rank(ascending=True)
        cases_only = cases_only[["date", "state_and_country", "first_case_state_rank"]]

        df = df.merge(cases_only, how="left", on=["date", "state_and_country"])

        self.data = df

    def hundred_day_case_state(self):
        """
        Create a rank column for each STATE that tracks
        the first date a case was seen and then increments
        from there
        """
        df = self.data.copy()

        cases_only = df.loc[df["running_total_cases"] >= 100]
        cases_only["hundred_case_state_rank"] = cases_only.groupby(["state_and_country"])[
            "date"
        ].rank(ascending=True)
        cases_only = cases_only[["date", "state_and_country", "hundred_case_state_rank"]]

        df = df.merge(cases_only, how="left", on=["date", "state_and_country"])

        self.data = df

    def zero_day_case_country(self):
        """
        Create a rank column for each COUNTRY that tracks
        the first date a case was seen and then increments
        from there
        """
        df = self.data.copy()
        country_cases_only = df.loc[df["running_total_cases"] >= 5]

        # Only keep 1 record for each date and country
        country_cases_only = country_cases_only.drop_duplicates(
            ["country_or_region", "date"]
        )
        country_cases_only["first_case_country_rank"] = country_cases_only.groupby(
            ["country_or_region"]
        )["date"].rank(ascending=True)
        country_cases_only = country_cases_only[
            ["date", "country_or_region", "first_case_country_rank"]
        ]

        df = df.merge(country_cases_only, how="left", on=["date", "country_or_region"])

        self.data = df

    def hundred_day_case_country(self):
        """
        Create a rank column for each COUNTRY that tracks
        the first date a case was seen and then increments
        from there
        """
        df = self.data.copy()
        country_cases_only = df.loc[df["running_total_cases"] >= 100]

        # Only keep 1 record for each date and country
        country_cases_only = country_cases_only.drop_duplicates(
            ["country_or_region", "date"]
        )
        country_cases_only["hundred_case_country_rank"] = country_cases_only.groupby(
            ["country_or_region"]
        )["date"].rank(ascending=True)
        country_cases_only = country_cases_only[
            ["date", "country_or_region", "hundred_case_country_rank"]
        ]

        df = df.merge(country_cases_only, how="left", on=["date", "country_or_region"])

        self.data = df

    def add_in_country_codes(self):
        """
        Add in 2 and 3 letter country codes using
        pycountry module
        """
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
                'date',
                'province_or_state',
                'running_total_cases',
                'data_source',
                'state_and_country',
                'running_total_cases_prev_day',
                'daily_new_cases',
                'running_total_deaths',
                'running_total_deaths_prev_day',
                'daily_new_deaths',
                'state_name',
                'latitude',
                'longitude',
                'first_case_state_rank',
                'first_case_country_rank',
                'country_code_2',
                'country_code_3',
                'country_population_2018',
                'country_median_age',
                'country_running_agg',
                'hundred_case_state_rank',
                'hundred_case_country_rank',]]

    def run(self):
        """
        Main run function to execute logic
        """
        self.execute_clean_classes()
        self.initial_merge()
        self.fix_latitude_longitude_us()
        self.zero_day_case_state()
        self.zero_day_case_country()
        self.add_in_country_codes()
        self.add_country_population()
        self.add_country_median_age()
        self.add_country_daily_new_agg()
        self.hundred_day_case_state()
        self.hundred_day_case_country()
        self.order_cols()