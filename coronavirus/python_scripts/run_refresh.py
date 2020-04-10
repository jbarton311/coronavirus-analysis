from datetime import datetime
import os
import logging
import pandas as pd
from data_aggregators import (GlobalDataJHU, USDataNYT, CauseOfDeath, JHUCountryAggregate)
from utils import push_output_to_github

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Adding a stream handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

LOGGER.addHandler(ch)

# NYT section
NYT = USDataNYT()
NYT.run()

COD = CauseOfDeath(NYT)
COD.run()

# JHU section
JHU = GlobalDataJHU()
JHU.run()

country_agg = JHUCountryAggregate(JHU)
country_agg.run()

push_output_to_github()