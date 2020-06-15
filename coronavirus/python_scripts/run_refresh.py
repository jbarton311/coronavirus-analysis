from datetime import datetime
import os
import logging
import pandas as pd
from data_aggregators import (GlobalDataJHU, USDataNYT, CauseOfDeath, JHUCountryAggregate)
from utils import push_output_to_github, send_slack

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Adding a stream handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

LOGGER.addHandler(ch)


send_slack("START OF CORONA SCRIPT")
try:
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
    send_slack("CORONA script ran successfully")
except Exception as e:
    send_slack("============ ERROR WITH CORONA SCRIPT ============")
    send_slack(f"{e}")