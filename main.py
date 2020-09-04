#########################################################################
# * Project           : Automated TD Ameritrade Trades
# *
# * Program name      : main.py
# *
# * Author            : Paul Lee (plee)
# *
# * Date created      : 20200718
# *
# * Purpose           : Automatically log TDA trades in a MySQL Server
# *
# * Revision History  :
# *
# * Date        Author      Ref    Revision (Date in YYYYMMDD format) 
# * 20200718    plee         1     Created bulk code 
# * 20200816    plee         2     Update SQL database, fixed bugs
# *
#########################################################################

from src.auth import *
from src.tda_api import *
from src.journal_sql import *
import pandas as pd

def main():
    ############ Order Time Frame ###################################
    start_time = '2020-01-01'
    end_time = '2020-12-31'

### TD AMERITRADE DATA COLLECTION
    ### Get Authorization token ###
    try:
        authorize = AUTHORIZATION()
        auth_headers = authorize.auth_headers
    except:
        print('ERROR: TD Ameritrade authorization failed')

    try:
        ### INTIALIZE TDA API OBJECT ###
        tda_api = TDA_API()
        # Performs the order query
        tda_data = tda_api.orders(auth_headers, start_time, end_time)
    except:
        print('ERROR: TD Ameritrade API call failed')

### ROBINHOOD DATA COLLECTION
    try:
        rh_data = pd.read_csv(rh_filename)
    except:
        print('ERROR: Robinhood CSV file is missing')

### GENERAL DATA ANALYSIS ####
    ### INITIALIZE JOURNAL OBJECT ###
    journal = JOURNAL_SQL(tda_data,rh_data)

    try:
        # process TDA orders
        tda_orders = journal.tda_orders()
        # write to TD Ameritrade SQL DB
        journal.write_tda_sql()
    except:
        print('ERROR: TD Ameritrade Data Analysis failed')

    try:
        # process Robinhood orders
        rh_orders = journal.rh_orders()
        # write to Robinhood SQL DB
        journal.write_rh_sql()
    except:
        print('ERROR: Robinhood Data Analysis failed')

    # update all the sql tables
    journal.update_all_tables()

if __name__ == '__main__':
    main()