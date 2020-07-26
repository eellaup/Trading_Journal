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
# *
#########################################################################

from src.auth import *
from src.tda_api import *
from src.journal_sql import *

def main():
    ############ Order Time Frame ###################################
    start_time = '2020-01-01'
    end_time = '2020-12-31'

    ### Get Authorization token ###
    authorize = AUTHORIZATION()
    auth_headers = authorize.auth_headers

    ### INTIALIZE TDA API OBJECT ###
    tda_api = TDA_API()
    # Performs the order query
    data = tda_api.orders(auth_headers, start_time, end_time)

    ### INITIALIZE JOURNAL OBJECT ###
    journal = JOURNAL_SQL(data)
    # clean TDA orders
    clean_orders = journal.clean_tda_orders()

    ### SQL ###
    journal.write_sql()

if __name__ == '__main__':
    main()