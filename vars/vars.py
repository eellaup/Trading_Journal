from vars.personal_vars import *
import sqlalchemy as sql

############ Login Code Token ###################################
method = "GET"
login_url = 'https://auth.tdameritrade.com/auth?'
login_payload = {
    'response_type':'code',
    'redirect_uri':'http://localhost/test',
    'client_id':client_id + '@AMER.OAUTHAP'
}

############ Auth Token ###################################
# define the endpoint
auth_url = r'https://api.tdameritrade.com/v1/oauth2/token'

# define headers
get_auth_headers = {
    'Content-Type':'application/x-www-form-urlencoded'
}

auth_payload = {
    'grant_type':'authorization_code',
    'access_type':'offline',
    'code':'',
    'client_id':client_id,
    'redirect_uri':'http://localhost/test'
}

############ Order Query ###################################
# order query url
order_url = 'https://api.tdameritrade.com/v1/orders'

# payload parameters
order_payload = {
    "accountId":account_num,
    'fromEnteredTime':'',
    'toEnteredTime':'end_time',
    'status':'FILLED'
}

############ Google Chrome Driver ###################################
executable_path = {'executable_path':r'C:\Program Files (x86)\chromeDriver\chromedriver'}

############ SQL Engine ###################################
sql_url = 'mysql://{}:{}@localhost:3306/trading'.format(sql_user,sql_pass)
sql_engine = sql.create_engine(sql_url)

############ Robinhood CSV filename ###################################
rh_filename = 'data/robinhood_data.csv'