import pandas as pd
from dateutil.parser import parse
from vars.vars import *

class JOURNAL_SQL():

    def __init__(self,tda_data,rh_data):
        self.tda_data = tda_data
        self.tda_df = self.pull_sql('tda')
        self.rh_data = rh_data
        self.rh_df = self.pull_sql('rh')
    
    def write_tda_sql(self):
        self.tda_df.to_sql('td_ameritrade',sql_engine,if_exists='replace',index=False)
        print('TDA Write to SQL Done')
    
    def write_rh_sql(self):
        self.rh_df.to_sql('robinhood',sql_engine,if_exists='replace',index=False)
        print('RH Write to SQL Done')

    def pull_sql(self,platform):
        query = {
            'tda':'''
                SELECT *
                FROM trading.td_ameritrade
            ''',
            'rh':'''
                SELECT *
                FROM trading.robinhood
            '''
        }
        # execute the sql query
        df = pd.read_sql(query[platform], sql_engine)
        df['buy_date'] = df['buy_date'].map(lambda x: str(x) if x is not None else x)
        df['sell_date'] = df['sell_date'].map(lambda x: str(x) if x is not None else x)
        df['buy_price'] = df['buy_price'].map(lambda x: float(x) if x is not None else x)
        df['sell_price'] = df['sell_price'].map(lambda x: float(x) if x is not None else x)

        return df
    
    def update_all_tables(self):
        query = '''
            (SELECT * FROM trading.td_ameritrade)
                UNION
            (SELECT * FROM trading.robinhood)
                UNION
            (SELECT * FROM trading.etrade)
                UNION
            (SELECT * FROM trading.td_ameritrade_futures)
                UNION
            (SELECT * FROM trading.tastyworks)

            ORDER BY buy_date DESC
        '''
        all_df = pd.read_sql(query,sql_engine)
        all_df.to_sql('all_trades',sql_engine,if_exists='replace',index=False)
        print('All SQL tables updated')
    
    # Function organizes the given stock info
    def orderOrganization(self, df, ticker, equity_type, tradeTime, quantity, platform, position, price):

        multiplier = {
            'OPTION':100,
            'EQUITY':1,
            'FUTURE':50
        }
        
        if position == 'OPENING':
            temp_list = [ticker,None,quantity,position,equity_type,price,None,tradeTime,None,platform]
            df.loc[len(df)] = temp_list
        else:
            # pull all rows that have open positions with the same ticker and equity type
            temp_df = df[ (df['position'] == 'OPENING') & (df['ticker'] == ticker) & (df['equity_type'] == equity_type)].sort_values('buy_date',ascending=True)
            
            # first go through all the matches
            df_index = 0
            while df_index < len(temp_df) and quantity > 0:
                # get the index of the lastest trade   
                last_index = temp_df.iloc[df_index].name
                
                # how many open quantities?
                open_quantity = df.at[last_index,'quantity']
                
                # check if there's same number of open positions as trying to close
                if open_quantity == quantity or open_quantity < quantity:

                    # change position opening
                    df.at[last_index,'position'] = position
                    # input sell_price
                    df.at[last_index,'sell_price'] = price
                    # sell date
                    df.at[last_index,'sell_date'] = tradeTime
                    # calc the profit_loss
                    df.at[last_index, 'profit_loss'] = round((price - df.at[last_index,'buy_price'])*quantity*multiplier[equity_type],2)

                    # next index
                    df_index += 1
                    
                # if there's more open orders than trying to close
                else:
                    # first step copy the open order (we'll close one of them)
                    df = df.append(df.loc[last_index]).reset_index(drop=True)

                    # this is still the open orders
                    df.at[len(df)-1,'quantity'] = open_quantity - quantity

                    # close out the other trades

                    # change position opening
                    df.at[last_index,'quantity'] = quantity
                    df.at[last_index,'position'] = position
                    # input sell_price
                    df.at[last_index,'sell_price'] = price
                    # sell date
                    df.at[last_index,'sell_date'] = tradeTime
                    # calc the profit_loss
                    df.at[last_index, 'profit_loss'] = round((price - df.at[last_index,'buy_price'])*quantity*100,2)

                # subtract quantities by the open_quantity
                quantity -= open_quantity

        return df

    # This function will import and analyze robinhood data
    def rh_orders(self):

        # SQL DB Column Titles
        columns = ['ticker','profit_loss','quantity','position','equity_type','buy_price','sell_price','buy_date','sell_date','platform']

        # Initialize empty dataframe if there's nothing in sql database
        if self.rh_df is None or len(self.rh_df) == 0:
            df = pd.DataFrame(columns=columns)
        else:
            df = self.rh_df

        # initialize counter for new items
        new_count = 1

        for i,row in self.rh_data.iterrows():
            ticker = row['symbol']
            equity_type = 'EQUITY'
            tradeTime = parse(row['created_at']).strftime('%Y-%m-%d %H:%M:%S')
            quantity = row['qty']
            platform = 'robinhood'
            position = 'OPENING' if row['side']=='buy' else 'CLOSING'
            price = row['avg_price']

            # 1. Is it already in the database? if found, just skip this entry
            
            # returns any matches found in the sql database
            sql_match = df[((df['buy_date'] == tradeTime) | (df['sell_date'] == tradeTime))
                & (df['ticker'] == ticker)
                & (df['equity_type'] == equity_type)
                & ((df['buy_price'] == price) | (df['sell_price'] == price))
            ]
            
            # no match is found, continue to do analysis
            if len(sql_match) == 0:
                print(new_count,'. Adding something new to ROBINHOOD database!')
                print('   ticker:     ',ticker)
                print('   price:      ',price)
                print('   trade time: ',tradeTime)
                print('   position:   ',position)
                print()

                # add a new count
                new_count += 1

                df = self.orderOrganization(df, ticker, equity_type, tradeTime, quantity, platform, position, price)
        
        self.rh_df = df
        return df

    # This function will update the existing SQL database and process the raw TDA data 
    def tda_orders(self):
        # SQL DB Column Titles
        columns = ['ticker','profit_loss','quantity','position','equity_type','buy_price','sell_price','buy_date','sell_date','platform']

        # Initialize empty dataframe if there's nothing in sql database
        if self.tda_df is None or len(self.tda_df) == 0:
            df = pd.DataFrame(columns=columns)
        else:
            df = self.tda_df

        # initialize counter for new items
        new_count = 1

        # Go through every data point
        for i in reversed(self.tda_data):

            # Go through each Activity Collection
            for j in i['orderActivityCollection']:

                # Go thorugh each Execution Leg
                for k in j['executionLegs']:
                    price = round(float(k['price']),2)
                    quantity = round(k['quantity'],1)
                    tradeTime = parse(k['time']).strftime('%Y-%m-%d %H:%M:%S')

                    # Get the leg ID correlation
                    legID = k['legId'] - 1

                    # assumption: legID corresponds to orderLegCollection index
                    ticker = i['orderLegCollection'][legID]['instrument']['symbol']
                    equity_type = i['orderLegCollection'][legID]['orderLegType']
                    action = i['orderLegCollection'][legID]['instruction']
                    position = i['orderLegCollection'][legID]['positionEffect']

                    # 1. Is it already in the database? if found, just skip this entry
            
                    # returns any matches found in the sql database
                    sql_match = df[((df['buy_date'].map(str) == tradeTime) | (df['sell_date'].map(str) == tradeTime))
                        & (df['ticker'] == ticker)
                        & (df['equity_type'] == equity_type)
                        & ((df['buy_price'] == price) | (df['sell_price'] == price))
                    ]
                    
                    # no match is found, continue to do analysis
                    if len(sql_match) == 0:
                        print(new_count,'. Adding something new to TD AMERITRADE database!')
                        print('   ticker:     ',ticker)
                        print('   price:      ',price)
                        print('   trade time: ',tradeTime)
                        print('   position:   ',position)
                        print()

                        # add a new count
                        new_count += 1
                        
                        # filters and re-organizes the data
                        df = self.orderOrganization(df, ticker, equity_type, tradeTime, quantity, 'td ameritrade', position, price)

        self.tda_df = df
        return df