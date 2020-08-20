import pandas as pd
from vars.vars import *

class JOURNAL_SQL():

    def __init__(self,data):
        self.data = data
        self.df = self.pull_sql()
    
    def write_tda_sql(self):
        self.df.to_sql('td_ameritrade',sql_engine,if_exists='replace',index=False)
        print('Write to SQL Done')

    def pull_sql(self):
        query = '''
            SELECT *
            FROM trading.td_ameritrade
        '''
        # execute the sql query
        df = pd.read_sql(query, sql_engine)
        return df
    
    # This function will update the existing SQL database and process the raw TDA data 
    def tda_orders(self):
        # SQL DB Column Titles
        columns = ['ticker','profit_loss','quantity','position','equity_type','buy_price','sell_price','buy_date','sell_date','platform']

        # Initialize empty dataframe if there's nothing in sql database
        if self.df is None or len(self.df) == 0:
            df = pd.DataFrame(columns=columns)
        else:
            df = self.df

        # counter for new items:
        new_count = 1

        # Go through every data point
        for i in reversed(self.data):

            # Go through each Activity Collection
            for j in i['orderActivityCollection']:

                # Go thorugh each Execution Leg
                for k in j['executionLegs']:
                    price = round(k['price'],2)
                    quantity = round(k['quantity'],1)
                    tradeTime = k['time']

                    # Get the leg ID correlation
                    legID = k['legId'] - 1

                    # assumption: legID corresponds to orderLegCollection index
                    ticker = i['orderLegCollection'][legID]['instrument']['symbol']
                    equity_type = i['orderLegCollection'][legID]['orderLegType']
                    action = i['orderLegCollection'][legID]['instruction']
                    position = i['orderLegCollection'][legID]['positionEffect']

                    # 1. Is it already in the database? if found, just skip this entry
            
                    # returns any matches found in the sql database
                    sql_match = df[((df['buy_date'] == tradeTime) ^ (df['sell_date'] == tradeTime))
                        & (df['ticker'] == ticker)
                        & (df['equity_type'] == equity_type)
                        & ((df['buy_price'] == price) ^ (df['sell_price'] == price))
                    ]
                    
                    # no match is found, continue to do analysis
                    if len(sql_match) == 0:
                        print(new_count,'. Adding something new to the database!')
                        print('   ticker:     ',ticker)
                        print('   trade time: ',tradeTime)
                        print('   position:   ',position)
                        print()
                        new_count += 1

                        # is it an opening or closing order
                        if position == 'OPENING':
                            # Create a list that will be added to the dataframe
                            temp_list = [ticker, None, quantity, position, equity_type, price, None, tradeTime, None, 'td ameritrade']
                            df.loc[len(df)] = temp_list
                        elif position == 'CLOSING':
                            # search for which one I am closing

                            # pull all rows that have open positions with the same ticker and equity type
                            temp_df = df[ (df['position'] == 'OPENING') & (df['ticker'] == ticker) & (df['equity_type'] == equity_type)]

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
                                    if equity_type == 'OPTION':
                                        df.at[last_index, 'profit_loss'] = round((price - df.at[last_index,'buy_price'])*quantity*100,2)
                                    else:
                                        df.at[last_index, 'profit_loss'] = round((price - df.at[last_index,'buy_price'])*quantity,2)
                                    
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
                                    if equity_type == 'OPTION':
                                        df.at[last_index, 'profit_loss'] = round((price - df.at[last_index,'buy_price'])*quantity*100,2)
                                    else:
                                        df.at[last_index, 'profit_loss'] = round((price - df.at[last_index,'buy_price'])*quantity,2)
                                    
                                # subtract quantities by the open_quantity
                                quantity -= open_quantity
        self.df = df
        return df