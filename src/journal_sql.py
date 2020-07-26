import pandas as pd
from vars.vars import *

class JOURNAL_SQL():

    def __init__(self,data):
        self.data = data
        self.order_df = pd.DataFrame()
    
    def write_sql(self):
        self.order_df.to_sql('trade',sql_engine,if_exists='replace',index=False)
    
    def clean_tda_orders(self):
        # SQL DB Column Titles
        columns = ['ticker','profit_loss','quantity','position','equity_type','buy_price','sell_price','buy_date','sell_date','platform']

        # Initialize empty dataframe
        df = pd.DataFrame(columns=columns)
        index = 0

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
                    
                    # is it an opening or closing order
                    if position == 'OPENING':
                        # Create a list that will be added to the dataframe
                        temp_list = [ticker, None, quantity, position, equity_type, price, None, tradeTime, None, 'td ameritrade']
                        df.loc[index] = temp_list
                        index += 1
                    elif position == 'CLOSING':
                        # search for which one I am closing
                        
                        # pull all rows that have open positions with the same ticker and equity type
                        temp_df = df[ (df['position'] == 'OPENING') & (df['ticker'] == ticker) & (df['equity_type'] == equity_type)]

                        # first go through all the matches
                        for df_index in range(len(temp_df)):
                        
                            # get the index of the last row   
                            last_index = temp_df.iloc[-1*df_index].name
                        
                            # how many open quantities?
                            open_quantity = df.at[last_index,'quantity']
                            
                            # check if there's same number of open positions as trying to close
                            if open_quantity == quantity or open_quantity < quantity:
                                if open_quantity < quantity:
                                    # decrement the open position quantity by the exectuion quantity
                                    quantity -= open_quantity

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
                            # if there's more open orders than trying to close
                            else:
                                # decrement the open position quantity by the execution quantity
                                df.at[last_index,'quantity'] = open_quantity - quantity
                                
                                # change the quantity to the execution amount
                                temp_df.at[last_index, 'quantity'] = quantity
                                # change position opening
                                temp_df.at[last_index,'position'] = position
                                # input sell_price
                                temp_df.at[last_index,'sell_price'] = price
                                # sell date
                                temp_df.at[last_index,'sell_date'] = tradeTime
                                # calc the profit_loss
                                if equity_type == 'OPTION':
                                    temp_df.at[last_index, 'profit_loss'] = round((price - temp_df.at[last_index,'buy_price'])*quantity*100,2)
                                else:
                                    temp_df.at[last_index, 'profit_loss'] = round((price - temp_df.at[last_index,'buy_price'])*quantity,2)
                    
                                # create a new row
                                df = df.append(temp_df.iloc[-1]).reset_index(drop=True)
                                index += 1

        self.order_df = df
        return df