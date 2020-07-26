from vars.vars import *
import requests

class TDA_API():
    
    def __init__(self):
        self.order_data = {}
    
    def orders(self, auth_headers, start_time, end_time):
        # redefine the start/end time for the order history
        order_payload['fromEnteredTime'] = start_time
        order_payload['toEnteredTime'] = end_time

        # submit the query
        content = requests.get(url = order_url, headers = auth_headers, params=order_payload)

        # convert the data into a JSON
        data = content.json()

        self.order_data = data
        return data