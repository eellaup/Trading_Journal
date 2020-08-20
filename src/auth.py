from vars.vars import *
from splinter import Browser
import requests, time, urllib

### Description ###
# Get the temporary authorization token

class AUTHORIZATION():
    def __init__(self):
        # Get the auth code first
        self.auth_code = self.auth_code()
        # Then get the auth token
        self.auth_headers = self.auth_token()

    # Retrieves the auth code needed to retrieve the auth token
    def auth_code(self):
        # create a new instance of chrome driver
        browser = Browser('chrome',**executable_path, headless=False)

        # build the url
        built_url = requests.Request(method, login_url, params=login_payload).prepare()
        built_url = built_url.url

        # go to our URL
        browser.visit(built_url)

        # fill out element on the form
        browser.find_by_id("username0").first.fill(username)
        browser.find_by_id("password").first.fill(password)
        browser.find_by_id('accept').first.click()

        # Move to security questions
        browser.find_by_tag("details").first.click()
        browser.find_by_name('init_secretquestion').first.click()

        # Find the question on the page
        question = browser.find_by_tag("main").find_by_tag("div")[1].find_by_tag("p")[1].value.replace('Question: ','')
        # Answer the question
        browser.find_by_id("secretquestion0").first.fill(sec_quest[question])

        # submit
        browser.find_by_id('accept').first.click()

        # yes trust this device
        browser.find_by_id('stepup_trustthisdevice0').find_by_tag('div')[0].find_by_tag('label').first.click()
        # click save
        browser.find_by_id('accept').first.click()

        # submit again
        browser.find_by_id('accept').first.click()

        # give it a second to load
        time.sleep(1)

        # grab new url and parse
        new_url = browser.url
        code = urllib.parse.unquote(new_url.split('code=')[1])

        # close the browser
        browser.quit()

        return code
    
    # Retrieves the temporary authorization token
    def auth_token(self):
        # redefine the authorization code
        auth_payload['code'] = self.auth_code

        # post the data to get the token
        authReply = requests.post(auth_url, headers=get_auth_headers, data=auth_payload)

        # convert json to dictionary
        decoded_content = authReply.json()

        # grab the access token
        access_token = decoded_content["access_token"]
        auth_headers = {'Authorization':'Bearer {}'.format(access_token)}

        return auth_headers