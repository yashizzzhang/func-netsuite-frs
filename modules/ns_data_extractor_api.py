# @title API Module
import requests, logging, json
import pandas as pd
from requests.adapters import HTTPAdapter, Retry
from requests_oauthlib import OAuth1Session
from oauthlib import oauth1

class Netsuite:

    ## security related
    api_url = ''
    consumer_key = ''
    consumer_secret = ''
    token_id     = ''
    token_secret = ''
    realm = ''
    signature_method = 'HMAC-SHA256'
    version = '1.0'
    deploy = 1
    script = 1740
    standard_params = {}
    max_rows = 50000

    ## Initialize
    def __init__(self, 
                 account_id, 
                 realm,  
                 consumer_key, 
                 consumer_secret, 
                 token_id, 
                 token_secret, 
                 script, 
                 signature_method='HMAC-SHA256', 
                 version='1.0',  
                 deploy=1) -> None:
        
        logging.info('Netsuite: initializing ...')
        self.account_id = account_id
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.token_id = token_id
        self.token_secret = token_secret
        self.signature_method = signature_method
        self.version = version
        self.script = script
        self.deploy = deploy
        self.realm = realm
        self.api_url = f"https://{account_id}.restlets.api.netsuite.com/app/site/hosting/restlet.nl"
        self.standard_params = {
            'script': script,
            'deploy': deploy
        }

    ############################
    ## Low Level Data Retrieval
    ############################

    def post(self, body):

        client = OAuth1Session(
            client_secret=self.consumer_secret,
            client_key=self.consumer_key,
            resource_owner_key=self.token_id,
            resource_owner_secret=self.token_secret,
            realm=self.realm,
            signature_method=oauth1.SIGNATURE_HMAC_SHA256
        )

        headers = {
            "Prefer": "transient",
            "Content-Type": "application/json"
        }

        response = client.post(url=self.api_url, json=body, headers=headers, params=self.standard_params)
        return response.json()

    ##################
    ## runSuiteQLPaged
    ##################

    def runSuiteQLPaged(self, query=None, max_rows=max_rows):

        body= {
            "action":   "runSuiteQLPaged",
            "query":    query,
            "max_rows": max_rows
        }

        ## loop till last page
        try:
            result = self.post(body)
            return pd.DataFrame(result)

        except Exception as e:
            logging.info(f'Netsuite: runSuiteQLPaged() - Error: {e}')
            return False


    ####################
    ## runSuiteQLOverlay
    ####################

    def runSuiteQLOverlay(self, query=None, max_rows=max_rows):

        body= {
            "action":   "runSuiteQLOverlay",
            "query":    query,
            "max_rows": max_rows
        }

        ## loop till last page
        try:
            result = self.post(body)
            return pd.DataFrame(result)

        except Exception as e:
            logging.info(f'Netsuite: runSuiteQLOverlay() - Error: {e}')
            return False

    ####################
    ## buildSearch
    ####################

    def runSearch(self, query=None, max_rows=max_rows):

        body= {
            "action":   "searchBuild",
            "query":    json.loads(query),
            "max_rows": max_rows
        }

        try:

            ## Get the date in DataFrame
            result = self.post(body)
            df = pd.DataFrame(result)

            ## Columns from Saved search is default to string, convert Numver Column To Float
            for c in df.columns:
                try:
                    df[c] = pd.to_numeric(df[c])
                except:
                    pass

            return df

        except Exception as e:
            logging.info(f'Netsuite: buildSearch() - Error: {e}')
            return False