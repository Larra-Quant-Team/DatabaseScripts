# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 12:12:28 2019

@author: Aback
"""
import requests
import json
from requests.auth import HTTPBasicAuth


BASE_URL = 'https://api-ciq.marketintelligence.spglobal.com/gdsapi/rest/v3/clientservice.json'
USERNAME = 'apiadmin1@larrainvial.com'
PASSWORD = 'LarrainVial.2020'

class ApiCapitalIQ():

    def __init__(self, url=BASE_URL, username=USERNAME, password=PASSWORD):

        self._url = url
        self._username = username
        self._password = password

    def sendRequest(self, reqArray):
        if not isinstance(reqArray, list):
            reqArray = [reqArray]

        result = None

        try:
            result = requests.post(self._url,
                                   headers={'Content-Type':
                                            'application/json'},
                                   auth=HTTPBasicAuth(self._username,
                                                      self._password),
                                   verify=True,
                                   data=json.dumps({'inputRequests': reqArray})
                                   )

            if result.status_code != 200:
                comment_error = 'Request failed with status code {}'.format(result.status_code)
                print(comment_error)
                print('Error: {}'.format(result.json()))
                # result.raise_for_status()
                return None, result
            else:
                print('Successful request')

        except requests.exceptions.RequestException as e:
            print('Exception!')
            print(e)
            # sys.exit(1)
            raise(e)

        return result

    @staticmethod
    def createRequest(function, identifier, mnemonic, properties):

        if properties is None:
            properties = {}

        reqArray = {'function': function,
                    'identifier': identifier,
                    'mnemonic': mnemonic,
                    'properties': properties
                    }
        return reqArray

    def point_in_time(self, identifier, mnemonic, properties=None):

        return ApiCapitalIQ.createRequest('GDSP', identifier, mnemonic,
                                          properties)

    def historical_value(self, identifier, mnemonic, properties=None):

        return ApiCapitalIQ.createRequest('GDSHE', identifier, mnemonic,
                                          properties)



if __name__ == '__main__':

    # Get username, password and applicationid
    

    api = ApiCapitalIQ(base_url, username, password)

