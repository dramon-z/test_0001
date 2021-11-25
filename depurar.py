# Use 'ISO-8859-1' instead of "utf-8" for decoding
import json
import logging
import os
import time
import csv
import datetime
import uuid
import requests
from key_handler import KeyHandler
import sqlite3
from sqlite3 import Error

import jsonschema
from jsonschema import validate

import pandas as pd

global logger

username = "IHM0915CMI"
password = "pr3uBa8l"
file_path= "file/eva_layout_5.csv"
XDEVOPSAUTH ="sDzeC29Wt72zbma8vJqFE5PvtLVbdBWjiEw"
XDEVOPSAPPNAME='apigee-dev'
xapikey= "GKD25Kfmc6VeoxGLAGtlrbYgZobDdB7S"
timerecord = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
subscriptionId = "2d622ff4-5c89-41e7-890a-eaf62c787df6" 

accentedCharacters = "àèìòùÀÈÌÒÙáéíóúýÁÉÍÓÚÝâêîôûÂÊÎÔÛãñõÃÑÕäëïöüÿÄËÏÖÜŸçÇßØøÅåÆæœ"
filterPatterns ="^([.,#A-Za-z " +  accentedCharacters + "0-9 _-]+)$"   
state =  "^([A-Za-z]+)$"
schema = {
  'title': 'Eva WithPrivacyNotice V2',
  'type': 'object',
  'required': ["privacyNotice","employmentVerification"],
  'properties': {
    'privacyNotice':{
        'type': "object",
        'required': ['fullName', 'address', 'acceptance', 'acceptanceDate'],
        'properties': {
            'fullName':{
                'type': "object",
                'required': ['firstName', 'firstSurname', 'secondSurname'],
                'properties': {
                    'firstName':{ 'type': 'string',"pattern": "^[A-Za-z " + accentedCharacters + "0-9\s]+$", 'minLength': 1},
                    'middleName':{ 'type': ['string','null'],"pattern": "^[A-Za-z " + accentedCharacters + "0-9\s]+$", 'minLength': 1},
                    'firstSurname':{'type': 'string',"pattern": "^[A-Za-z " + accentedCharacters + "0-9\s]+$"},
                    'secondSurname':{ 'type': 'string',"pattern": "^[A-Za-z " + accentedCharacters + "0-9\s]+$", 'minLength': 1},
                    'aditionalSurname':{'type': ['string','null'],"pattern": "^[A-Za-z " + accentedCharacters + "0-9\s]+$"}
                }
            },
            'address': {
                'type': 'object',
                'required': ["streetAndNumber","settlement","county","city","state","postalCode"],
                'properties': {
                    'streetAndNumber': {'type': "string",'pattern': filterPatterns, 'minLength':2, 'maxLength':40},
                    'settlement':{'type': "string",'pattern': filterPatterns, 'minLength':2, 'maxLength':60},
                    'county':{'type': "string",'pattern': filterPatterns, 'minLength':2, 'maxLength':60},
                    'city':{'type': "string",'pattern': filterPatterns, 'minLength':2, 'maxLength':40},
                    'state':{'type': "string",'pattern': state , 'minLength':2, 'maxLength':4},
                    'postalCode':{'type': "string",'pattern': '^\\d{5}$' , 'minLength':5, 'maxLength':10}
                }
            },
            'acceptance':{'type':['string'], "enum":["Y","N"]},
            'acceptanceDate': { 'type': ['string'], 'pattern': "^[1-9]\d{3}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{2}Z$" }
        }
    },
    'employmentVerification':{
        'type': "object",
        'required': ['employmentVerificationRequestId', 'subscriptionId', 'curp', 'email'],
        'properties': {
            'employmentVerificationRequestId': { 'type': ['string'], 'pattern': '^[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}$' },
            'subscriptionId': { 'type': ['string'], 'pattern': '^[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}$' },
            'curp': { 'type': ['string'], "pattern": '^[A-Z][AEIOUX][A-Z]{2}[0-9]{2}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])[MH]([ABCMTZ]S|[BCJMOT]C|[CNPST]L|[GNQ]T|[GQS]R|C[MH]|[MY]N|[DH]G|NE|VZ|DF|SP)[BCDFGHJ-NP-TV-Z]{3}[0-9A-Z][0-9]$'},
            'nss': { 'type': ['string','null'], 'pattern': '^\d{11}$' },
            'email': { 'type': ['string'], 'maxLength':80,'minLength':3, "pattern": """^(?:[a-z0-9!#$%&'*+/=?^_`{|}~]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])$""" }
        }
    }
  }
}

global countError
countError=0


def csv_to_json(csvFilePath):

    logger.info("Read file %s init" % (csvFilePath))
    jsonArray = []
    #read csv file
    with open(csvFilePath, encoding='UTF-8') as csvf: 
        #load csv file data using csv library's dictionary reader
        #csvReader = csv.DictReader(csvf,delimiter='|', quoting=csv.QUOTE_NONE) 
        csvReader = csv.DictReader(csvf) 
       
        #convert each csv row into python dict
        for row in csvReader: 
            #add this python dict to json array
            item={}
            for k, v in row.items():
                item[k.strip()]=v.strip()
            jsonArray.append(item)
  
    #convert python jsonArray to JSON String and write to file

    logger.info("Read file %s end" % (csvFilePath))
    return (jsonArray)



def initialize_logger():
    global logger
    format_colors = '%(asctime)s %(levelname)7s %(process)5d %(filename)20s %(lineno)3d %(message)s'
    logging.basicConfig(filename=f'logs/depurar_{timerecord}.log', format=format_colors, filemode='w', level=logging.INFO)
    logger = logging.getLogger()

def validateJson(jsonData):
    global countError
    try:
        validate(instance=jsonData, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        print(err)
        
        countError = countError + 1
        logger.info(f'Error: {err}' )
        return False
    return True

def main():
    global logger
    global countError
    start_time = time.time()
    initialize_logger()
        

    array = csv_to_json(file_path)
    count = 0
    for item in array:
        #print(item)
        logger.info("\n-------------------------------------------------------------------------------------------------------------------------------")
        
        requestId=str(uuid.uuid4())

        logger.info(f'RequestId: {requestId}')
    
        logger.info(f'Orign: {item}')
        jsonObj ={
            "privacyNotice": {
                "fullName": {
                    "firstName": item['primerNombre'],
                    "firstSurname": item['apellidoPaterno'],
                    "secondSurname": item['apellidoMaterno']
                },
                "address": {
                    "streetAndNumber": item['calleNumero'],
                    "settlement": item['colonia'],
                    "county": "MX",
                    "city": item['ciudad'],
                    "state": item['estado'],
                    "postalCode": item['codigoPostal'].zfill(5)
                },
                "acceptanceDate": item['fechaHoraAceptacion'],
                "acceptance": item['aceptacion']
            },
            "employmentVerification": {
                "employmentVerificationRequestId": requestId,
                "subscriptionId": subscriptionId,
                "curp": item['curp'],
                "nss": "92919084431",
                "email": item['email']
            }
        }
        isValid = validateJson(jsonObj)
        if isValid == False:
            logger.info(f'[{str(count)}] Request {isValid}: {jsonObj}' )
        count = count +1

 
       
    

    logger.info(f'Total Errores: {str(countError)} ')
    logger.info("Total %s seconds execution" % (time.time() - start_time))

if __name__ == '__main__':
    main()