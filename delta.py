# -*- coding: utf-8 -*-

"""
Options Delta scraped from various websites

"""

from bs4 import BeautifulSoup
import datetime
import lxml
import openpyxl
import os.path
import requests
import smtplib
import ssl
import time
import urllib3
import pandas as pd

PORT_TICKERS_FILE = 'port_tickers.xlsx' #need to leave header in excel


def db_price(ticker_ls): # For HK Options
  URL_1 = 'http://www.dbpower.com.hk/en/option/option-search?otype=ucode&ucode='
  URL_2 = '&hcode=&mdate='
  code = ticker_ls[0].zfill(5)
  exp = ticker_ls[2].split('/')
  exp_code = '20' + exp[-1] + '-' + exp[0] #YYYY-MM
  callput = ticker_ls[3][0].upper()
  strike = ticker_ls[3][1:]
  db_url = URL_1 + code + URL_2 + exp_code
  soup = get_soup(db_url)
  option_chain = soup.table.find(text = strike, class_ = 'strike').parent.find_all('td',class_='live_option_search')
  if callput == 'P':
    price = option_chain[-1].text if (price := option_chain[4].text) == '-' else price
  else:
    price = option_chain[3].text if (price := option_chain[0].text) == '-' else price
  if price == '-':
    price = 0 
  return price

def delta_grab(ticker): # Bloomberg ticker to source format and scrapes for delta
  URL_1 = 'http://www.dbpower.com.hk/en/option/quote-option?oid='
  # 1088 HK 09/29/21 C19 Equity
  code, exch, exp, cpstrike, sec_type = ticker.split()
  ats_code = get_hkats_code(ticker)
  cp = '7' if cpstrike[0].lower() == 'p' else '6'
  exp = exp[-2:] + exp[:2]
  strike = cpstrike[1:].split('.')
  trail = strike[1].ljust(3,'0') if len(strike) > 1 else '000'
  dbstrike = strike[0] + '.' + trail
  db_url = URL_1 + '^'.join([ats_code,cp,exp,dbstrike])
  soup = get_soup(db_url)
  delta = soup.table.find(text = 'Delta (%)').parent.find_next_sibling('td').text
  delta = str(float(delta)/100)

  return delta

def get_hkats_code(ticker):
  URL = 'https://www.hkex.com.hk/Products/Listed-Derivatives/Single-Stock/Stock-Options?sc_lang=en'
  bb_tckr = ticker.split()[0]
  soup = get_soup(URL)
  page = soup.find_all('tbody')
  for table in page:
    rows = table.find_all('tr')
    for row in rows:
      stock = row.find_all('td')
      sehk_code = stock[1].text.strip()
      ats_code = stock[3].text.strip()
      if sehk_code == bb_tckr:
        return ats_code

def get_soup(url):
  http = urllib3.PoolManager()
  r = http.request('GET', url)
  soup = BeautifulSoup(r.data, 'lxml')
  return soup

print(ticker + 'delta is ' + delta_grab('1088 hk 09/29/21 c18 equity'))

# TDP Loader Definitions
TDP_CRED_LS = [
              '#!CONNECT=HK053_RMO/HK053_RMO@PROD_HO3ORC08_FM.world',
              '#!MAX_ERROR=1000',
              '#!OPF=TDP_LOADER.import_price',
              ]
# file type header chooser
TDP_HEADERS_DICT = {
                    'px'    : [ #headers for this type of loader
                              '#in_ladder_date',
                              'in_ident_type',
                              'in_ext_ident',
                              'in_value_spec',
                              'in_price',
                              'in_hilo_ind',
                              'in_price_ccy',
                              'in_notes',
                              ],
                    'fx'    : [],
                    'delta' : [],
                  }

# TDP Loader Construction
# ldr_dict = {} # for loader content to be converted into pandas dataframe
# row = 0 # row counter in loader
# for line in TDP_CRED_LS: # this loop sets up the loader credentials into the dictionary to be converted into pandas df
#   ldr_list = [''] * len(TDP_HEADERS_DICT['px']) # sets number of blank cells and file width
#   ldr_list[0] = line # inserts tdp credentials into first column of row
#   ldr_dict[row] = ldr_list # add row to dataframe dict
#   row += 1
# ldr_dict[row] = TDP_HEADERS_DICT['px']
# row += 1
# for i in data_dict:
#   ldr_dict[row] = [loader_format_ddmmmyy,'BB_TCM',i,1,data_dict[i],'','','']
#   row +=1
# ldr_df = pd.DataFrame.from_dict(ldr_dict,orient='index')

# print(filename := 'tdp_loader_PRICE_' + file_format_yymmdd)
# ldr_df.to_excel(filename + '.xlsx',index=0,header=False) # save xlsx copy
# if os.path.exists(tdp_folder := 'C:\\tdp_loader\\hk053\\input\\'): # generate csv loader in tdp folder
#     ldr_df.to_csv(tdp_folder + filename + '.csv',index=0,header=False)