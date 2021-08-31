
# -*- coding: utf-8 -*-
"""
Stock Prices scraped from AA Stocks

"""

from bs4 import BeautifulSoup
import requests
import lxml
import urllib3
import datetime
import openpyxl
import pandas as pd


PORT_TICKERS_FILE = 'port_tickers.xlsx'


def convert_ticker(old_ticker, px_source)
  return new_ticker


def yh_price(ticker)
  return price

def aa_price(ticker):
  #Convert Bloomberg ticker to AA Stock code to generate URL
  ticker_ls = ticker.split()
  code = ticker_ls[0].upper()
  sec_type =ticker_ls[-1].lower()

  INDEX_DICT = {
              'HSI':'110000.HK',
              'HSCEI':'110010.HK',
              'HSTECH':'110078.HK',
              'SHSZ300':'000300.SH',
              }

  EXCH_DICT = {'HK':'HK','C1':'SH','C2':'SZ','US':'US'}
  URL_1 = 'chartdata1.internet.aastocks.com/servlet/iDataServlet/getdaily?id='
  URL_2 = '&type=24&market=1&level=1&period=56&encoding=utf8'
  
  if sec_type == 'index':
    code = INDEX_DICT[code]
    exchange = 'hk' if code[0] == 'h' else 'ch'
  elif len(ticker_ls) > 3:
    print('options')#####################################################################################
    pass
  else:
    exchange = ticker_ls[1]
    exchange = exchange.upper() if exchange.upper() != 'CH' else 'C1' if code[0] == '6' else 'C2'
    code = code + '.' + EXCH_DICT[exchange]

  us_ex = 'us' if exchange == 'US' else ''
  chartdata1_url= 'http://' + us_ex + URL_1 + code + URL_2
  
  #Request Soup
  http = urllib3.PoolManager()
  r = http.request('GET', chartdata1_url)
  soup = BeautifulSoup(r.data, 'lxml')
  soup_text = soup.body.p.text
  
  #Extract Price from Soup
  todayy = datetime.datetime.today()
  #todayy = datetime.datetime.fromisoformat("2021-08-27 10:10:10") #########TESTING############
  todayy = str(todayy.strftime('%m/%d/%Y'))
  price = soup_text.split(todayy)[1].split(';')[4]

  return price


#dates
in_date = datetime.datetime.today()
#in_date = datetime.datetime.fromisoformat("2021-08-27 10:10:10") #########TESTING############
file_format_yymmdd = in_date.strftime('%y%m%d')
loader_format_ddmmmyy = in_date.strftime('%d-%b-%Y')

#Extract tickers from ticker file and store in dataframe
portdf = pd.read_excel(PORT_TICKERS_FILE) #need to leave header in excel
tckr_ls = list(portdf[portdf.columns[0]])

print('Getting prices for ' + loader_format_ddmmmyy) #display

px_dict = {} #stores prices and corresponding ticker
for ticker in tckr_ls:
  try:
    price = aa_price(ticker)   #grabs price from AA Charts
    px_dict[ticker]=price
  except:
    print('skipped '+ ticker)

#TDP Loader Definitions
ldr_dict = {} #loader content
tdp_cred = ['#!CONNECT=HK053_RMO/HK053_RMO@PROD_HO3ORC08_FM.world','#!MAX_ERROR=1000','#!OPF=TDP_LOADER.import_price'] #loader credentials
#file type header chooser
px_hdr = ['#in_ladder_date','in_ident_type','in_ext_ident','in_value_spec','in_price','in_hilo_ind','in_price_ccy','in_notes'] #headers for this type of loader
fx_hdr = []

ldr_hdr = px_hdr #picked which file/header to use
ldr_col = len(ldr_hdr) #counter and defines the width of the file by # of columns
ldr_row = 0 #counter

#TDP Loader Construction
ldr_dict = {}
tdp_hdr = ['#!CONNECT=HK053_RMO/HK053_RMO@PROD_HO3ORC08_FM.world','#!MAX_ERROR=1000','#!OPF=TDP_LOADER.import_price']
px_hdr = ['#in_ladder_date','in_ident_type','in_ext_ident','in_value_spec','in_price','in_hilo_ind','in_price_ccy','in_notes']
ldr_col = len(px_hdr)
ldr_row = 0

for hdr in tdp_hdr:
  ldr_list = [''] * ldr_col
  ldr_list[0] = hdr
  ldr_dict[ldr_row] = ldr_list
  ldr_row += 1
ldr_dict[ldr_row] = ldr_hdr
ldr_row += 1
for i in px_dict:
  ldr_dict[ldr_row] = [loader_format_ddmmmyy,'BB_TCM',i,1,px_dict[i],'','','']
  ldr_row +=1
ldr_df = pd.DataFrame.from_dict(ldr_dict,orient='index')

#Generate PRICE Loader File
filename = 'tdp_loader_PRICE_' + file_format_yymmdd + '.xlsx'
print(filename)
ldr_df.to_excel(filename,index=0,header=False)

