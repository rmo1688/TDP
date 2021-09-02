
# -*- coding: utf-8 -*-

"""
Stock Prices scraped from AA Stocks and Yahoo Finance

"""

from bs4 import BeautifulSoup
import datetime
import lxml
import openpyxl
import requests
import time
import urllib3
import pandas as pd
import yfinance as yf


PORT_TICKERS_FILE = 'port_tickers.xlsx' #need to leave header in excel
INDEX_DICT = {
            'HSI'    : ['aa','110000.HK'],
            'HSCEI'  : ['aa','110010.HK'],
            'HSTECH' : ['aa','110078.HK'],
            'SHSZ300': ['aa','000300.SH'],
            'XIN9I'  : ['yh','XIN9.FGI' ],
            'SPX'    : ['yh','^SPX'     ],
            'NDX'    : ['yh','^NDX'     ],
            }
FUTURES_DICT = {
            'HI' : ['aa', ['221000.HK','221001.HK']],
            'HC' : ['aa', ['221006.HK','221008.HK']],
            'HCT': ['aa', ['221014.HK','221016.HK']],
            'ES' : ['yh','ESU21.CME'],
            'NQ' : ['yh','NQU21.CME'],
            'XU' : ['yh','CN-U21.SI'],
            }
FUT_CONT_DICT = {
            'G':'01',
            'H':'02',
            'J':'03',
            'K':'04',
            'M':'05',
            'N':'06',
            'P':'07',
            'Q':'08',
            'U':'09',
            'V':'10',
            'X':'11',
            'Z':'12',
            }
EXCH_DICT = { # exchange : [price source, exchange code in source]
            'HK': ['aa', 'HK'],
            'C1': ['aa', 'SH'],
            'C2': ['aa', 'SZ'],
            'TT': ['yh', 'TW'],
            'US': ['yh',  '' ],
            }


def get_soup(url):
  http = urllib3.PoolManager()
  r = http.request('GET', url)
  soup = BeautifulSoup(r.data, 'lxml')
  return soup

def price_grab(ticker): # Selects price source and converts Bloomberg ticker to source format
  ticker_ls = ticker.split()
  code = ticker_ls[0].upper()
  sec_type = ticker_ls[-1].upper() if len(ticker_ls) <= 3 else 'OPTION'
  sec_type = sec_type if sec_type != 'INDEX' else 'FUTURE' if code not in INDEX_DICT else 'INDEX'
  exch = '' if sec_type != 'EQUITY' else ticker_ls[1].upper()

  if sec_type == 'INDEX':
    px_src = INDEX_DICT[code][0]
    code = INDEX_DICT[code][1]
  elif sec_type == 'FUTURE': 
    px_src = FUTURES_DICT[code[:-2]][0]
    mo = datetime.datetime.today().strftime('%m')
    yr = datetime.datetime.today().strftime('%y')
    cont_mo = 0 ##################################################set contract month/year here 
    code = FUTURES_DICT[code[:-2]][1][cont_mo]
  elif sec_type == 'OPTION':
    sec_type = 'OPTION' # HK Options use AA stocks, Other manual? US Index options use Yahoo?
  else: # EQUITY
    exch = exch if exch != 'CH' else 'C1' if code.startswith('6') else 'C2'
    px_src = EXCH_DICT[exch][0] 

  ticker_ls = [code, exch, sec_type]
  src_dict = {
            'aa':aa_price, 
            'yh':yh_price,
            }           
  price = src_dict[px_src](ticker_ls)
  return price

def yh_price(ticker_ls): # For SG/TW/US Equity/Futures/Index
  code = ticker_ls[0]
  sec_type = ticker_ls[-1]
  no_exch_frmt = ((exch := ticker_ls[1])  == 'US') or (sec_type == 'INDEX')
  code = code if no_exch_frmt else (code := code + '.' + exch)
  print('yh ',code)
  price = round(yf.Ticker(code).history(period='1d').iloc[0, 3],2)
  print(code,str(price))
  return price

def aa_price(ticker_ls): # For HK/CH Equity/Futures/Index
  URL_1 = 'chartdata1.internet.aastocks.com/servlet/iDataServlet/getdaily?id='
  URL_2 = '&type=24&market=1&level=1&period=56&encoding=utf8'

  code = ticker_ls[0]
  exch = EXCH_DICT[ticker_ls[1]][1] if (exch := ticker_ls[1]) != '' else exch
  sec_type = ticker_ls[-1]
  if sec_type == 'EQUITY':
    code = code.zfill(5) + '.' + exch

  is_us = '' if exch != 'US' else exch
  chartdata1_url = 'https://' + is_us + URL_1 + code + URL_2
  soup = get_soup(chartdata1_url)
  soup_text = soup.body.p.text
  todayy = datetime.datetime.today().strftime('%m/%d/%Y')
  price = round(float(soup_text.split(todayy)[1].split(';')[4]),2)  #Extract Price from Soup
  print(code,sec_type,str(price))
  return price


portdf = pd.read_excel(PORT_TICKERS_FILE) # Extract tickers from ticker file
tckr_ls = list(portdf[portdf.columns[0]]) # Store extracted tickers in dataframe

in_date = datetime.datetime.today()
# in_date = datetime.datetime.fromisoformat("2021-09-01 10:10:10") ###### FOR TESTING ######
file_format_yymmdd = in_date.strftime('%y%m%d')
loader_format_ddmmmyy = in_date.strftime('%d-%b-%Y')

px_dict = {} # stores prices and corresponding ticker
skipped_tickers = {} # stores tickers of skipped tickers

print('Getting prices for ' + loader_format_ddmmmyy) #display

for ticker in tckr_ls:
  try:
    price = price_grab(ticker)   #grabs price from AA Charts
    px_dict[ticker] = price
  except:
    print('skipped '+ ticker)
    skipped_tickers[ticker] = ''

#TDP Loader Definitions
ldr_dict = {} # for loader content
tdp_cred = [ #loader credentials
            '#!CONNECT=HK053_RMO/HK053_RMO@PROD_HO3ORC08_FM.world',
            '#!MAX_ERROR=1000',
            '#!OPF=TDP_LOADER.import_price'
            ]
#file type header chooser
px_hdr = [ #headers for this type of loader
          '#in_ladder_date',
          'in_ident_type',
          'in_ext_ident',
          'in_value_spec',
          'in_price',
          'in_hilo_ind',
          'in_price_ccy',
          'in_notes',
          ] 
fx_hdr = []
delta_hdr = []

ldr_hdr = px_hdr #picked which file/header to use
ldr_col = len(ldr_hdr) #counter and defines the width of the file by # of columns
ldr_row = 0 #counter

#TDP Loader Construction
ldr_dict = {}
tdp_hdr = [
            '#!CONNECT=HK053_RMO/HK053_RMO@PROD_HO3ORC08_FM.world',
            '#!MAX_ERROR=1000',
            '#!OPF=TDP_LOADER.import_price',
          ]
px_hdr = [
            '#in_ladder_date',
            'in_ident_type',
            'in_ext_ident',
            'in_value_spec',
            'in_price',
            'in_hilo_ind',
            'in_price_ccy',
            'in_notes'
            ]
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

