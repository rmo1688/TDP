
# -*- coding: utf-8 -*-

"""
Stock Prices scraped from AA Stocks and Yahoo Finance

"""

from bs4 import BeautifulSoup
import config
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
FUT_CONT_DICT = {
                  'HI' : ['aa',['221000.HK','221001.HK']],
                  'HC' : ['aa',['221006.HK','221008.HK']],
                  'HCT': ['aa',['221014.HK','221016.HK']],
                  'ES' : ['yh','ES .CME',],
                  'NQ' : ['yh','NQ .CME',],
                  'XU' : ['yh','CN- .SI',],
                  }
FUT_MONTH_DICT = {
            'F':'01', '01':'F',
            'G':'02', '02':'G',
            'H':'03', '03':'H',
            'J':'04', '04':'J',
            'K':'05', '05':'K',
            'M':'06', '06':'M',
            'N':'07', '07':'N',
            'Q':'08', '08':'Q',
            'U':'09', '09':'U',
            'V':'10', '10':'V',
            'X':'11', '11':'X',
            'Z':'12', '12':'Z',
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

def front_fut():
  url = 'https://www.aastocks.com/en/stocks/market/bmpfutures.aspx?future=200000'
  soup = get_soup(url)
  exp = soup.body.find_all('div', class_ = 'float_r cls')[-1].text
  moyr = datetime.datetime.strptime(exp,'%Y/%m/%d').strftime('%m%y')
  return moyr

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
    px_src = FUT_CONT_DICT[code[:-2]][0] # [:-2] removes last 2 characters
    curr_moyr = front_fut() # MMYY format
    front_cont = ''.join([FUT_MONTH_DICT[curr_moyr[:2]],curr_moyr[-1]]) # front month & year eg.Z1,H3
    if px_src == 'yh':
      cont_ls = (FUT_CONT_DICT[code[:-2]][1]).split()
      yr = str(int(yr) + 1) if (yr := curr_moyr[-2:])[-1] == '9' and code[-1] == '0' else yr
      code = ''.join([cont_ls[0], code[-2], yr[-2], code[-1], cont_ls[-1]])
    else:
      cont_mo = 1 if code [-2:] != front_cont else 0
      code = FUT_CONT_DICT[code[:-2]][1][cont_mo]
  elif sec_type == 'OPTION':
    px_src = 'db'
  elif int(code) >= 9999 and int(code) < 30000:
    sec_type = 'WARRANT'
    px_src = 'db'
  else: # EQUITY
    exch = exch if exch != 'CH' else 'C1' if code.startswith('6') else 'C2'
    px_src = EXCH_DICT[exch][0]

  ticker_ls = [code, exch, sec_type] if sec_type != 'OPTION' else ticker.split()
  src_dict = {
            'aa':aa_price,
            'db':db_price,
            'yh':yh_price,
            }
  price = src_dict[px_src](ticker_ls)
  return price

def db_price(ticker_ls):
  URL_1 = 'http://www.dbpower.com.hk/en/option/option-search?otype=ucode&ucode='
  URL_2 = '&hcode=&mdate='
  
  code = ticker_ls[0].zfill(5) #1088
  exp = ticker_ls[2].split('/')
  exp_code = '20' + exp[-1] + '-' + exp[0] #YYYY-MM
  callput = ticker_ls[3][0].upper()
  strike = ticker_ls[3][1:]
  db_url = URL_1 + code + URL_2 + exp_code
  soup = get_soup(db_url)
  option_chain = soup.table.find(text = strike, class_ = 'strike').parent
  if callput == 'P':
    price = option_chain.find_next_sibling('td', class_ = 'live_option_search').text
  else:
    price = option_chain.find('td', class_ = 'live_option_search').text
  price = '0' if price == '-' else price
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
  #print('aa',code,str(price))
  return price

def yh_price(ticker_ls): # For SG/TW/US Equity/Futures/Index
  code = ticker_ls[0]
  sec_type = ticker_ls[-1]
  no_exch_frmt = ((exch := ticker_ls[1])  == 'US') or (sec_type in ['INDEX','FUTURE'])
  code = code if no_exch_frmt else (code := (code + '.' + exch))
  price = round(yf.Ticker(code).history(period='1d').iloc[0, 3],2)
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
    price = price_grab(str(ticker))   #grabs price from AA Charts
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
ldr_row = 0
for hdr in tdp_hdr:
  ldr_list = [''] * len(px_hdr)
  ldr_list[0] = hdr
  ldr_dict[ldr_row] = ldr_list
  ldr_row += 1
ldr_dict[ldr_row] = ldr_hdr
ldr_row += 1
for i in px_dict:
  ldr_dict[ldr_row] = [loader_format_ddmmmyy,'BB_TCM',i,1,px_dict[i],'','','']
  ldr_row +=1
ldr_df = pd.DataFrame.from_dict(ldr_dict,orient='index')

print(filename := 'tdp_loader_PRICE_' + file_format_yymmdd)
ldr_df.to_excel(filename + '.xlsx',index=0,header=False) # save xlsx copy
if os.path.exists(tdp_folder := 'C:\\tdp_loader\\hk053\\input\\'): # generate csv loader in tdp folder
    ldr_df.to_csv(tdp_folder + filename + '.csv',index=0,header=False)

email = config.email
to_email = config.to_email
pascode = config.pascode
subj = 'Prices loader is ready'
message = 'From: %s\r\n' % email + 'To: %s\r\n' % to_email + 'Subject: %s\r\n' % subj + '\r\n' + ''
port = 465
context = ssl.create_default_context()
try:
    with smtplib.SMTP_SSL('smtp.gmail.com', port, context = context) as server:
    	server.login(email, pascode)
    	server.sendmail(email, email, message)
except:
    print('Notification could not be sent.')
