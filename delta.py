# -*- coding: utf-8 -*-














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
ldr_dict = {} # for loader content to be converted into pandas dataframe
row = 0 # row counter in loader
for line in TDP_CRED_LS: # this loop sets up the loader credentials into the dictionary to be converted into pandas df
  ldr_list = [''] * len(TDP_HEADERS_DICT['px']) # sets number of blank cells and file width
  ldr_list[0] = line # inserts tdp credentials into first column of row
  ldr_dict[row] = ldr_list # add row to dataframe dict
  row += 1
ldr_dict[row] = TDP_HEADERS_DICT['px']
row += 1
for i in data_dict:
  ldr_dict[row] = [loader_format_ddmmmyy,'BB_TCM',i,1,data_dict[i],'','','']
  row +=1
ldr_df = pd.DataFrame.from_dict(ldr_dict,orient='index')

print(filename := 'tdp_loader_PRICE_' + file_format_yymmdd)
ldr_df.to_excel(filename + '.xlsx',index=0,header=False) # save xlsx copy
if os.path.exists(tdp_folder := 'C:\\tdp_loader\\hk053\\input\\'): # generate csv loader in tdp folder
    ldr_df.to_csv(tdp_folder + filename + '.csv',index=0,header=False)