#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  5 20:27:41 2024

@author: sako
"""

#%% 1. Import Needed packages and functions 

import os
os.chdir("/home/sako/Data science lessons/My  projects/clean_data")

import pandas as pd 
from utils import text_cleaner
from dateutil import parser as date_parser







#%% 



def optimize_dataframe(df, return_metadata=False, 
                            pre_clean_str='.',
                            date_delimter='-',
                            id_prefix="",
                            id_suffix=""):

    
    convertion_types = ['bytes',
                     'floating',
                     'integer',
                     'mixed-integer',
                     'mixed-integer-float',
                     'decimal',
                     'complex',
                     'categorical',
                     'boolean',
                     'datetime64',
                     'datetime',
                     'date',
                     'timedelta64',
                     'timedelta',
                     'time',
                     'period',
                     'mixed']
    

    df_use = df.copy().convert_dtypes()
    
    
    # Now we will analyze the data     
    database_meta = {'col_name':[],'clean_col_name':[],"col_type":[]} 
    for col_name,col_type in df_use.dtypes.astype(str).to_dict().items():
        
        optimal_type = col_type
        
        if optimal_type =='string':
            
            # Maybe it's mixed of numbers and strings 
            col_temp = pd.Series(df_use[col_name].unique())
            col_temp = col_temp.apply(pd.to_numeric,errors='coerce').fillna(col_temp)
            
            # Extract after convertig numerical
            analysis_type = pd.api.types.infer_dtype(col_temp)
            
            # This activates if the type changes
            if analysis_type in convertion_types: 
                optimal_type = analysis_type
                
            else: 
                try:
                    # Let's parse dates
                    df_use[col_name].dropna().sample(10).\
                            apply(lambda x : text_cleaner(x,
                                                          digit_replacement=False,     
                                                          character_replacer=date_delimter)).apply(date_parser.parse)   
                    optimal_type = 'datetime'
                    
                except: 
                    pass
        
        # store the information         
        database_meta['col_type'].append(optimal_type)
        database_meta["col_name"].append(col_name)
        database_meta["clean_col_name"].append(text_cleaner(col_name,pre_replacements=pre_clean_str))
    
    # now we will convert the datetypes into date format    
    """If there is format error, we'll be able to detect it here"""
    for col_name,col_type in zip(database_meta['col_name'],database_meta['col_type']):
        if col_type =='datetime':
            df_use[col_name] = df_use[col_name].apply(lambda x : text_cleaner(x,
                                                    digit_replacement=False,     
                                                    character_replacer=date_delimter)).apply(date_parser.parse)  
    
    #TODO: maybe the names are not unique... We might need to fix that       
            
    # Rename database
    df_use = df_use.rename({i:j for i,j in zip(database_meta["col_name"],database_meta["clean_col_name"])},axis=1)        
    
    
    # Add unique id
    if id_prefix: id_prefix = id_prefix+"_"
    if id_suffix: id_prefix = "_"+id_suffix
    
    
    max_num = df_use.shape[0]
    max_digit_size = len(str(max_num))
    number_list = [str(num).zfill(max_digit_size) for num in range(1, max_num+1)]
    df_use['db_ID'] = [f"{id_prefix}{i}{id_suffix}" for i in number_list]
        
    
    if return_metadata: 
        return  {"db":df_use, "metadata": database_meta} 

    else:
        return df_use    



#%% 2. Import data 

# We will store them in a dict 
database = {  }
database['MAIN']  = optimize_dataframe(pd.read_csv("fixtures/main.csv"),return_metadata=True,id_prefix='MAIN')

database['HR']  = optimize_dataframe(pd.read_csv("fixtures/HR.csv"),return_metadata=True)

database['MEDICAL']  = optimize_dataframe(pd.read_csv("fixtures/medical.csv"),return_metadata=True)

database['FIELD_EXAM']  = optimize_dataframe(pd.read_csv("fixtures/field_exam.csv"),return_metadata=True)

database['THEORY_EXAM']  =optimize_dataframe( pd.read_csv("fixtures/theory_exam.csv"),return_metadata=True)
