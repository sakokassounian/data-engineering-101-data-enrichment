#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  5 21:03:13 2024

@author: sako


"""
import re
import pandas as pd
from dateutil import parser as date_parser
import re


def text_cleaner(val_str, 
                        pre_replacements = None,
                        digit_replacement = True,
                        character_replacer="_"):
    
    """
    This function will clean the strings and replace them with a standardized
    form.  We will remove punctuation. Large spaces replaced witha a single underscore. 
    Digits will be replace other resembling letters. Finally everything will be lower cased. 

    Parameters
    ----------
    val_str : string
        The string to be cleaned
    custom_replacements: string
        This is usually a list of string you want to replace with an "_" before removing them. 
        
       

    Returns
    -------
    val_str_clean: list of string.
        The final cleaned string
    

    """
    val_str_clean = val_str
    
    if pre_replacements:
        val_str_clean =  val_str_clean.translate({ord(i): character_replacer for i in pre_replacements})

    
    # I got this from yje package "string". The underscore was removed
    punct = '!"#$%&\'()*+,-–./:;<=>?@[\\]^`{|}~_'.replace(character_replacer,"")
    digits = '0123456789'
    
    # Remove all the special characters and the spaces at the begining and end
    val_str_clean = val_str_clean.translate({ord(i): "" for i in punct}).lstrip().rstrip().lower()
    
    
    # Replace spaces with "_"
    val_str_clean = re.sub("\s+",character_replacer,val_str_clean)
    
    # replace digits with special characters
    morphological_letters = {
    '0': 'O',
    '1': 'I',
    '2': 'Z',
    '3': 'E',
    '4': 'A',
    '5': 'S',
    '6': 'G',
    '7': 'T',
    '8': 'B',
    '9': 'g'}
    
    if digit_replacement:        
        # Remove all the special characters and the spaces at the begining and end
        val_str_clean = val_str_clean.translate({ord(i): morphological_letters[i] for i in digits})
    
    return val_str_clean 
    

def format_memory_size(memory_bytes):
    if memory_bytes < 1024:
        return f"{memory_bytes} B"
    elif memory_bytes < 1024**2:
        return f"{memory_bytes / 1024:.2f} KB"
    elif memory_bytes < 1024**3:
        return f"{memory_bytes / (1024**2):.2f} MB"
    else:
        return f"{memory_bytes / (1024**3):.2f} GB"



def optimize_dataframe(df, 
                        pre_clean_str='.',
                        date_delimter='-',
                        id_prefix="",
                        id_suffix=""):
    """
    This function cleans a database to it's optimal datatype and tries to
    parse dates. You can retrun the metadata or simply the clean dataframe. 

    Parameters
    ----------
    df : Pandas DataFrame
        The dataframe to be cleaned.
    pre_clean_str : str, optional
        A string that contains different character. If you want to do a pre-clean
        to replace specific characters with and "_". The process of the cleaning
        is based on removing all the special characters. 
        Example: "NAME.first" becomes "namefirst", but if you keep the value
        to ".", it becomes "name_first". The default is '.'.

    date_delimter : str, optional
        This value is replaced with the special characters if the columns types
        is detected to be a date. This improves the date parsing. The default is '-'.
    id_prefix : str, optional
        Add a prefix to the ID column values. The default is "".
    id_suffix : TYPE, optional
        Add a suffix to the ID column valies. The default is "".

    Returns
    -------
    dict
        Contains the database and its metadata.
    """

    
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
    
    
            
    # Rename database: As the renaming is the last step duplicate columns will not cause issues.
    df_use = df_use.rename({i:j for i,j in zip(database_meta["col_name"],database_meta["clean_col_name"])},axis=1)        
    
    
    # Add unique id
    if id_prefix: id_prefix = id_prefix+"_"
    if id_suffix: id_prefix = "_"+id_suffix
    
    
    max_num = df_use.shape[0]
    max_digit_size = len(str(max_num))
    number_list = [str(num).zfill(max_digit_size) for num in range(1, max_num+1)]
    df_use['db_ID'] = [f"{id_prefix}{i}{id_suffix}" for i in number_list]
        
    

    return  {"db":df_use, "metadata": database_meta} 
