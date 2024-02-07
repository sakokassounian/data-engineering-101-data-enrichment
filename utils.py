#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  5 21:03:13 2024

@author: sako


"""
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
    