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
from utils import optimize_dataframe



#%% 3. Import data 

# We will store them in a dict 
database = {  }
database['HR']  = optimize_dataframe(pd.read_csv("fixtures/HR.csv"),id_prefix='hr')
database['MEDICAL']  = optimize_dataframe(pd.read_csv("fixtures/medical.csv"),id_prefix='med')
database['PE_EXAM']  = optimize_dataframe(pd.read_csv("fixtures/field_exam.csv"),id_prefix='fe')
database['THEO_EXAM']  =optimize_dataframe( pd.read_csv("fixtures/theory_exam.csv"),id_prefix='te')


df_hr = database['HR']['db'].copy()
df_med = database['MEDICAL']['db'].copy()
df_pe= database['PE_EXAM']['db']
df_theo= database['THEO_EXAM']['db']




#%% 4. Test 1: Merging with no preprocessing and enrichment

# merge student
"""The theo student table is complete and we merge it with the practical"""
df_report = pd.merge(df_theo,df_pe,on='student_id',how='left')

df_report.info()



# merge student 
"""The medical table can have passport ids we don't know about. Plus we have no access to it"""
df_report = pd.merge(df_report,df_med,on='passport_id',how='outer')

df_report.info()

"""
missing data, x and ys, and a total mess
"""


#%% 5.  Test 2: Merge with preprocessing

# merge student
"""The theo student table is complete and we merge it with the practical"""
df_report = pd.merge(df_theo,df_pe,on='student_id',how='outer',indicator=True,suffixes=('_theo','_pe'))

print(df_report.info(),"\n")
print(df_report._merge.value_counts(),"\n")

"""We have arround 18 student who do not have values which we need to populate."""

"""We can see that we have 98 unique theoretical IDs and that the physical education are 95"""





#%% 6.  Test 3: Merge with preprocessing and enrichment 












