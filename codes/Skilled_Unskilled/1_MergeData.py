#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 25 2026

@author: valentinavasquez
"""

import pandas as pd
import numpy as  np

''' Column Names '''

afiliados_names = [
    'ID_Person', 'Gender', 'Birthdate', 'Educ_Level', 
    'Total_Years_Approved', 'Civil_Status', 'Comuna', 
    'AFP', 'Nationality'
]

cuentas_names = [
    'ID_Person', 'Affiliation_Type', 
    'Affiliation_Place', 'Affiliation_Date', 'Status_Code', 
    'Affiliation_Modality', 'Balance_Installments', 'Cero_Balance'
]

rentas_names = [ 'ID_Person', 'Wage_Date', 'Contract_Tyoe', 'Subs_Laboral_Inhability', 
                'Economic_Activity', 'Employer_Comuna', 'Taxable_Income', 
                'Number_of_Workers', 'Average_Employee_Income', 'Std_Employee_Income',
                'Cero_Taxable_Income_Indicator', 'Cap_Taxable_Income_Indicator', 
                'Min_Income_Idicator', 'ID_Employer']


''' READ CSV AND MERGE ALL AVAILABLE DATA '''

all_data_list = []
all_giros = []

for i in [3, 5, 12]:
    # Read CSV files
    afiliados = pd.read_csv(f'/Users/valentinavasquez/Documents/GitHub/HANK_Quant/HANK_Quant/bases/muestraasc{i}%/1_afiliados.csv', 
                            sep=';', header=None, names=afiliados_names)
    cuentas = pd.read_csv(f'/Users/valentinavasquez/Documents/GitHub/HANK_Quant/HANK_Quant/bases/muestraasc{i}%/2_cuentas.csv', 
                          sep=';', header=None, names=cuentas_names)
    rentas = pd.read_csv(f'/Users/valentinavasquez/Documents/GitHub/HANK_Quant/HANK_Quant/bases/muestraasc{i}%/5_rentas_imponibles.csv', 
                         sep=';', header=None, names=rentas_names)
   
    # Merge DataFrames
    merged_afil_cuentas = pd.merge(afiliados, cuentas, on='ID_Person', how='inner')
    all_data = pd.merge(merged_afil_cuentas, rentas, on='ID_Person', how='inner')
    
    # Aggregate dataframe to the list
    all_data_list.append(all_data)

# combine dataframes
Data  = pd.concat(all_data_list, ignore_index=True)

Data.to_csv('/Users/valentinavasquez/Documents/GitHub/HANK_Quant/HANK_Quant/bases/Data_20_percent.csv', index=False)


