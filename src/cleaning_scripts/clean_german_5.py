import pandas as pd
import numpy as np

def clean_german_5(filepath):
    #ver colab
    #https://colab.research.google.com/drive/1BW2bkIdiRn8jV9ROUzF5-wWUdKmdOKcf#scrollTo=xYLh0RtGTo3G


    df = pd.read_csv(filepath)

    # --------------------------- 1. Estandarización de Texto ---------------------------
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = (df[column]
                          .str.lower()
                          .str.strip()
                          .str.replace(r'\s+', ' ', regex=True))
            


    # --------------------------- 2. Mapeos para validaciones ---------------------------
    df = df[df['language'] == 'de']
    
    expect_priority = ['medium', 'high', 'very_low', 'critical', 'low']
    df = df[df['priority'].isin(expect_priority)]



    # --------------------------- 3. Transformación ---------------------------
    # Convertir código 'de' a 'german' para consistencia con el dataset multi-lenguaje
    df['language'] = df['language'].replace({'de': 'german'})


    priority_map = {
        'very_low': 1,
        'low': 2,
        'medium': 3,
        'high': 4,
        'critical': 5
    }
    df['priority_level'] = df['priority'].map(priority_map)


    df['queue'] = df['queue'].fillna('').astype(str)

    split_data = df['queue'].str.split('/', n=1)
    df['sector'] = split_data.str[0].str.strip()
    df['specific_sector'] = split_data.str[1].astype(str).str.strip()
    df['specific_sector'] = df['specific_sector'].fillna("not provided")



    #-------------------------- 4. Reorden -------------------------------
    column_order = [
        'subject',
        'body',
        'queue',
        'sector',
        'specific_sector',
        'priority',
        'priority_level',
        'language'
    ]
    df = df[column_order]


    #imp final
    df['dataset_source'] = 'german_5k'

    return df