import pandas as pd

def clean_german(filepath):
    #ver colab
    #https://colab.research.google.com/drive/14y_iLSFjMrDzH_oxh4kgOpQZn9bspTym#scrollTo=-cBvJy2b-rLS

    df = pd.read_csv(filepath)

    # --------------------------- 1. Estandarización de Texto ---------------------------
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = (df[column]
                          .str.lower()
                          .str.strip()
                          .str.replace(r'\s+', ' ', regex=True))
            

    # --------------------------- 2. Validacion ---------------------------
    df = df[df['language'] == 'de']    # el cambio a german esta abajo

    expect_priority = ['high', 'medium', 'low']
    df = df[df['priority'].isin(expect_priority)]         



    # --------------------------- 3. Transformación ---------------------------
    df['language'] = df['language'].replace({'de': 'german'})

    priority_map = {'low': 2, 'medium': 3, 'high': 4}
    df['priority_level'] = df['priority'].map(priority_map)


    df['queue'] = df['queue'].fillna('').astype(str)
    
    split_data = df['queue'].str.split('/', n=1)
    df['sector'] = split_data.str[0].str.strip()
    df['specific_sector'] = split_data.str[1].astype(str).str.strip()
    df['specific_sector'] = df['specific_sector'].fillna("not provided")




    # --------------------------- 4. Reorden ---------------------------
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
    
    
    # Identificador de origen
    df['dataset_source'] = 'german'


    return df