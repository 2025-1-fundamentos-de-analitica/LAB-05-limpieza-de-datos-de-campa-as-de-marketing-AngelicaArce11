"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import zipfile
import glob
import fnmatch
import os
import pandas as pd

def client_cvs(df):
    # Hacemos una copia 
    df = df.copy()
    # Nos quedamos con las columnas que nos interesan 
    df = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']]

    # job: se debe cambiar el "." por "" y el "-" por "_"
    df['job'] = df['job'].str.replace('-', '_')
    df['job'] = df['job'].str.replace('.', '')

    # education: se debe cambiar "." por "_" y "unknown" por pd.NA
    df['education'] = df['education'].replace('unknown', pd.NA)
    df['education'] = df['education'].str.replace('.', '_')

    # credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    df['credit_default'] = df['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)

    # mortgage: convertir a "yes" a 1 y cualquier otro valor a 0
    df['mortgage'] = df['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)

    return df

def campaing_cvs(df):
    # Hacemos una copia 
    df = df.copy()
    # Nos quedamos con las columnas que nos interesan 
    df = df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'month', 'day']]

    # - previous_outcome: cambiar "success" por 1, y cualquier otro valor a 0
    df['previous_outcome'] = df['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)

    # - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    df['campaign_outcome'] = df['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)

    # - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
    #     combinando los campos "day" y "month" con el año 2022.

    #  Convertimos el mes a numero
    df['month'] = pd.to_datetime(df['month'], format='%b').dt.month
    df['last_contact_date'] = pd.to_datetime({
        'year': 2022,
        'month': df['month'],
        'day': df['day']
    })

    return df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'last_contact_date']]

def economics_cvs(df):
    # Hacemos una copia 
    df = df.copy()
    # Nos quedamos con las columnas que nos interesan 
    df = df[['client_id', 'cons_price_idx', 'euribor_three_months']]

    return df


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerles un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    # Dataframe para cada archivo
    dataframeClient = pd.DataFrame()
    dataframeCampaing = pd.DataFrame()
    dataframeEconomics = pd.DataFrame()
    
    # Obtenemos los nombres de los archivos de una ruta especifica
    files = glob.glob(f"files/input/*")

    # Leemos los archivos zip
    for file in files:
        with zipfile.ZipFile(file, 'r') as zip:
            # Obtenemos todos los nombres de los archivos dentro del zip
            csvs = zip.namelist()
        
            # Buscamos todos los .csv dentro del zip
            csvs = fnmatch.filter(csvs, '*.csv')
            
            # Leemos cada archivo csv
            for csv in csvs:
                with zip.open(csv) as f:
                    # Creamos un dataframe con el archivo
                    df = pd.read_csv(f)

                    # Procesamos el cvs de acuerdo con lo que se necesita en cada archivo de salida
                    dfClient = client_cvs(df)
                    dfCampaing = campaing_cvs(df)
                    dfEconomics = economics_cvs(df)

                    # Concatenamos el df resultante con el df final de cada archivo de salida
                    dataframeClient =  pd.concat([dataframeClient, dfClient])
                    dataframeCampaing = pd.concat([dataframeCampaing, dfCampaing])
                    dataframeEconomics = pd.concat([dataframeEconomics, dfEconomics])
                    
    # Creamos la carpeta de output
    os.makedirs('files/output', exist_ok=True)


    dataframeClient.to_csv('files/output/client.csv', columns=['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage'], index=False, encoding='utf-8')
    dataframeCampaing.to_csv('files/output/campaign.csv', columns=['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome', 'last_contact_date'], index=False, encoding='utf-8')
    dataframeEconomics.to_csv('files/output/economics.csv', columns=['client_id', 'cons_price_idx', 'euribor_three_months'], index=False, encoding='utf-8')


if __name__ == "__main__":
    clean_campaign_data()
