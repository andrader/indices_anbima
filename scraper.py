#!/usr/bin/env python
# coding: utf-8
import requests
import pandas as pd
import numpy as np
import io
from time import sleep
from datetime import date, datetime
import datatest
import sqlite3
from tqdm import tqdm
import random

from helpers import clean_names, dtf

# to generate requirements.txt:
# > pip install pipreqs
# > pipreqs .

url = 'https://www.anbima.com.br/informacoes/ima/ima-sh-down.asp'

params = {
    "Idioma": "PT",
    "Dt_Ref": "20/03/2021",
    "DataIni": "06/01/2020",
    "DataFim": "20/03/2021",
    "Indice": "quadro-resumo",
    "Consulta": "Ambos",
    "saida": "csv"
}

valid_dtypes = {
    'indice': 'O',
    'data_referencia': 'datetime64[ns]',
    'numero_indice': 'float64',
    'variacao_diaria': 'float64',
    'variacao_mes': 'float64',
    'variacao_ano': 'float64',
    'variacao_12_meses': 'float64',
    'variacao_24_meses': 'float64',
    'peso': 'float64',
    'duration': 'O',
    'carteira_a_mercado': 'O',
    'numero_operacoes': 'float64',
    'quant_negociada_titulos': 'float64',
    'valor_negociado': 'float64',
    'pmr': 'O',
    'convexidade': 'float64',
    'yield': 'float64',
    'redemption_yield': 'float64'
}

nomes_validos = list(valid_dtypes.keys())

# lista de user agents
uas = pd.read_table('input/user-agents.txt',names=['ua'],skiprows=4,squeeze=True)
# lista de feriados anbima
fer = pd.read_excel('input/feriados_nacionais.xls',skipfooter=9, usecols=['Data'], parse_dates=['Data'], squeeze=True)
bday = pd.offsets.CDay(holidays=fer)



def get_indices_anbima(dt, wait=True):
    """
    dt: str '%d/%m/%Y' ou dt obj
    """
    if wait:
        if isinstance(wait,bool): wait = random.randint(1,3)
        sleep(wait)
    
    headers = {"User-Agent": np.random.choice(uas)}
    params["Dt_Ref"] = params["DataIni"] = params["DataFim"] = dt.strftime("%d/%m/%Y")
    r = requests.get(url, params=params, stream=True, headers=headers)
    r.raise_for_status()

    try:
        df = pd.read_csv(io.StringIO(r.text),
                         sep=";",decimal=",",thousands=".",na_values="--",
                         skiprows=1,parse_dates=["Data de Referência"],dayfirst=True,)
        assert df.shape[0] > 0, "0 linhas. "
    except pd.errors.EmptyDataError as e:
        print(dt, e, r.text, sep='\n')
        df = pd.DataFrame(columns=nomes_validos)

    # trata col_names e dtypes
    to_remove = ["<BR>","1.000","R$ mil"," de "," no ","d.u.","%","(",")","*",".",]
    df = df.set_axis(clean_names(df.columns, to_remove), axis=1).astype(valid_dtypes)

    # validacao
    datatest.validate(df.columns, nomes_validos)
    datatest.validate(df.dtypes, valid_dtypes)
    
    return df


def get_max_dt_db(db_table_name, db_name='data.sqlite', default_dt='2001-12-03'):
    # tenta pegar data mais antiga na base
    try:
        with sqlite3.connect(db_name) as conn:
            dt_max = pd.read_sql_query(f"select max(data_referencia) data_referencia from {db_table_name}",
                                       conn, parse_dates=['data_referencia']).squeeze()
    except Exception as e:
        print(e, 'getting min available date')
        dt_max = pd.Timestamp(default_dt).normalize()
    return dt_max



def scrape_indices_to(db_table_name, db_name='data.sqlite'):
    print('Starting...')
    # generate date series
    dt_start = get_max_dt_db(db_table_name)
    dt_end = pd.Timestamp.today().normalize() - bday
    dates = pd.bdate_range(dt_start+ bday, dt_end, freq="C", holidays=fer).to_series()
    print( dtf(dt_start), dtf(dt_end))
    
    if len(dates)==0:
        print('Already update!')
        return

    for month, days in tqdm(dates.groupby(pd.Grouper(freq='MS')),unit='mês',
                            desc=f'De {dtf(dt_start)} até {dtf(dt_end)}. Meses'):
        # progress bar
        pbar = tqdm(days,leave=False,unit='day', desc=f'Scraping {month.strftime("%Y-%m")}', position=0)
        # scrape bdays in month
        df = pd.concat((get_indices_anbima(dt,wait=0.5) for dt in pbar), ignore_index=True)
        
        # add df to db
        print('\nSaving to table')
        with sqlite3.connect(db_name) as conn:
            df.to_sql(db_table_name, conn, if_exists="append", index=False)
    print('\nSuccess!')
    return


if __name__=='__main__':
    
    scrape_indices_to('data', "data.sqlite")
    print('\nEnd.')