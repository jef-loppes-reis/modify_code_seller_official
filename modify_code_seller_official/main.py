import pandas as pd
from pecista import Postgres, MLInterface
from requests import put
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from itertools import repeat
from tqdm import tqdm


def get_tables_postgres() -> tuple[pd.DataFrame]:
    with Postgres() as db:
        df_ml_info = db.query('SELECT * FROM "ECOMM".ml1_info')
        df_produto = db.query('SELECT codpro, num_fab, fantasia FROM "D-1".produto')
    return df_ml_info, df_produto

def created_df_ml_brand(df_ml_info:pd.DataFrame, df_produto:pd.DataFrame) -> pd.DataFrame:
    return pd.merge(df_ml_info, df_produto,
                    on='codpro', how='left'
                    )

def modify_code_seller_official(item_id:str, code_seller_official:int, token:str):
    url = f'https://api.mercadolibre.com/items/{item_id}'
    headers = {
        'Authorization': f'Bearer {token}'
        }
    payload = {"official_store_id": code_seller_official}
    response = put(url=url, headers=headers, json=payload)
    return response.text, response.status_code

def iteration(item_id:str, code_seller_official:int, token:str) -> dict:
    response_text, response_status_code = modify_code_seller_official(item_id, code_seller_official, token)
    if response_status_code == 400:
        response_text, response_status_code = modify_code_seller_official(item_id, code_seller_official, token)
    while (response_status_code == 429) or (response_status_code == 500):
        sleep(10)
        response_text, response_status_code = modify_code_seller_official(item_id, code_seller_official, token)
    return {'item_id':item_id, 'response_text':response_text, 'response_status_code':response_status_code}

def main(brand:str, code_seller_official:int) -> None:
    access_token = MLInterface()._get_token(1)
    df_ml_info, df_produto = get_tables_postgres()
    df_merge_products_brand = created_df_ml_brand(df_ml_info,df_produto)
    df_merge_products_brand = df_merge_products_brand.query(f'fantasia == "{brand}"').reset_index(drop=True)
    df_aux = pd.DataFrame(columns=['item_id','response_text','response_status_code'])
    with ThreadPoolExecutor() as task:
        for future in tqdm(task.map(iteration, df_merge_products_brand.item_id, repeat(code_seller_official), repeat(access_token)), total=len(df_merge_products_brand)):
            df_aux.loc[len(df_aux)] = future
    return df_aux

if __name__ == '__main__':
    df_aux = main('TRW',2469)
