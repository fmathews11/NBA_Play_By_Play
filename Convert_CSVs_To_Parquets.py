import pandas as pd
import os
from tqdm import tqdm


if __name__  == "__main__":
    
    csv_file_names = [i.name for i in os.scandir('data/csvs') if i.name.endswith('.csv')]

    for csv_file_name in tqdm(csv_file_names):

        output_parquet_file_name = csv_file_name.replace(".csv",".gzip")
        csv_df = pd.read_csv(f"data/csvs/{csv_file_name}")
        csv_df.to_parquet(f'data/parquets/{output_parquet_file_name}',compression='gzip')