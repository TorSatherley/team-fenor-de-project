import pandas as pd
import numpy as np

jsonl_file = 'counterparty'


with open(f'data/json_lines_s3_format/{jsonl_file}.jsonl') as fin:
    df = pd.json_normalize(pd.DataFrame(fin.read().splitlines())[0].apply(eval))


# Sometimes, there's some encoding issues, so this is done:
for col in df.columns:
    if df[col].dtype == object:
        df[col] = df[col].apply(lambda x: np.nan if x==np.nan else str(x).encode('utf8', 'replace').decode('utf8'))

df.to_parquet(f'data/json_lines_s3_format/parquet_files/{jsonl_file}.parquet')