# %%
import requests
import zipfile
import polars as pl
import io
import pandas as pd
import contextlib
from typing import Optional
import joblib
from tqdm.auto import tqdm
import os

@contextlib.contextmanager
def tqdm_joblib(total: Optional[int] = None, **kwargs):

    pbar = tqdm(total=total, miniters=1, smoothing=0, **kwargs)

    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            pbar.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback

    try:
        yield pbar
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        pbar.close()
# %%
token = os.environ.get('TOKEN')


def read_csv__from_zip(url):
    _zip = zipfile.ZipFile(io.BytesIO(requests.get(url).content))
    return pl.read_csv(_zip.read(_zip.namelist()[0]))


class get_data:
    def __init__(self, token:str, date:str) -> None:
        self.token = token
        self.date = date

    def location(self) -> pl.DataFrame:
        location_url = f"https://api.daiwa-open-challenge.jp/files/sensors_map/sensors_map_{self.date}.zip?acl:consumerKey={self.token}"
        df = read_csv__from_zip(location_url)
        return df

    def vital(self) -> pl.DataFrame:
        vital_url = f"https://api.daiwa-open-challenge.jp/files/driving_vitals/driving_vitals_{self.date}.zip?acl:consumerKey={token}"
        df = read_csv__from_zip(vital_url)
        return df

    def vehicle_master(self) -> pl.DataFrame:
        vital_url = f"https://api.daiwa-open-challenge.jp/files/masters/master_cars_202301.zip?acl:consumerKey={token}"
        df = read_csv__from_zip(vital_url)
        return df

    def user_master(self) -> pl.DataFrame:
        vital_url = f"hhttps://api.daiwa-open-challenge.jp/files/masters/master_users_202301.zip?acl:consumerKey={token}"
        df = read_csv__from_zip(vital_url)
        return df
# %%
def save_logs(token, date):
    data = get_data(token, date)
    try:
        vital = data.vital()
        vital.write_parquet(f"./input/vital/{date}.parquet")
        location = data.location()
        location = location.filter(pl.col("user_id").is_in(vital["user_id"].unique().to_list()))
        location.write_parquet(f"./input/location/{date}.parquet")

    except:
        pass
# %%
target = pd.date_range("2022-07-19", "2022-12-31").strftime("%Y%m%d").to_list()
# %%
with tqdm_joblib(len(target)):
    joblib.Parallel(n_jobs=-1)(joblib.delayed(save_logs)(token, i) for i in target)
# %%
