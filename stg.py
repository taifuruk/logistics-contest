# %%
import polars as pl

loc = pl.scan_parquet("input/location/*")
vit = pl.scan_parquet("input/vital/*")
# %%
