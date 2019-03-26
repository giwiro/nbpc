# Amason dataset

## 1. download_amazon_ds.py

usage: `python download_amazon_ds.py [-h] --out OUT [--threads THREADS]`

example: ` python scripts/download_amazon_ds.py --out ~/Documents/nbpc/datasets/amazon --threads 8  `

In order to use this, the aws credentials must be setted as said in 
[boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration) docs.
There should be 2 files inside `~/.aws`: `credentials` and `config`.

## 2. 