# https://databricks.com/blog/2020/01/27/time-series-forecasting-prophet-spark.html
# https://www.kaggle.com/c/demand-forecasting-kernels-only/data

from time import time
import argparse

from collections import OrderedDict
import numpy as np
import pandas as pd
from fbprophet import Prophet
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import (
    lit,
    col,
    year,
    month,
    array_min,
    array
)


# Make spark session global so any function can access it and use it
global spark


def load_coocs(path):
    """ load co-occurrences from parquet dataset coming from path"""
    (spark.read.parquet(path))


def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_cooccurrences', metavar="<path>", help=('Input co-occurrences parquet dataset'), type=str, required=True)
    parser.add_argument('--in_diseases', metavar="<path>", help=('Input diseases json dataset from beta ETL'), type=str, required=True)
    parser.add_argument('--in_targets', metavar="<path>", help=('Input targets json dataset from beta ETL'), type=str, required=True)
    parser.add_argument('--out_prefix', metavar="<path>", help=("Output path prefix"), type=str, required=True)
    parser.add_argument('--local', help="run local[*]", action='store_true', required=False, default=True)
    args = parser.parse_args()
    return args


def main(args):
    sparkConf = (SparkConf()
                 .set("parquet.enable.summary-metadata", "true")
                 .set("spark.driver.maxResultSize", "0")
                 .set("spark.debug.maxToStringFields", "2000")
                 .set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
            )

    if args.local:
        spark = (
            SparkSession.builder
                .config(conf=sparkConf)
                .master('local[*]')
                .getOrCreate()
        )
    else:
        spark = (
            SparkSession.builder
                .config(conf=sparkConf)
                .getOrCreate()
        )

    print('args: ', args)
    print('Spark version: ', spark.version)
    start_time = time()

    coocs_columns = [
        "year",
        "month",
        "day",
        "pmid",
        "keywordId1",
        "keywordId2",
        "evidence_score"
    ]

    # load co-occurrences from parquet dataset coming from path
    coocs = (spark.read.parquet(args.in_cooccurrences))
    targets = (spark.read.json(args.in_targets))
    diseases = (spark.read.json(args.in_diseases))

    aggregated = (
        coocs
            .withColumn("year", year(coocs.pubDate))
            .withColumn("month", month(coocs.pubDate))
            .withColumn("day", lit(1))
            .filter(coocs.isMapped == True and coocs.type == "GP-DS" and coocs.year.isNotNull and coocs.month.isNotNull)
            .withColumn("evidence_score", array_min(array(coocs.evidence_score / 10.0, lit(1.0))))
            .selectExpr(*coocs_columns)
    )

    # write the processed data out to out_parquet arg
    (aggregated.write.parquet(args.out_parquet))

    print('Completed in {:.1f} secs'.format(time() - start_time))

    return 0


if __name__ == '__main__':
    args = parse_args()
    exit(main(args))

