# https://databricks.com/blog/2020/01/27/time-series-forecasting-prophet-spark.html
# https://www.kaggle.com/c/demand-forecasting-kernels-only/data
#
# python opentargets-epmc-analysis-ts.py \
#   --in_cooccurrences epmc-cooccurrences \
#   --in_diseases etl/diseases \
#   --in_targets etl/targets \
#   --out_prefix epmc-analysis

from time import time
import argparse
import string
from random import choice

# from collections import OrderedDict
from functools import partial
# import numpy as np
# import pandas as pd
# from fbprophet import Prophet
# from pyspark import SparkConf
# from pyspark.sql import SparkSession
from pyspark import *
from pyspark.sql import *
# from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    lit,
    col,
    year,
    month,
    array_min,
    array,
    broadcast,
    countDistinct,
    collect_list,
    collect_set,
    mean,
    stddev,
    min,
    max,
    expr,
    count,
    sort_array,
    sequence,
    size as array_size,
    pow as pow_fn,
    row_number,
    sum as sum_fn,
    first,
    element_at
)

# Make spark session global so any function can access it and use it
global spark


def make_random_string(length=5):
    allowed_chars = string.ascii_letters  # + string.punctuation
    tmp_name = ''.join(choice(allowed_chars) for _ in range(length))
    return tmp_name


def harmonic_fn(df: DataFrame, partition_cols, over_col, output_col) -> DataFrame:
    prefix = make_random_string()
    i_harmonic = f"{prefix}_harmonic_i"
    partial_harmonic = f"{prefix}_harmonic_dx"

    pcols = [col(x) for x in partition_cols]
    overcol: Column = col(over_col)
    w = Window.partitionBy(*pcols)

    harmonic_df = (
        df.withColumn(i_harmonic, row_number().over(w.orderBy(overcol.desc())))
            .withColumn(partial_harmonic, overcol / pow_fn(i_harmonic, 2.0))
            .withColumn(output_col, sum_fn(col(partial_harmonic)).over(w))
            .drop(i_harmonic, partial_harmonic)
    )

    return harmonic_df


def assoc_fn(df: DataFrame, group_by_cols):
    harmonic_col = "harmonic"
    gbc = [col(x) for x in group_by_cols]
    h_fn = partial(harmonic_fn,
                   partition_cols=group_by_cols,
                   over_col="evs_score",
                   output_col=harmonic_col)
    assoc_df = (
        df.withColumn("evs_score", array_min(array(col("evidence_score") / 10.0, lit(1.0))))
            .transform(h_fn)
            .groupBy(*gbc)
            .agg(countDistinct(col("pmid")).alias("f"),
                 mean(col("evidence_score")).alias("mean"),
                 stddev(col("evidence_score")).alias("std"),
                 max(col("evidence_score")).alias("max"),
                 min(col("evidence_score")).alias("min"),
                 expr("approx_percentile(evidence_score, array(0.25, 0.5, 0.75))").alias("q"),
                 count(col("pmid")).alias("N"),
                 first(col(harmonic_col)).alias(harmonic_col))
            .withColumn("median", element_at(col("q"), 2))
            .withColumn("q1", element_at(col("q"), 1))
            .withColumn("q3", element_at(col("q"), 3))
            .drop("q")
    )

    return assoc_df


def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_cooccurrences', metavar="<path>", help=('Input co-occurrences parquet dataset'), type=str,
                        required=True)
    parser.add_argument('--in_diseases', metavar="<path>", help=('Input diseases json dataset from beta ETL'), type=str,
                        required=True)
    parser.add_argument('--in_targets', metavar="<path>", help=('Input targets json dataset from beta ETL'), type=str,
                        required=True)
    parser.add_argument('--out_prefix', metavar="<path>", help=("Output path prefix"), type=str, required=True)
    parser.add_argument('--local', help="run local[*]", action='store_true', required=False, default=True)
    args = parser.parse_args()
    return args


def main(args):
    sparkConf = (SparkConf()
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

    grouped_keys = [
        "year",
        "month",
        "day",
        "keywordId1",
        "keywordId2"
    ]

    # load co-occurrences from parquet dataset coming from path
    coocs = (spark.read.parquet(args.in_cooccurrences))
    targets = (broadcast(spark.read.json(args.in_targets)
                         .withColumnRenamed("id", "targetId")
                         .orderBy(col("targetId")))
               )
    diseases = (broadcast(spark.read.json(args.in_diseases)
                          .withColumnRenamed("id", "diseaseId")
                          .orderBy(col("diseaseId")))
                )

    # curry function to pass to transform with the keys to group by
    tfn = partial(assoc_fn, group_by_cols=grouped_keys)
    aggregated = (
        coocs
            .withColumn("year", year(coocs.pubDate))
            .withColumn("month", month(coocs.pubDate))
            .withColumn("day", lit(1))
            .filter(
            (coocs.isMapped == True) & (coocs.type == "GP-DS") & col("year").isNotNull() & col("month").isNotNull())
            .selectExpr(*coocs_columns)
            .transform(tfn)
            .withColumnRenamed("keywordId1", "targetId")
            .withColumnRenamed("keywordId2", "diseaseId")
            .join(diseases.selectExpr("diseaseId", "name as label"),
                  on=["diseaseId"])
            .join(targets.selectExpr("targetId", "approvedSymbol as symbol"),
                  on=["targetId"])

    )

    # write the processed data out to out_parquet arg
    (aggregated.write.json(f"{args.out_prefix}/associationsFromCoocsByYM"))

    print('Completed in {:.1f} secs'.format(time() - start_time))

    return 0


if __name__ == '__main__':
    args = parse_args()
    exit(main(args))
