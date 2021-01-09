# https://github.com/asidlo/sparkprophet/blob/master/sparkprophet.py
# https://databricks.com/blog/2020/01/27/time-series-forecasting-prophet-spark.html
# https://pages.databricks.com/rs/094-YMS-629/images/Fine-Grained-Time-Series-Forecasting.html
# https://www.kaggle.com/c/demand-forecasting-kernels-only/data
#
# export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/
# export JAVA_OPTS="-server -Xms1G -Xmx20G -Dlogback.configurationFile=logback.xml"
# conda activate opentargets-epmc-analisys-ts
# python opentargets-epmc-analysis-ts.py \
#   --in_cooccurrences epmc-cooccurrences \
#   --in_diseases etl/diseases \
#   --in_targets etl/targets \
#   --out_prefix epmc-analysis

import argparse
import string
import logging

from time import time
from random import choice
from functools import partial

import pandas as pd
from fbprophet import Prophet

from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
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
    element_at,
    concat_ws,
    current_date,
    pandas_udf,
    PandasUDFType,
    to_date
)

# Make some readonly global so any function can access it and use it
global spark
harmonic_col = "harmonic"

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

predictions_selection_keys = [
    "keywordId1",
    "keywordId2",
    "ds",
    "y"
]

predictions_new_keys = [
    "ds",
    "yhat",
    "yhat_upper",
    "yhat_lower"
]

predictions_grouped_keys = [
    "keywordId1",
    "keywordId2",
]

prediction_schema = StructType([
    StructField("keywordId1", StringType()),
    StructField("keywordId2", StringType()),
    StructField("ds", DateType()),
    StructField('y', FloatType()),
    StructField("yhat", FloatType()),
    StructField("yhat_lower", FloatType()),
    StructField("yhat_upper", FloatType())
])


# @pandas_udf(prediction_schema, PandasUDFType.GROUPED_MAP)
def make_predictions(pdf: pd.DataFrame) -> pd.DataFrame:
    """ create the model with a month frequency and cap and floor for a logistic growth """
    periods = 12
    growth_mode = 'logistic'

    pdf = pdf.dropna()
    pdf['cap'] = 1.66
    pdf['floor'] = 0.0
    m = Prophet(growth=growth_mode)
    m.fit(pdf)

    future = m.make_future_dataframe(periods=periods, freq="M", include_history=True)
    future['cap'] = 1.66
    future['floor'] = 0.0
    forecast = m.predict(future)

    fpd = forecast[predictions_new_keys].set_index('ds')
    hpd = pdf[predictions_selection_keys].set_index('ds')

    results = fpd.join(hpd, how='left')
    results.reset_index(level=0, inplace=True)
    results['keywordId1'] = pdf['keywordId1'].iloc[0]
    results['keywordId2'] = pdf['keywordId2'].iloc[0]

    return results[predictions_selection_keys + predictions_new_keys[1:]]


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
                 .set("spark.driver.memory", "10g")
                 .set("spark.driver.maxResultSize", "0")
                 .set("spark.debug.maxToStringFields", "2000")
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

    # load co-occurrences from parquet dataset coming from path
    coocs = (spark.read.parquet(args.in_cooccurrences))
    # targets = (broadcast(spark.read.json(args.in_targets)
    #                      .withColumnRenamed("id", "targetId")
    #                      .orderBy(col("targetId")))
    #            )
    # diseases = (broadcast(spark.read.json(args.in_diseases)
    #                       .withColumnRenamed("id", "diseaseId")
    #                       .orderBy(col("diseaseId")))
    #             )

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
    )

    # write the processed data out to out_parquet arg
    # aggregated.write.json(f"{args.out_prefix}/associationsFromCoocsByYM")

    print('Completed aggregated data in {:.1f} secs'.format(time() - start_time))

    # generate the models
    start_time = time()

    # we need some filtering; not all data is ready to be used
    # 1. at least 2 data points per month
    # 2. there must be data for the year 2020
    w2 = Window.partitionBy(*predictions_grouped_keys)

    fbp = (
        aggregated
            .withColumn("ds", to_date(concat_ws("-", col("year"), col("month"), col("day"))))
            .withColumn("y", col(harmonic_col))
            .withColumn("dtCount", count(col("y")).over(w2))
            .withColumn("dtMaxYear", max(col("year")).over(w2))
            .filter((col("dtCount") > 1) & (col("dtMaxYear") == 2020))
            .select(*predictions_selection_keys)
            .repartition(*predictions_grouped_keys)
            .groupBy(*predictions_grouped_keys)
            .applyInPandas(make_predictions, prediction_schema)
    )

    fbp.write.json(f"{args.out_prefix}/associationsFromCoocsPredictions")
    print('Completed TS analysis (FB Prophet) data in {:.1f} secs'.format(time() - start_time))
    return 0


if __name__ == '__main__':
    args = parse_args()
    exit(main(args))
