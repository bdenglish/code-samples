import argparse
import logging
import sys

from pyspark.sql import SparkSession


formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(funcName)s  | %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


def write_manifest(_df, _data_set, label='clicked'):
    meta_data = dict(columns=_df.columns,
                     positives=_df.where('label == "clicked"').count(),
                     rows=_df.count())


def main():
    parser = argparse.ArgumentParser(description="app inputs and outputs")
    parser.add_argument("--input_bucket", type=str, help="s3 bucket where input data is stored",
                        default='')
    parser.add_argument("--input_prefix", type=str, help="s3 input files",
                        default='')
    parser.add_argument("--output_bucket", type=str, help="s3 bucket where output data is stored",
                        default='')
    parser.add_argument("--output_prefix", type=str, help="s3 output location",
                        default='')
    args = parser.parse_args()

    for arg in vars(args):
        logger.warning(f'{arg} - {getattr(args, arg)}')

    spark = SparkSession.builder \
        .appName("convert_parquet_data_to_csv") \
        .getOrCreate()

    input_bucket = args.input_bucket
    input_prefix = args.input_prefix
    output_bucket = args.output_bucket
    output_prefix = args.output_prefix

    input_path = f's3://{input_bucket}/{input_prefix}'
    output_path = f's3://{output_bucket}/{output_prefix}'

    logger.warning(f'reading parquet files in {input_path} to spark data frame')
    df = spark.read.parquet(input_path)
    logger.warning(f'writing dataframe to {output_path} as csv')
    df.repartition(32).write.csv(output_path, mode='overwrite')

    spark.stop()


if __name__ == '__main__':
    main()
