## Purpose of this script is to process the files of multiple formats(xls/csv) and provide useful insights in summarized manner.


# Import required packages & libraries
import os
import sys
import boto3
import xlrd
import pandas as pd

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from datetime import date
from botocore.exceptions import ClientError
from pyspark.sql.functions import count, col, weekofyear, year, lit, concat, sum, min, max, avg, to_date, coalesce, first, last
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
sc.setLogLevel("ERROR")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY") # Using older date format setting of spark 2.x

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get boto3 client
def get_client():
    return boto3.client("s3")


# Get filenames in a list and convert all xls files to csv
def get_files(bucket, src_dir):
    try:
        s3_client = get_client()
        res = s3_client.list_objects_v2(Bucket=bucket, Prefix=src_dir)
        keys = res.get("Contents")
        fl_list = []
        for file in keys:
            if file["Key"].endswith(".csv") or file["Key"].endswith(".txt"):
                fl_list.append(file["Key"])
            elif file["Key"].endswith(".xls"):
                detail = get_file_details(file["Key"])
                folder = detail[2]
                fl_nm = detail[1]
                df= pd.read_excel(f"s3://{bucket}/{file['Key']}", keep_default_na=False)
                df.to_csv(f"s3://{bucket}/{src_dir}{folder}/{fl_nm}.csv", index=False, sep = ",")
                fl_list.append(f"{src_dir}{folder}/{fl_nm}.csv")
        return fl_list
    except ClientError as Error:
        raise Error.response["Error"]["Code"]


# Get file details
def get_file_details(file):
    full_fl_nm = file.split("/")[-1]
    fl_nm = full_fl_nm.split(".")[-2]
    folder = file.split("/")[-2]
    fl_format = full_fl_nm.split(".")[-1]
    return full_fl_nm, fl_nm, folder


# Read data from file
def read_file(bucket, path):
    s3_client = get_client()
    response = s3_client.get_object(Bucket=bucket, Key=path)
    return response['Body'].read().decode("utf-8")


# Move file from one location to another
def file_move(bucket, src_file, tgt_dir, fl_nm):
    tgt_folder = tgt_dir.split("/")[-2]
    s3_client = get_client()
    copy_source = {
        "Bucket": bucket,
        "Key": src_file
    }
    s3_client.copy_object(Bucket=bucket, CopySource=copy_source, Key=f"{tgt_dir}/{fl_nm}")
    print(f"{fl_nm} has been processed and moved to {tgt_folder} folder.")


# Check header, count, indicator & nulls
def misc_check(bucket, src_file, src_dir, fl_format):
    counter = read_file(bucket, f"{src_dir}/counter.txt")
    header = read_file(bucket, f"{src_dir}/header.txt").split(",")
    indicator = read_file(bucket, f"{src_dir}/indicator.txt")

    if indicator == "indicator":
        if fl_format == "csv":
            path = f"s3://{bucket}/{src_file}"
            df = spark.read.csv(path, sep=",", header=True)
            nulls = df.na.drop(how="all").filter(col("Script name").isNull()).count() # Checking if null values present in script name column
            cnt = df.count()
            head = df.columns
            

        if str(cnt) == counter and header == head and nulls == 0:
            return True
        else:
            print(cnt, head)
            print("counter/header/null issue.")
            return False
    else:
        print("Indicator file issue.")
        return False


# Convert date from multiple types to single standarized format
def convert_date(dt_column, formats=("MM/dd/yyyy", "yyyy-MM-dd")):
    return coalesce(*[to_date(dt_column, f) for f in formats])


# Move files from inbound to pre-processed
def inbound_to_pre_processed(bucket, src_dir, tgt_dir):
    try:
        resp = get_files(bucket, src_dir)
        for file in resp:
            detail = get_file_details(file)
            folder = detail[2]
            full_fl_nm = detail[0]
            file_move(bucket, file, f"{tgt_dir}{folder}", full_fl_nm)
    except Exception as e:
        raise e


# Validate files and move to landing
def pre_processed_to_landing(bucket, src_dir, tgt_dir):
    try:
        resp = get_files(bucket, src_dir)
        for file in resp:
            folder = file.split("/")[-2]
            file_name = file.split("/")[-1]
            file_format = file_name.split(".")[-1]
            if file_format == "csv":
                res = misc_check(bucket, file, f"{src_dir}{folder}", file_format)
                if res:
                    file_move(bucket, file, f"{tgt_dir}{folder}", file_name)
                else:
                    print(f"{file_name} has an issue.")
            elif file_format == "txt":
                pass
            else:
                print(f"Invalid file format at {file}.")
    except Exception as e:
        raise e


# Standarized all the files and move it to standardized layer
def landing_to_standardized(bucket, src_dir, tgt_dir):
    
    Schema = StructType([
        StructField('Script name', StringType()),
        StructField('Date', StringType()),
        StructField('Open', DoubleType()),
        StructField('High', DoubleType()),
        StructField('Low', DoubleType()),
        StructField('Close', DoubleType()),
        StructField('Adj Close', DoubleType()),
        StructField('Volume', LongType()),
        ])
    
    src_path = f"s3://{bucket}/{src_dir}/*/*.csv"
    
    df = spark.read.csv(src_path, sep=',', header=True, schema=Schema)
    df = df.withColumn('Date', convert_date(col('Date'))) \
            .withColumnRenamed("Adj Close", "Adj_Close") \
            .withColumnRenamed("Script name", "Script_name")

    df.write.partitionBy("Script_name").mode("Overwrite").parquet(f"s3://{bucket}/{tgt_dir}")
    print("Landing to standarized is completed successfully!")


# Produce insights in summarized/outbound layer & move earlier files to archive according to date
def standarized_to_outbound(bucket, src_dir, tgt_dir, archive):
    
    cols = ['Script_name', 'Week_num', 'St_Date', 'End_Date', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']
    today = date.today()
    today = today.strftime("%d%m%Y")
    
    df = spark.read.parquet(f"s3://{bucket}/{src_dir}", header=True)
    df = df.withColumn("Week_num", concat(lit("Week"), weekofyear(df.Date)).cast(StringType())) \
            .withColumn('Year', year('Date')) \
            .groupBy('Week_num', 'Script_name').agg(sum('Volume').alias('Volume'),
                                                    min('Date').alias('St_Date'),
                                                    max('Date').alias('End_Date'),
                                                    first('Open', ignorenulls=True).alias('Open'),
                                                    max('High').alias('High'),
                                                    min('Low').alias('Low'),
                                                    last('Close', ignorenulls=True).alias('Close'),
                                                    avg('Adj_Close').alias('Adj_Close')) \
            .select(cols)

    try:
        files = get_files(bucket, tgt_dir)
        for file in files:
            detail = get_file_details(file)
            file_nm = detail[0]
            file_move(bucket, file, f"{archive}{today}", file_nm)
    except:
        print('Error/No files to archive.')
    
    df.write.mode('Overwrite').csv(f"s3://{bucket}/{tgt_dir}")


if __name__ == "__main__":
    bucket = "saama-gene-training-data-bucket"
    inbound = "AshishAmbre/inbound/"
    pre_processed = "AshishAmbre/pre-processed/"
    landing = "AshishAmbre/Landing/"
    standardized = "AshishAmbre/Standardized/"
    outbound = "AshishAmbre/Outbound/"
    archive = "AshishAmbre/Archive/"
    
    inbound_to_pre_processed(bucket, inbound, pre_processed)
    pre_processed_to_landing(bucket, pre_processed, landing)
    landing_to_standardized(bucket, landing, standardized)
    standarized_to_outbound(bucket, standardized, outbound, archive)

job.commit()
