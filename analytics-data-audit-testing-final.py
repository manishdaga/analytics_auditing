# all the library to import
import sys
import os
import io
import json
import time
from datetime import datetime, timedelta
import math
import pandas as pd
import boto3
import pytz
import traceback
from botocore.exceptions import ClientError
import logging
import json
import inspect
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import concat, col, lit, to_timestamp, year, month, dayofmonth, lower, ceil, row_number, max, \
    when, expr, desc, lag, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import email_sender as es
import concurrent.futures

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'HistoricalData'])
job_name = args.get('JOB_NAME')
HistoricalData = json.loads(args.get('HistoricalData'))

######### S3 Configuration #############
s3_bucket = 'analytics-manishdaga'
target_data_dir = 'datalake/audit_data/hudi_data/'
source_data_dir = 'datalake/audit_data/source_data/'

schema_config_dir = f'datalake/audit_data/glue_configs/data_audit_config_file.json'
smtp_config_dir = 'configs/auth_configs/smtp_server_config.json'

s3_source_path = f's3://{s3_bucket}/{source_data_dir}'
s3_target_path = f's3a://{s3_bucket}/{target_data_dir}'

######### S3 Configuration END #############

####### JOB Initialization #########
spark = SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(job_name, args)
spark.conf.set("spark.sql.caseSensitive", "true")
####### JOB Initialization END #########

###### LOGGER SETUP ##########
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(asctime)s :: %(levelname)s ::  %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
###### LOGGER SETUP  END ##########


##### VARIABLES ########
error_message = []
all_table_details = []
region_name = os.environ['AWS_DEFAULT_REGION']

# Timzone Setup ######
tz = pytz.timezone('Asia/Kolkata')
job_start_time_dt = datetime.now(tz)
current_year = job_start_time_dt.year
current_month = job_start_time_dt.month
current_hour = job_start_time_dt.hour
start_time = job_start_time_dt.strftime("%d_%B_%Y_%H_%M")
job_start_time = time.time()
logger.info(f"Job Start date and time : {start_time}  ")
# Timzone Setup End ######

# Boto3 clients Setup ######
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
glue_client = boto3.client('glue')
s3_resource = boto3.resource('s3')
dms_client = boto3.client('dms')

logger.info('Config Done  Started Processiong Job ')


class DataFrameFormatter:
    def __init__(self, rds_name, db_name, athena_tb_name, tb_name, isTableExists, primaryKey, partitionKey,
                 partitionColumn, date_column, s3_hudi_dir, all_primary_keys, audit_column, fixed_columns):
        self.rds_name = rds_name
        self.db_name = db_name
        self.athena_tb_name = athena_tb_name
        self.tb_name = tb_name
        self.primaryKey = primaryKey
        self.partitionKey = partitionKey
        self.partitionColumn = partitionColumn
        self.date_column = date_column
        self.isTableExists = isTableExists
        self.s3_hudi_dir = s3_hudi_dir
        self.all_primary_keys = all_primary_keys
        self.audit_column = audit_column
        self.fixed_columns = fixed_columns

    def read_df_from_hudi(self, rds_name, db_name, athena_tb_name, tb_name, primary_key, date_column, s3_hudi_dir):
        hudi_options = {
            "hoodie.datasource.read.partitionpath.field": "rds,db,tb",
            "hoodie.datasource.write.recordkey.field": primary_key,
            "hoodie.datasource.hive_sync.table": athena_tb_name
        }
        partition_path = f"/{rds_name}/{db_name}/{tb_name}"
        s3_hudi_dir_partition = s3_hudi_dir + partition_path
        logger.info(f'Reading data from This path :-  {s3_hudi_dir_partition} and create spark dataframe ')
        df = spark.read.format("org.apache.hudi").options(**hudi_options).load(s3_hudi_dir_partition)
        logger.info('Hudi dataframe readed Sucessfully here is the look of the df ')
        columns_to_drop = df.columns[:5]
        df = df.drop(*columns_to_drop)
        logger.info(f"Drop unnecessary columns That was created by hudi {columns_to_drop} ")
        df = df.withColumn('is_row_exists_at_athena', lit(True))
        logger.info('After droping columns and creating is_row_exists_at_athena hudi df looks like this ')
        logger.info('Use Window function to rank rows based on the date column within each ID')
        partition_key_to_select_row = primary_key + ['column_name']
        logger.info(f'partition key we are using is {partition_key_to_select_row}')
        window_spec = Window.partitionBy(partition_key_to_select_row).orderBy(desc(date_column))
        ranked_df = df.withColumn("row_num", row_number().over(window_spec))
        logger.info('Select only the latest rows for each ID')
        latest_rows_df = ranked_df.filter(col("row_num") == 1).drop("row_num")
        logger.info('After selecting only one row from each id ')
        return latest_rows_df

    def create_primary_keys(self, df, primary_keys):
        logger.info("Runing Function to map primary key to k1 k2 and k3 ")

        def column_exists(column_name_to_check, all_columns):
            return column_name_to_check in all_columns

        # Function to ensure the primary key has at least 3 elements
        def check_pk(primary_key):
            if len(primary_key) < 3:
                # Add null values to make the list length 3
                primary_key += [None] * (3 - len(primary_key))
            return primary_key[:3]

        primary_keys = check_pk(primary_keys)
        # Get column names of the DataFrame
        column_names = df.columns

        # Iterate through primary key columns
        for i, key_column in enumerate(primary_keys, start=1):
            if column_exists(key_column, column_names):
                df = df.withColumnRenamed(key_column, f'k{i}')
            else:
                df = df.withColumn(f'k{i}', lit(None).cast('string'))

        return df

    def melt_dataframe(self, df, all_primary_keys, fixed_columns, audit_columns, date_column):
        logger.info('Melting DataFrame from wide to long ')
        # Create a window specification based on the fixed_columns for partitioning and ordering by update_at
        window_spec = Window.partitionBy(*all_primary_keys).orderBy(date_column)
        # Add lag columns for each audit column to compare with the previous row
        for col in audit_columns:
            df = df.withColumn(col, F.col(col).cast("string"))
            df = df.withColumn(f"prev_{col}", F.lag(col).over(window_spec))

        # Filter rows where the value has changed from the previous row
        filter_condition = (
            F.expr(" OR ".join([f"({col} != prev_{col} OR (prev_{col} IS NULL AND {col} IS NOT NULL))" for col in audit_columns])))
        # Apply the filter condition
        df = df.filter(filter_condition)
        expressions = ([F.col(fixed_column) for fixed_column in fixed_columns] +
                       [F.expr(f"stack({len(audit_columns)}, " + ", ".join([f"'{col}', {col}, prev_{col}" for col in
                                                                            audit_columns]) + ") as (column_name, column_value, prev_column_value)"), ])
        # Select the necessary columns without date_column
        melted_df = df.select(*expressions)
        # Additional filter to exclude rows where column_value and prev_column_value are the same
        melted_df = melted_df.filter("(column_value != prev_column_value) OR (prev_column_value IS NULL)")
        logger.info('Melting DataFrame done ')
        return melted_df


    def unmelt_dataframe(self, df, all_primary_keys, fixed_columns, date_column):
        # Group by fixed columns and pivot based on melted columns
        logger.info('unmelting DataFrame from long to wide so that we can compare and append data to hudi ')
        # df.show()
        logger.info(
            f'fixed_columns is {fixed_columns} and all_primary_keys is {all_primary_keys} and date_column is {date_column} ')
        unmelted_df = df.groupBy(*fixed_columns).pivot("column_name").agg(expr("first(column_value)"))
        logger.info(f'Grouping the data on :- {fixed_columns}')
        # unmelted_df.show()
        # Define the window specification
        window_spec = Window.partitionBy(*all_primary_keys).orderBy(desc(date_column))
        # Add a row_number column based on the window specification
        logger.info(
            f'applying partitionBy on :- {all_primary_keys} and crating row_num using the {date_column} date_column   ')
        spark_df = unmelted_df.withColumn("row_num", F.row_number().over(window_spec))
        # Select the latest row for each group
        latest_df = spark_df.filter("row_num = 1").drop("row_num")
        latest_df = latest_df.withColumn("is_row_exists_at_athena", lit(True))
        return latest_df

    def create_column_and_format_df(self, spark_df, partitionColumn):
        spark_df = spark_df.withColumn('rds', lit(self.rds_name))
        spark_df = spark_df.withColumn('db', lit(self.db_name))
        spark_df = spark_df.withColumn('tb', lit(self.tb_name))
        spark_df = spark_df.withColumn('is_row_exists_at_athena', lit(False))
        temp_partitionColumn = partitionColumn + '_temp'
        spark_df = spark_df.withColumn(temp_partitionColumn, to_timestamp(col(partitionColumn)))
        spark_df = spark_df.withColumn('yearpart', year(spark_df[temp_partitionColumn])) \
            .withColumn('monthpart', month(spark_df[temp_partitionColumn])) \
            .withColumn('daypart', dayofmonth(spark_df[temp_partitionColumn]))
        spark_df = spark_df.drop(temp_partitionColumn)
        return spark_df

    def format_dataframe(self, df, df_count):
        if df_count <= 0:
            logger.info('df count is less than or equal to 0. Nothing to process')
            return None
        df = self.create_primary_keys(df, self.primaryKey)
        df = self.create_column_and_format_df(df, partitionColumn)
        if self.isTableExists:
            hudi_df = self.read_df_from_hudi(self.rds_name, self.db_name, self.athena_tb_name, self.tb_name,
                                             self.all_primary_keys, self.date_column, self.s3_hudi_dir)
            logger.info('Hudi dataframe has readed sucessfully below is the dataframe from athen ')
            # hudi_df.show()
            athena_df = self.unmelt_dataframe(hudi_df, all_primary_keys, self.fixed_columns, self.date_column)
            logger.info('unmeleted hudi dataframe from long to wide below is df ')
            # athena_df.show()
            hudi_athena_df = df.unionByName(athena_df)
            logger.info('Combined both the dataframe hudi and cdc below is the dataframe  ')
            # hudi_athena_df.show()
            logger.info('Now convert this dataframe from wide to long ')
            final_df = self.melt_dataframe(hudi_athena_df, self.all_primary_keys, self.fixed_columns, self.audit_column,
                                           self.date_column)
            final_df = final_df.filter(final_df.is_row_exists_at_athena == False)
            logger.info('This is the final dataframe that we will push to athena')
            # final_df.show()
        else:
            logger.info('When table does not exists we direcly read data and melt the data')
            final_df = self.melt_dataframe(df, self.all_primary_keys, self.fixed_columns, self.audit_column,self.date_column)
        final_df = final_df.drop('is_row_exists_at_athena', 'prev_column_value')

        return final_df


class HudiWriter:
    def __init__(self, db_name, tb_name, s3_target_path, isTableExists, primaryKey,
                 partitionKey, partitionColumn, date_column, spark_df, df_size):
        self.db_name = db_name
        self.tb_name = tb_name
        self.isTableExists = isTableExists
        self.primaryKey = primaryKey
        self.partitionKey = partitionKey
        self.partitionColumn = partitionColumn
        self.date_column = date_column
        self.spark_df = spark_df
        self.df_size = df_size
        self.table_detail = {}
        self.s3_target_path = s3_target_path

        self.commonConfig = {
            'className': 'org.apache.hudi',
            'hoodie.datasource.hive_sync.use_jdbc': 'false',
            'hoodie.datasource.write.precombine.field': '',
            'hoodie.datasource.write.recordkey.field': self.primaryKey,
            'hoodie.table.name': self.tb_name,
            'hoodie.datasource.hive_sync.database': self.db_name,
            'hoodie.datasource.hive_sync.table': self.tb_name,
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.write.allow.multirow.primarykey': 'true'
        }
        self.partitionDataConfig = {
            'hoodie.datasource.write.partitionpath.field': self.partitionKey,
            'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
            'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.ComplexKeyGenerator',
            'hoodie.datasource.hive_sync.partition_fields': self.partitionKey}
        self.insertConfig = {
            'hoodie.insert.shuffle.parallelism': 20,
            'hoodie.datasource.write.operation': 'insert',
            'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
            'hoodie.cleaner.commits.retained': 10,
            'hoodie.datasource.write.insert.drop.duplicates': 'false',
            'hoodie.merge.allow.duplicate.on.inserts': 'true'
        }

    def write_df_to_hudi(self, df, mode, config):
        try:
            logger.info('Config that we are using is ')
            print(config)
            df.write.format('org.apache.hudi').options(**config).mode(mode).save(self.s3_target_path)
            logger.info(f" {self.db_name} {self.tb_name} Sucessfully written to Hudi at {self.s3_target_path} ")
        except Exception as e:
            err_msg = f"Error while writing in Hudi for {self.db_name} {self.tb_name}. Here is the traceback:\n{traceback.format_exc()}"
            logger.error(error_message)
            err_subject = err_msg.split(':')[0]
            error_message.append(err_msg)
            email_sender.send_error_mail(err_subject, str(e))

    def hudi_initial_load(self):
        try:
            logger.info(f"Initial Load: {self.df_size}")
            self.table_detail['Initial Load'] = self.df_size
            init_partition_config = {**self.commonConfig, **self.partitionDataConfig, **self.insertConfig}
            logger.info(f"Writing to partition {self.db_name} {self.tb_name} Hudi table.")
            self.write_df_to_hudi(self.spark_df, 'Append', init_partition_config)
            logger.info(f" Sucessully write {self.db_name} {self.tb_name}  to Hudi table.")
        except Exception as e:
            err_msg = f"An error occurred during the initial load for {self.db_name} {self.tb_name}: {str(e)}"
            logger.error(err_msg)
            err_subject = err_msg.split(':')[0]
            email_sender.send_error_mail(err_subject, str(e))
            error_message.append(err_msg)

    def hudi_increment_load(self):
        try:
            increment_upsert_partition_config = {**self.commonConfig, **self.partitionDataConfig, **self.insertConfig}
            self.write_df_to_hudi(self.spark_df, 'Append', increment_upsert_partition_config)
        except Exception as e:
            err_msg = f"An error occurred during the incremental load  for {self.db_name} {self.tb_name}: {str(e)}"
            logger.error(err_msg)
            err_subject = err_msg.split(':')[0]
            email_sender.send_error_mail(err_subject, str(e))
            error_message.append(err_msg)

    def process_table(self):
        if self.isTableExists:
            self.hudi_increment_load()
        else:
            logger.info(f'{self.db_name} {self.tb_name} Table does not exist. Going for Initial Load.')
            self.hudi_initial_load()

        self.table_detail['hudi_last_updated_time'] = datetime.now(tz).replace(microsecond=0).now().strftime(
            "%d-%m-%Y %H:%M")
        return self.table_detail


def read_file_from_s3(s3_bucket, file_path, file_type='json'):
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=file_path)
        file_content = response['Body'].read().decode('utf-8')

        if file_type == "json":
            json_content = json.loads(file_content)
            return json_content
        else:
            return file_content
    except Exception as e:
        err_msg = f"Error reading file from S3 bucket '{s3_bucket}' at path '{file_path}': {str(e)}"
        logger.info(err_msg)
        error_subject = err_msg.split(':')[0]
        email_sender.send_error_mail(error_subject, str(e))
        return None


def select_columns_from_df(glue_df, selected_columns):
    logger.info(f'Selecting only these columns :- {selected_columns}')
    schema = glue_df.schema()
    column_names = [field.name for field in schema.fields]
    print(f'Here is the column that we got from DataFrame  {column_names}')
    if 'updated_by_id' in column_names:
        logger.info('Renamming updated_by_id to updated_by')
        glue_df = glue_df.rename_field('updated_by_id', 'updated_by')
    elif 'updated_by'  in column_names:
        glue_df = glue_df.rename_field('updated_by_id', 'updated_by')
        logger.info('updated by is there no need to anything ')
    else:
        glue_df = glue_df.rename_field('updated_by', 'updated_by')
    if selected_columns:
        glue_df = glue_df.apply_mapping([(col, col, 'string') for col in selected_columns])
    glue_df = glue_df.rename_field('update_ts_dms', 'updated_at')
    return glue_df


def read_s3_and_create_df(rds_name, dbName, tbName, s3_path, isTableExists, isHistoricalData, selected_columns,num_days=1):
    try:
        if isTableExists:
            s3_path = generate_s3_paths(s3_path, num_days)
            logger.info(f'Table already exists. Reading CDC files from {s3_path} ')
            glue_df = glueContext.create_dynamic_frame_from_options(connection_type='s3',connection_options={'paths': s3_path},format='parquet')
            glue_df = select_columns_from_df(glue_df, selected_columns)
            spark_df = glue_df.toDF()
        else:
            if isHistoricalData:
                full_load_folder_path = s3_path + full_load_folder_name + '/'
                cdc_folder_path = s3_path + old_cdc_folder_name + '/'
                glue_df_full_load = glueContext.create_dynamic_frame_from_options(connection_type='s3',connection_options={'paths': [full_load_folder_path]},
                                                                                  format='parquet')
                glue_df_cdc = glueContext.create_dynamic_frame_from_options(connection_type='s3', connection_options={
                    'paths': [cdc_folder_path]}, format='parquet')
                glue_df_full_load = select_columns_from_df(glue_df_full_load, selected_columns)
                glue_df_cdc = select_columns_from_df(glue_df_cdc, selected_columns)

                spark_df_full_load = glue_df_full_load.toDF()
                spark_df_cdc = glue_df_cdc.toDF()
                spark_df_full_load = spark_df_full_load.withColumn("Op", lit("I"))
                spark_df = spark_df_full_load.unionByName(spark_df_cdc)
            else:
                pattern = "[\"" + s3_path + "{[!L],O[!A],LO[!A]}**\"]"
                logger.info(f'When isHistoricalData is set to false using this pattern {pattern}  ')
                glue_df = glueContext.create_dynamic_frame_from_options(connection_type='s3',connection_options={'paths': [s3_path],'exclusions': pattern},format='parquet')
                glue_df = select_columns_from_df(glue_df, selected_columns)
                logger.info('Applying and selecting some of the columns ')
                spark_df = glue_df.toDF()
                spark_df = spark_df.withColumn("Op", lit("I"))
        if 'updated_by' not in spark_df.columns:
            spark_df = spark_df.withColumn('updated_by', lit(None).cast(StringType()))
        # spark_df = spark_df.select(*selected_columns)
        df_count = spark_df.count()
        return spark_df, df_count
    except Exception as e:
        err_msg = f' Error occurred while reading S3 {s3_path} and creating DataFrame for {dbName} {tbName} : '
        logger.error(f'{err_msg} Here is more details  {str(e)} ')
        email_sender.send_error_mail(err_msg, str(e))
        error_message.append(err_msg)


def run_query(query):
    '''Runs a query in Athena'''
    try:
        response = athena_client.start_query_execution(QueryString=query,
                                                       QueryExecutionContext={'Database': 'default', },
                                                       ResultConfiguration={'OutputLocation': query_output_path})
        query_execution_id = response['QueryExecutionId']
        while True:
            query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = query_status['QueryExecution']['Status']['State']

            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                return response
            time.sleep(2)

    except Exception as e:
        err_msg = f"An error occurred while running the query: {str(e)}"
        logger.error(f"{err_msg} Here is more details {e}")
        email_sender.send_error_mail(err_msg, e)
        return None


def generate_db_tb_dict(query_response):
    output = athena_client.get_query_results(QueryExecutionId=query_response['QueryExecutionId'])
    result_dict = {}
    for entry in output['ResultSet']['Rows'][1:]:
        db_name = entry['Data'][0]['VarCharValue']
        table_name = entry['Data'][1]['VarCharValue']

        if db_name in result_dict:
            result_dict[db_name].append(table_name)
        else:
            result_dict[db_name] = [table_name]
    return result_dict


def check_table_exists(db_name, tb_name, audit_database, audit_table):
    try:
        glue_client.get_table(DatabaseName=audit_database, Name=audit_table)
        query_string = f"""SELECT DISTINCT db,tb FROM "{audit_database}"."{audit_table}" """
        query_response = run_query(query_string)
        athena_db_tb = generate_db_tb_dict(query_response)

        if db_name in athena_db_tb and tb_name in athena_db_tb[db_name]:
            logger.info(f'Table {db_name}.{tb_name} exists in Athena.')
            return True
        else:
            logger.info(f'Table {db_name}.{tb_name} Does not exists in Athena.')
            return False
    except glue_client.exceptions.EntityNotFoundException:
        logger.info(f'Table {db_name}.{tb_name} does not exist. Table will be created.')
        return False
    except Exception as e:
        err_msg = f'An error occurred while checking table {db_name}.{tb_name} : {str(e)}'
        logger.error(err_msg)
        err_subject = err_msg.split(':')[0]
        email_sender.send_error_mail(err_subject, str(e))
        error_message.append(err_msg)
        raise e


def generate_s3_paths(s3_path, num_days):
    try:
        current_date = datetime.now()
        date_ranges = [(current_date - timedelta(days=i)).strftime("%Y/%m/%d") for i in range(num_days, 0, -1)]
        s3_paths = [s3_path + date_pattern for date_pattern in date_ranges]
        if num_days == 0:
            logger.info('When num days is 0')
            today_date_prefix = datetime.now().strftime("%Y/%m/%d")
            s3_paths =  [s3_path +today_date_prefix ]
    except Exception as e:
        logger.error(f'Error occurred: {str(e)}')
        s3_paths = []
    return s3_paths


def find_dms_file(rds_name, instance_type, db_name=None):
    try:
        if instance_type == 'mysql':
            dms_config_file_name = 'analytics-audit-testing-' + rds_name + '-' + instance_type + '.json'
        elif instance_type == 'postgres':
            dms_config_file_name = 'analytics-audit-testing-' + rds_name + '-' + db_name + '-' + instance_type + '.json'
        logger.info(f'dms config file name is {dms_config_file_name}')
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix='datalake/audit_data/dms_configs')
        all_file_names = [obj['Key'].split('/')[-1] for obj in response.get('Contents', [])]
        dms_config_dir = f'datalake/audit_data/dms_configs/{dms_config_file_name}'
        dms_config_file_content = read_file_from_s3(s3_bucket, dms_config_dir)
        dms_task_name = dms_config_file_name.split('.json')[0].replace('_', '-')
        return dms_task_name, dms_config_file_name, dms_config_file_content, dms_config_dir
    except Exception as e:
        logger.error(f'There was some error while searching file name for the dms task pls check the rds name and database name is correct or not Here is error :- {e}')
        return False


def add_table_to_dms_config(dms_config, dms_config_dir, instance_type, db_name, tb_name):
    if db_name in dms_config['database_name']:
        if tb_name not in dms_config['table_include'][db_name]:
            old_table = dms_config['table_include'][db_name]
            old_table.append(tb_name)
            dms_config['table_include'][db_name] = old_table
            s3_client.put_object(Bucket=s3_bucket, Key=dms_config_dir, Body=json.dumps(dms_config, indent=4))
            return True
            logger.info('file uploaded sucessfully ')
        else:
            logger.info('Table already exists in dms config file  no need to add table')
            return False
    else:
        logger.info('database & table not exists will add the database to the dms task ')
        all_datbase = dms_config['database_name']
        all_datbase.append(db_name)
        dms_config['database_name'] = all_datbase
        if instance_type == 'postgres':
            dms_config['schema_name'][db_name] = 'public'
            dms_config['dms_schema_rename'][db_name] = db_name
        else:
            dms_config['schema_name'][db_name] = db_name
        dms_config['table_include'][db_name] = [tb_name]
        s3_client.put_object(Bucket=s3_bucket, Key=dms_config_dir, Body=json.dumps(dms_config, indent=4))
        return True


def add_table_to_dms_via_glue(job_name, dms_config_dir):
    # Run Glue job with parameters
    job_parameters = {'--file_path': dms_config_dir, }
    response = glue_client.start_job_run(JobName=job_name, Arguments=job_parameters)
    # Get the job run ID
    job_run_id = response['JobRunId']
    logger.info(f"Started Glue job run with ID: {job_run_id}")
    return job_run_id


def check_job_status(job_name, job_run_id):
    # Check job run status
    while True:
        job_run = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        status = job_run['JobRun']['JobRunState']
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break
        # Add a delay before checking again (optional)
        time.sleep(10)
    # Determine whether the job run was successful or failed
    if status == 'SUCCEEDED':
        logger.info("Glue job run was successful!")
        return True
    else:
        logger.error("Glue job run failed!")
        return False


def is_table_in_dms_task(task_name, database_name, table_name):
    try:
        # Describe the replication task
        response = dms_client.describe_replication_tasks(
            Filters=[{'Name': 'replication-task-id', 'Values': [task_name]}])
        replication_task = response['ReplicationTasks'][0]
        table_loading = replication_task['ReplicationTaskStats']['TablesLoading']
        full_load_percent = replication_task['ReplicationTaskStats']['FullLoadProgressPercent']
        # Get the table mappings from the replication task
        table_mappings = replication_task['TableMappings']
        table_mappings_json = json.loads(table_mappings)
        for i in table_mappings_json['rules']:
            if i['rule-type'] == 'selection' and i['object-locator']['table-name'] == table_name:
                    logger.info(f'{database_name}.{table_name} exists in mappning rules ')
                    if full_load_percent == 100 and table_loading == 0:
                        logger.info(f'{database_name}.{table_name} Loaded sucessfully in DMS Task  ')
                        return True,full_load_percent
        logger.info(f'{database_name}.{table_name} does not exist in DMS task.')
        return False, full_load_percent
    except Exception as e:
        logger.error(f"Error checking table existence in DMS Task: {e}")
        return False, None


def copy_object(source_bucket, source_key, destination_bucket, destination_prefix):
    source_file_name = source_key.split('/')[-1]
    if source_file_name.startswith('LOAD'):
        destination_key = f"{destination_prefix}/FullLoad_Files/{source_file_name}"
    elif source_file_name.startswith('202'):
        destination_key = f"{destination_prefix}/OldCdc_Files/{source_file_name}"
    else:
        return
    copy_source = {'Bucket': source_bucket, 'Key': source_key}
    s3_client.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)


def copy_s3_folder_parallel(source_bucket, target_bucket, db_name, tb_name):
    source_prefix = f'datalake/source_data/{db_name}/{tb_name}'
    destination_prefix = f'datalake/audit_data/source_data/{db_name}/{tb_name}'
    logger.info(f'Copying data from {source_prefix} ->  {destination_prefix}')
    continuation_token = None
    while True:
        list_objects_params = {'Bucket': source_bucket, 'Prefix': source_prefix}
        if continuation_token:
            list_objects_params['ContinuationToken'] = continuation_token
        response = s3_client.list_objects_v2(**list_objects_params)
        objects_to_copy = response.get('Contents', [])
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for obj in objects_to_copy:
                source_key = obj['Key']
                futures.append(executor.submit(copy_object, source_bucket, source_key, target_bucket, destination_prefix))
                concurrent.futures.wait(futures)
        if 'NextContinuationToken' in response:
            continuation_token = response['NextContinuationToken']
        else:
            break
    logger.info("All files copied successfully.")


def isHistoricalData_fun(db_name, tb_name):
    databases = HistoricalData.get(db_name, {})
    if databases:
        return tb_name in databases
    else:
        return False


def remove_table_from_hudi(bucket_name, rds_name, db_name, tb_name):
    directory_path = f'datalake/audit_data/hudi_data/{audit_database}/{rds_name}/{db_name}/{tb_name}'
    logger.info(f'Running remove_table_from_hudi and will delete  {directory_path}')
    objects_to_delete = []
    continuation_token = None
    while True:
        list_objects_params = {'Bucket': bucket_name, 'Prefix': directory_path}
        if continuation_token:
            list_objects_params['ContinuationToken'] = continuation_token
        response = s3_client.list_objects_v2(**list_objects_params)

        for obj in response.get('Contents', []):
            objects_to_delete.append({'Key': obj['Key']})

        if 'NextContinuationToken' in response:
            continuation_token = response['NextContinuationToken']
        else:
            break

    # Delete objects in the specified directory
    if objects_to_delete:
        response = s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
        # Check if the delete operation was successful
        if 'Errors' in response:
            logger.error(f"Error deleting objects: {response['Errors']}")
        else:
            logger.info(f"Successfully deleted objects in {bucket_name}/{directory_path}")
    else:
        logger.info(f"No objects found in {bucket_name}/{directory_path}")


email_sender = es.EmailSender(job_name, region_name, logger)
schema_config = read_file_from_s3(s3_bucket, schema_config_dir)
logger.info(f'config that we are using :- {schema_config}')

all_primary_keys = ['rds', 'db', 'tb', 'k1', 'k2', 'k3']
all_primary_keys_str = (',').join(all_primary_keys)
fixed_columns = ['Op', 'rds', 'db', 'tb', 'k1', 'k2', 'k3', 'updated_at', 'updated_by', 'is_row_exists_at_athena','yearpart', 'monthpart', 'daypart']
extra_columns = ['updated_by']
query_output_path = 's3://analytics-manishdaga/datalake/audit_data/temp_query_output/'
audit_database = 'audit'
audit_table = 'audit_data'
old_data_source_bucket = 'analytics-manishdaga '
audit_data_target_bucket = 'analytics-manishdaga'
dms_add_table_glue_job = 'analytics-audit-dms-table-addition'
date_column_ts_dms = 'update_ts_dms'
date_column = 'updated_at'
partitionColumn = date_column
partitionKey = 'rds,db,tb,column_name,yearpart,monthpart,daypart'
full_load_folder_name = 'FullLoad_Files'
old_cdc_folder_name = 'OldCdc_Files'

for table_detail in schema_config[-1:]:
    table_start_time = time.time()
    table_time_dict = {}

    rds_name = table_detail['rds_name']
    rds_instance_type = table_detail['rds_instance_type']
    db_name = table_detail['db_name']
    tb_name = table_detail['tb_name']
    isHistoricalData = isHistoricalData_fun(db_name, tb_name)
    audit_column = table_detail.get('audit_column', '')
    primaryKey = table_detail.get('primaryKey', '')
    isTableExists = check_table_exists(db_name, tb_name, audit_database, audit_table)
    IsDelete = table_detail.get('IsDelete', False)
    s3_source_dir = s3_source_path + db_name + '/' + tb_name + '/'
    s3_target_dir = s3_target_path + audit_database

    logger.info(f'Processing Start for {db_name} {tb_name}')
    logger.info(f'Is delete is {IsDelete}')
    logger.info(f'isHistoricalData is {isHistoricalData}')
    logger.info(f' Table Details for  {db_name} {tb_name} ')
    logger.info(f' Reading data from :- {s3_source_dir} and Writing data to {s3_target_dir} ')
    logger.info(f'{db_name} {tb_name} isTableExists: {isTableExists} ')
    logger.info(f'{db_name} {tb_name}  primaryKey: {primaryKey} ')
    logger.info(f'{db_name} {tb_name}  partitionKey: {partitionKey}')
    logger.info(f'{db_name} {tb_name} date_column : {date_column} ')
    logger.info(f'{db_name} {tb_name} audit_colums : {audit_column} ')

    if IsDelete:
        logger.info('Is Delete is True because of that removing data ')
        remove_table_from_hudi(s3_bucket, rds_name, db_name, tb_name)
        continue

    if not (isTableExists):
        logger.info('Table does not exists we have to add table to  dms task and copy the data from source folder to s3 target ')
        dms_task_name, dms_config_file_name, dms_config_file_content, dms_config_dir = find_dms_file(rds_name,
                                                                                                     rds_instance_type,
                                                                                                     db_name)
        if dms_config_file_content:
            db_name_dms = db_name.replace('-', '_')
            add_table_to_dms = add_table_to_dms_config(dms_config_file_content, dms_config_dir, rds_instance_type,db_name_dms, tb_name)
            if add_table_to_dms:
                job_run_id = add_table_to_dms_via_glue(dms_add_table_glue_job, dms_config_dir)
                job_status = check_job_status(dms_add_table_glue_job, job_run_id)
                if job_status:
                    IsTableAdded, full_load_percent = is_table_in_dms_task(dms_task_name, db_name_dms, tb_name)
                    while not IsTableAdded:
                        logger.info(f"Waiting for {db_name_dms}.{tb_name} to be added  Completed {full_load_percent} to the DMS task...")
                        time.sleep(10)
                        IsTableAdded, full_load_percent = is_table_in_dms_task(dms_task_name, db_name_dms, tb_name)
                else:
                    logger.error(f'{dms_add_table_glue_job} Got Failed ')
            else:
                logger.info(f'add_table_to_dms :- {add_table_to_dms} Table already exists in config  will check the dms task ')
                IsTableAdded, full_load_percent = is_table_in_dms_task(dms_task_name, db_name_dms, tb_name)

        if isHistoricalData:
            logger.info(f'isHistoricalData :- {isHistoricalData} Copying older data ')
            logger.info(f'Copying data from {old_data_source_bucket} to  {audit_data_target_bucket}  for {db_name} {tb_name} ')
            # copy_s3_folder_parallel(old_data_source_bucket, audit_data_target_bucket, db_name, tb_name)
        else:
            logger.info(f'isHistoricalData :- {isHistoricalData} Not copying older data ')
    else:
        logger.info('Table Already exists we have to just process the data ')


    read_only_column = ('Op' + ',' + ",".join(primaryKey) + ',' + ",".join(audit_column) + ',' + date_column_ts_dms + ',' + ",".join(extra_columns)).split(',')
    spark_df, df_count = read_s3_and_create_df(rds_name, db_name, tb_name, s3_source_dir, isTableExists,isHistoricalData, read_only_column,num_days=0)
    logger.info('Dataframe creation is sucessfully below is preview ')
    spark_df.show(5)
    logger.info('here is the schema for spark_df ')
    spark_df.printSchema()
    if df_count:
        df_formatter = DataFrameFormatter(rds_name, db_name, audit_table, tb_name, isTableExists, primaryKey,partitionKey, partitionColumn, date_column, s3_target_dir, all_primary_keys,audit_column, fixed_columns)
        spark_df = df_formatter.format_dataframe(spark_df, df_count)
        logger.info('formating done ')
        spark_df.show(5)
        hudi_writer = HudiWriter(audit_database, audit_table, s3_target_dir, isTableExists, all_primary_keys_str,
                                 partitionKey, partitionColumn, date_column, spark_df, df_count)
        logger.info('Writing to Hudi ')
        hudi_operations_stats = hudi_writer.process_table()
        table_detail.update(hudi_operations_stats)
        table_detail['Total Time'] = int(time.time() - table_start_time)
        table_detail.update(table_time_dict)
        all_table_details.append(table_detail)
    else:
        logger.info('Empty ')

if len(error_message) > 0:
    final_erro_mes = f'{job_name}  is not sucessfully completed pls check below errors '
    logger.critical(error_message)
    email_sender.send_error_mail(final_erro_mes, error_message)
else:
    logger.info('Job is sucessfully completed')
    logger.info('Done With processing all the database and tables ')
    # updates_df = update_df_s3_mail(all_table_details)
    # email_sender.send_success_mail(updates_df)
    job.commit()
    logger.info('Job Commited')


