# Databricks notebook source
spark

# COMMAND ----------

storage_account = "oliststoragehuilu"
application_id = "379b3855-1556-46f7-a89c-05ed4ea80bb7"
directory_id = "a6dcd622-7624-4c7b-94ab-a8e9c9ee7d5e"


spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", '**************************')
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

csv_path = "abfss://olistdatastorage@oliststoragehuilu.dfs.core.windows.net/bronze/olist_customers_dataset.csv"
customer_df = (spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(csv_path)
     )

customer_df.printSchema()

# COMMAND ----------

csv_path = "abfss://olistdatastorage@oliststoragehuilu.dfs.core.windows.net/bronze/olist_geolocation_dataset.csv"
geolocation_df = (spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(csv_path)
     )

geolocation_df.printSchema()

# COMMAND ----------

csv_path = "abfss://olistdatastorage@oliststoragehuilu.dfs.core.windows.net/bronze/olist_order_items_dataset.csv"
order_item_df = (spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(csv_path)
     )

order_item_df.printSchema()

# COMMAND ----------

csv_path = "abfss://olistdatastorage@oliststoragehuilu.dfs.core.windows.net/bronze/olist_order_payments_dataset.csv"
order_payment_df = (spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(csv_path)
     )

order_payment_df.printSchema()

# COMMAND ----------

csv_path = "abfss://olistdatastorage@oliststoragehuilu.dfs.core.windows.net/bronze/olist_order_reviews_dataset.csv"
order_review_df = (spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(csv_path)
     )

order_review_df.printSchema()

# COMMAND ----------

csv_path = "abfss://olistdatastorage@oliststoragehuilu.dfs.core.windows.net/bronze/olist_orders_dataset.csv"
order_df = (spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(csv_path)
     )

order_df.printSchema()

# COMMAND ----------

csv_path = "abfss://olistdatastorage@oliststoragehuilu.dfs.core.windows.net/bronze/olist_products_dataset.csv"
product_df = (spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(csv_path)
     )

product_df.printSchema()

# COMMAND ----------

csv_path = "abfss://olistdatastorage@oliststoragehuilu.dfs.core.windows.net/bronze/olist_sellers_dataset.csv"
seller_df = (spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema", True)
      .load(csv_path)
     )

seller_df.printSchema()

# COMMAND ----------

!pip install pymongo

# COMMAND ----------

import pandas as pd
from pymongo import MongoClient

# COMMAND ----------

# MongoDB connection details (assuming these are already defined in your script)
hostname = "3fymfo.h.filess.io"
database = "olist_NoSQL_againgreen"
port = "61034"
username = "olist_NoSQL_againgreen"
password = "9cd51c06c55823ec01aa0d3f6dcea6aeda57ee60"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

try:
    # Establish a connection to MongoDB
    client = MongoClient(uri)
    db = client[database]

    # Select the collection (or create if it doesn't exist)
    collection = db["product_categories"]  # Choose a suitable name for your collection

    # Convert the DataFrame to a list of dictionaries for insertion into MongoDB
    product_categories_df = pd.DataFrame(list(collection.find()))

    print("Data uploaded successfully!")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the MongoDB connection
    if client:
        client.close()

# COMMAND ----------

product_categories_df.head()

# COMMAND ----------

product_categories_df = spark.createDataFrame(product_categories_df[['product_category_name','product_category_name_english']])
product_categories_df.printSchema()

# COMMAND ----------

product_english_df = product_df.join(product_categories_df, 'product_category_name', 'inner')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

def clean_df(df, name):
    print(f'cleaning + {name}')
    return df.dropDuplicates().na.drop('all')

# COMMAND ----------

order_df = clean_df(order_df, 'order_df')
customer_df = clean_df(customer_df, 'customer_df')
geolocation_df = clean_df(geolocation_df, 'geolocation_df')
order_item_df = clean_df(order_item_df, 'order_item_df')
order_payment_df = clean_df(order_payment_df, 'order_payment_df')
order_review_df = clean_df(order_review_df, 'order_review_df')
product_df = clean_df(product_df, 'product_df')
seller_df = clean_df(seller_df, 'seller_df')
product_english_df = clean_df(product_english_df, 'product_english_df')

# COMMAND ----------

type(order_df.printSchema())

# COMMAND ----------

def timestampToDate(df,col):
    df = df.withColumn(col, to_date(col))
    return df
order_df = timestampToDate(order_df,'order_purchase_timestamp')
order_df = timestampToDate(order_df,'order_approved_at')
order_df = timestampToDate(order_df,'order_delivered_carrier_date')
order_df = timestampToDate(order_df,'order_delivered_customer_date')
order_df = timestampToDate(order_df,'order_estimated_delivery_date')
order_item_df = timestampToDate(order_item_df,'shipping_limit_date')
order_review_df = timestampToDate(order_review_df,'review_creation_date')
order_review_df = timestampToDate(order_review_df,'review_answer_timestamp')

    

# COMMAND ----------

order_df = order_df.withColumn('actual_delivery_days', date_diff('order_delivered_customer_date', 'order_purchase_timestamp'))
order_df = order_df.withColumn('estimate_delivery_days', date_diff('order_estimated_delivery_date', 'order_purchase_timestamp'))
order_df = order_df.withColumn('delay', when(col('actual_delivery_days') > col('estimate_delivery_days'), 1).otherwise(0))
order_df = order_df.withColumn('delay_days', when(col('actual_delivery_days') - col('estimate_delivery_days')>0, col('actual_delivery_days') - col('estimate_delivery_days')).otherwise(0))


# COMMAND ----------

order_full_detailed_df = order_df.join(customer_df, 'customer_id', 'left')\
                                 .join(order_payment_df, 'order_id', 'left')\
                                 .join(order_review_df, 'order_id', 'left')\
                                 .join(order_item_df, 'order_id', 'left')\
                                 .join(product_english_df, 'product_id', 'left')\
                                 .join(seller_df, 'seller_id', 'left')

order_full_detailed_df = order_full_detailed_df.join(geolocation_df, order_full_detailed_df.customer_zip_code_prefix == geolocation_df.geolocation_zip_code_prefix, 'left')
order_full_detailed_df = order_full_detailed_df.drop('geolocation_zip_code_prefix')
order_full_detailed_df.printSchema()

# COMMAND ----------

display(order_full_detailed_df)

# COMMAND ----------

order_full_detailed_df.write.mode('overwrite').parquet("abfss://olistdatastorage@oliststoragehuilu.dfs.core.windows.net/silver/processed_order_full_detailed")