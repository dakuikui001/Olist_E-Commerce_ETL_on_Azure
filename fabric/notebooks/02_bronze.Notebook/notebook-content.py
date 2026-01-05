# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fb6a13b2-9085-409a-bcc6-9940b0b18f65",
# META       "default_lakehouse_name": "olist_LH",
# META       "default_lakehouse_workspace_id": "e0e0a94e-6cb9-428b-a2b5-cfd7a2eace67",
# META       "known_lakehouses": [
# META         {
# META           "id": "fb6a13b2-9085-409a-bcc6-9940b0b18f65"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "8f20039c-2119-9844-4326-b13751909782",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

Once = True
ProcessingTime = '5 seconds'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def check_duplicates(df):    
    print('Checking for duplicates ',end='')
    cleaned_df = df.dropDuplicates()
    print('OK')
    return df

def check_all_null(df):
    print('Checking for all-null rows ', end='')
    cleaned_df = df.dropna(how='all')
    print('OK')
    return cleaned_df

def check_null(df, columns):
    print('Checking for nulls on string columns ',end = '')
    processed_df1 = df.fillna('Unknown', subset = columns)
    print('OK')
    print('Checking for nulls on numeric columns ',end = '')
    processed_df2  = processed_df1.fillna(0, subset = columns)
    print('OK')
    return processed_df2

def preprocessing(df):
    df1 = check_duplicates(df)
    df2 = check_all_null(df1)
    df3 = check_null(df2, df2.schema.names)
    return df3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class Bronze():
    def __init__(self):
        self.initialized = True
        self.bronze = "abfss://olist_project@onelake.dfs.fabric.microsoft.com/olist_LH.Lakehouse/Files/medallion/bronze"
  
    def consume_order_bz(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        schema = '''
                order_id string,
                customer_id string,
                order_status string,
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP
        '''
        
        df_stream = (spark.readStream
                        .format("csv")
                        .schema(schema)
                        .option("header", "true")
                        .option("recursiveFileLookup", "true") 
                        .option("pathGlobFilter", "*.csv")
                        .option("maxFilesPerTrigger", 10) 
                        .load('Files/landing/olist_orders_dataset')
                        .withColumn("load_time", F.current_timestamp())
                        .withColumn("source_file", F.col("_metadata.file_path"))
                    )
        #processed_stream = preprocessing(df_stream)
        return self._write_stream_append(df_stream, "order_bz", "order_bz_ingestion_stream", "bronze_p1", once, processing_time)
    

    def consume_order_item_bz(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        schema = '''
                order_id string,
                order_item_id string,
                product_id string,
                seller_id string,
                shipping_limit_date TIMESTAMP,
                price DOUBLE,
                freight_value DOUBLE,
                order_purchase_timestamp TIMESTAMP    
        '''
        
        df_stream = (spark.readStream
                        .format("csv")
                        .schema(schema)
                        .option("header", "true")
                        .option("recursiveFileLookup", "true") 
                        .option("pathGlobFilter", "*.csv")
                        .option("maxFilesPerTrigger", 10) 
                        .load("Files/landing/olist_order_items_dataset")
                        .withColumn("load_time", F.current_timestamp())
                        .withColumn("source_file", F.col("_metadata.file_path"))
                    )
        #processed_stream = preprocessing(df_stream)
        return self._write_stream_append(df_stream, "order_item_bz", "order_item_bz_ingestion_stream", "bronze_p1", once, processing_time)
    
    def consume_review_bz(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        schema = '''
                review_id string,
                order_id string,
                review_score INTEGER,
                review_comment_title string,
                review_comment_message string,
                review_creation_date TIMESTAMP,
                review_answer_timestamp TIMESTAMP,
                order_purchase_timestamp TIMESTAMP
        '''
        
        df_stream = (spark.readStream
                        .format("csv")
                        .schema(schema)
                        .option("header", "true")
                        .option("recursiveFileLookup", "true") 
                        .option("pathGlobFilter", "*.csv")
                        .option("maxFilesPerTrigger", 10) 
                        .option("multiLine", "true")   
                        .option("escape", '"')
                        .option("quote", '"')
                        .load("Files/landing/olist_order_reviews_dataset")
                        .withColumn("load_time", F.current_timestamp())
                        .withColumn("source_file", F.col("_metadata.file_path"))
                    )
        #processed_stream = preprocessing(df_stream)
        return self._write_stream_append(df_stream, "review_bz", "review_bz_ingestion_stream", "bronze_p1", once, processing_time)

    def consume_payment_bz(self, once=True, processing_time="5 seconds"):
        from pyspark.sql import functions as F
        schema = '''
                order_id string,
                payment_sequential INTEGER,
                payment_type string,
                payment_installments INTEGER,
                payment_value double,
                order_purchase_timestamp TIMESTAMP
        '''
        
        df_stream = (spark.readStream
                        .format("csv")
                        .schema(schema)
                        .option("header", "true")
                        .option("recursiveFileLookup", "true") 
                        .option("pathGlobFilter", "*.csv")
                        .option("maxFilesPerTrigger", 10) 
                        .load("Files/landing/olist_order_payments_dataset")
                        .withColumn("load_time", F.current_timestamp())
                        .withColumn("source_file", F.col("_metadata.file_path"))
                    )
        #processed_stream = preprocessing(df_stream)
        return self._write_stream_append(df_stream, "payment_bz", "payment_bz_ingestion_stream", "bronze_p1", once, processing_time)
   
    
    def _write_stream_append(self, df, path, query_name, pool, once, processing_time):
        table_name = path 
        
        stream_writer = (df.writeStream
            .foreachBatch(lambda micro_df, batch_id: validate_and_insert_process_batch(micro_df,"", "olist_LH", batch_id, table_name))
            .option("checkpointLocation", f"{self.bronze}/{path}/checkpoints")
            .queryName(query_name)
        )
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
        
        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()


    def consume(self, once=True, processing_time="5 seconds"):
        import time
        start = int(time.time())
        print(f"\nStarting bronze layer consumption ...")
        
        self.consume_order_bz(once, processing_time)
        self.consume_order_item_bz(once, processing_time)
        self.consume_review_bz(once, processing_time)
        self.consume_payment_bz(once, processing_time)
        
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
                
        print(f"Completed bronze layer consumtion {int(time.time()) - start} seconds")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
