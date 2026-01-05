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

class Upserter:
    def __init__(self, merge_query, temp_view_name):
        self.merge_query = merge_query
        self.temp_view_name = temp_view_name

    def upsert(self, df_micro_batch, batch_id):
        df_micro_batch.createOrReplaceTempView(self.temp_view_name)
        df_micro_batch._jdf.sparkSession().sql(self.merge_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class Gold():
    def __init__(self):
        self.initialized = True
        self.gold = "abfss://olist_project@onelake.dfs.fabric.microsoft.com/olist_LH.Lakehouse/Files/medallion/gold"
 
    def upsert_order_gl(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        query = f"""
            MERGE INTO order_gl a
            USING order_gl_delta b
            ON a.order_id = b.order_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        data_upserter = Upserter(query, "order_gl_delta")

        df_delta = (spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table("order_sl")
            .withWatermark("update_time", "30 seconds")
            .withColumn("order_purchase_date", F.to_date("order_purchase_timestamp"))
            .withColumn("order_purchase_time", F.date_format("order_purchase_timestamp", 'HH:mm:ss'))
            .withColumn("delivery_duration", F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
        )
        return self._write_stream_update(df_delta, data_upserter, "order_gl", "order_gl_upsert_stream", "gold_p1", once, processing_time)

    def upsert_order_item_gl(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        query = f"""
            MERGE INTO order_item_gl a
            USING order_item_gl_delta b
            ON a.order_id = b.order_id and a.order_item_id = b.order_item_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        data_upserter = Upserter(query, "order_item_gl_delta")

        df_delta = (spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table("order_item_sl")
            .withWatermark("update_time", "30 seconds")
            .withColumn("order_purchase_date", F.to_date("order_purchase_timestamp"))
            .withColumn("order_purchase_time", F.date_format("order_purchase_timestamp", 'HH:mm:ss'))
            .withColumn("total_value", F.col("price") + F.col("freight_value"))
                
        )
        return self._write_stream_update(df_delta, data_upserter, "order_item_gl", "order_item_gl_upsert_stream", "gold_p1", once, processing_time)

    def upsert_payment_gl(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        query = f"""
            MERGE INTO payment_gl a
            USING payment_gl_delta b
            ON a.order_id = b.order_id and a.payment_sequential = b.payment_sequential
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        data_upserter = Upserter(query, "payment_gl_delta")

        df_delta = (spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table("payment_sl")
            .withWatermark("update_time", "30 seconds")
            .withColumn("order_purchase_date", F.to_date("order_purchase_timestamp"))
            .withColumn("order_purchase_time", F.date_format("order_purchase_timestamp", 'HH:mm:ss'))
        )
        return self._write_stream_update(df_delta, data_upserter, "payment_gl", "payment_gl_upsert_stream", "gold_p1", once, processing_time)

    # --- 辅助写入方法 ---
    def _write_stream_update(self, df, upserter, path, query_name, pool, once, processing_time):
        stream_writer = (df.writeStream
            .foreachBatch(upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", f"{self.gold}/{path}/checkpoints")
            .queryName(query_name)
        )
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
        if once:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
    
    
    def _await_queries(self, once):
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
    
    def upsert(self, once=True, processing_time="5 seconds"):
        import time
        start = int(time.time())
        print(f"\nExecuting gold layer upsert ...")

        self.upsert_order_gl(once, processing_time)
        self.upsert_order_item_gl(once, processing_time)
        self.upsert_payment_gl(once, processing_time)
        self._await_queries(once)
        print(f"Completed gold layer 1 upsert {int(time.time()) - start} seconds")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
