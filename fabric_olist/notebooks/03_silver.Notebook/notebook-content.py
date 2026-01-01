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

# Welcome to your new notebook
# Type here in the cell editor to add code!
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

class Silver():
    def __init__(self):
        self.initialized = True
        self.silver = "abfss://olist_project@onelake.dfs.fabric.microsoft.com/olist_LH.Lakehouse/Files/medallion/silver"
 
    def upsert_order_sl(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        query = f"""
            MERGE INTO order_sl a
            USING order_sl_delta b
            ON a.order_id = b.order_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        data_upserter = Upserter(query, "order_sl_delta")

        df_delta = (spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table("order_bz")
            .withWatermark("load_time", "30 seconds")
            .withColumn("update_time", F.current_timestamp())
        )
        return self._write_stream_update(df_delta, data_upserter, "order_sl", "order_sl_upsert_stream", "silver_p1", once, processing_time)

    def upsert_order_item_sl(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        query = f"""
            MERGE INTO order_item_sl a
            USING order_item_sl_delta b
            ON a.order_id = b.order_id and a.order_item_id = b.order_item_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        data_upserter = Upserter(query, "order_item_sl_delta")

        df_delta = (spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table("order_item_bz")
            .withWatermark("load_time", "30 seconds")
            .withColumn("update_time", F.current_timestamp())
        )
        return self._write_stream_update(df_delta, data_upserter, "order_item_sl", "order_item_sl_upsert_stream", "silver_p1", once, processing_time)

    def upsert_payment_sl(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        query = f"""
            MERGE INTO payment_sl a
            USING payment_sl_delta b
            ON a.order_id = b.order_id and a.payment_sequential = b.payment_sequential
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        data_upserter = Upserter(query, "payment_sl_delta")

        df_delta = (spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table("payment_bz")
            .withWatermark("load_time", "30 seconds")
            .withColumn("update_time", F.current_timestamp())
        )
        return self._write_stream_update(df_delta, data_upserter, "payment_sl", "payment_sl_upsert_stream", "silver_p1", once, processing_time)

    def upsert_review_sl(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        query = f"""
            MERGE INTO review_sl a
            USING review_sl_delta b
            ON a.order_id = b.order_id and a.review_id = b.review_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        data_upserter = Upserter(query, "review_sl_delta")

        df_delta = (spark.readStream
            .option("startingVersion", startingVersion)
            .option("ignoreDeletes", True)
            .table("review_bz")
            .withWatermark("load_time", "30 seconds")
            .withColumn("update_time", F.current_timestamp())
        )
        return self._write_stream_update(df_delta, data_upserter, "review_sl", "review_sl_upsert_stream", "silver_p1", once, processing_time)


    # --- 辅助写入方法 ---
    def _write_stream_update(self, df, upserter, path, query_name, pool, once, processing_time):
        stream_writer = (df.writeStream
            .foreachBatch(upserter.upsert)
            .outputMode("update")
            .option("checkpointLocation", f"{self.silver}/{path}/checkpoints")
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
        print(f"\nExecuting silver layer upsert ...")

        # 阶段 1: 基础事实表/维度表同步
        self.upsert_order_sl(once, processing_time)
        self.upsert_order_item_sl(once, processing_time)
        self.upsert_payment_sl(once, processing_time)
        self.upsert_review_sl(once, processing_time)
        self._await_queries(once)
        print(f"Completed silver layer 1 upsert {int(time.time()) - start} seconds")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
