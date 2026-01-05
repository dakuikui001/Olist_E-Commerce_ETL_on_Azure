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

# CELL ********************

import great_expectations as gx
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
import traceback
import os
import json
import threading
import gc

# ==========================================
# 1. 基础配置
# ==========================================
PLATFORM = "FABRIC" 

BASE_PATH = "/lakehouse/default/Files/gx_config/expectations/"
QUARANTINE_TABLE = "data_quality_quarantine"


_SHARED_GX_CONTEXT = None
_CACHED_SUITES_JSON = {}
gx_lock = threading.RLock() 

# ==========================================
# 2. 配置预加载
# ==========================================
def preload_all_suites():
    global _CACHED_SUITES_JSON
    if not os.path.exists(BASE_PATH):
        print(f"⚠️ Warning: Path not found {BASE_PATH}")
        return
    files = [f for f in os.listdir(BASE_PATH) if f.endswith(".json")]
    for f in files:
        suite_name = f.replace(".json", "")
        try:
            with open(os.path.join(BASE_PATH, f), "r", encoding='utf-8') as file:
                suite_dict = json.load(file)
                # 移除可能引起冲突的元数据字段
                suite_dict.pop("name", None)
                suite_dict.pop("data_context_id", None)
                _CACHED_SUITES_JSON[suite_name] = suite_dict
            print(f"✅ Preloaded Suite: {suite_name}")
        except Exception as e:
            print(f"❌ Load error {f}: {e}")

preload_all_suites()

def get_gx_context():
    global _SHARED_GX_CONTEXT
    with gx_lock:
        if _SHARED_GX_CONTEXT is None:
            _SHARED_GX_CONTEXT = gx.get_context(mode="ephemeral")
        return _SHARED_GX_CONTEXT

def load_suite_simple(context, suite_name):
    try:
        return context.suites.get(name=suite_name)
    except Exception:
        if suite_name in _CACHED_SUITES_JSON:
            suite_data = _CACHED_SUITES_JSON[suite_name]
            new_suite = gx.ExpectationSuite(
                name=suite_name, 
                expectations=suite_data.get("expectations", [])
            )
            return context.suites.add(new_suite)
        else:
            raise FileNotFoundError(f"Suite {suite_name} not found in cache.")

# ==========================================
# 3. 核心处理函数
# ==========================================
def validate_and_insert_process_batch(df, catalog, schema, batch_id, table_name): 
    spark_internal = df.sparkSession
    if df.limit(1).count() == 0: return

    full_target_table = f"{catalog}.{schema}.{table_name}" if PLATFORM == "DATABRICKS" else table_name
    temp_id_col = "_dq_batch_id"
    ds_name = f"ds_{table_name}_{batch_id}"
    val_def_name = f"val_{table_name}_{batch_id}"
    
    # 持久化以确保 ID 稳定
    df_with_id = df.withColumn(temp_id_col, F.monotonically_increasing_id()).persist()
    result = None 

    # --- 锁内验证流 ---
    with gx_lock:
        try:
            print(f"🔒 Batch {batch_id}: Processing {table_name}...", flush=True)
            context = get_gx_context()
            
            # 清理旧定义
            for n in [val_def_name, ds_name]:
                try: 
                    context.validation_definitions.delete(n) if "val" in n else context.data_sources.delete(n)
                except: pass

            datasource = context.data_sources.add_spark(name=ds_name)
            asset = datasource.add_dataframe_asset(name=f"asset_{batch_id}")
            batch_def = asset.add_batch_definition_whole_dataframe(name="batch_def")
            suite = load_suite_simple(context, f"{table_name}_suite")
            
            val_definition = context.validation_definitions.add(
                gx.ValidationDefinition(name=val_def_name, data=batch_def, suite=suite)
            )

            print(f"🚀 Batch {batch_id}: Running GX for {table_name}...", flush=True)
            result = val_definition.run(
                batch_parameters={"dataframe": df_with_id},
                result_format={"result_format": "COMPLETE", "unexpected_index_column_names": [temp_id_col]}
            )
            
            if result.success:
                print(f"✅ Batch {batch_id}: {table_name} Passed.", flush=True)
            else:
                print(f"⚠️ Batch {batch_id}: {table_name} FAILED.", flush=True)

            # 及时释放 Context 资源
            context.validation_definitions.delete(val_def_name)
            context.data_sources.delete(ds_name)
        except Exception as e:
            print(f"❌ Batch {batch_id} GX Error on {table_name}: {str(e)}", flush=True)
            df_with_id.drop(temp_id_col).write.mode("append").saveAsTable(full_target_table)
            return 
        finally:
            print(f"🔓 Batch {batch_id}: {table_name} Released Lock.", flush=True)
            gc.collect()

    # --- 锁外入库逻辑  ---
    try:
        if result and result.success:
            df_with_id.drop(temp_id_col).write.mode("append").option("mergeSchema", "true").saveAsTable(full_target_table)
        
        elif result:
            errors = []
            for r in result.results:
                if not r.success:
                    # 提取行级错误 ID
                    conf = r.expectation_config
                    col = conf.kwargs.get("column", "Table")
                    rule = conf.type
                    ids = r.result.get("unexpected_index_list")
                    if ids:
                        for row_id_dict in ids:
                            val = row_id_dict.get(temp_id_col)
                            if val is not None:
                                errors.append((val, f"[{col}] {rule}"))
            
            # 如果验证失败但没有任何具体的坏行 ID -> 判定为表级结构错误，整批拦截
            if not errors: 
                print(f"🚨 Batch {batch_id}: {table_name} TABLE-LEVEL ERROR! Quarantining entire batch.", flush=True)
                # 打印具体错在哪,帮助排查
                for r in result.results:
                    if not r.success:
                        print(f"   Mismatch Details: {r.result.get('details')}")
                
                # 隔离整批数据
                bad_df = df_with_id.withColumn("violated_rules", F.lit("Table-level Schema/Count Error")) \
                    .withColumn("raw_data", F.to_json(F.struct([c for c in df.columns]))) \
                    .withColumn("origin_table", F.lit(table_name)) \
                    .withColumn("ingestion_time", F.current_timestamp()) \
                    .select(F.col("origin_table").alias("table_name"), F.lit(str(batch_id)).alias("gx_batch_id"),
                            "violated_rules", "raw_data", "ingestion_time")
                bad_df.write.mode("append").option("mergeSchema", "true").saveAsTable(QUARANTINE_TABLE)
                return

            # 行级隔离逻辑
            error_schema = StructType([StructField(temp_id_col, LongType(), True), StructField("violated_rule", StringType(), True)])
            error_info_df = spark_internal.createDataFrame(errors, schema=error_schema) \
                .groupBy(temp_id_col).agg(F.concat_ws("; ", F.collect_list("violated_rule")).alias("violated_rules"))

            bad_row_ids = [e[0] for e in errors]
            bad_df = df_with_id.filter(F.col(temp_id_col).isin(bad_row_ids)).join(error_info_df, on=temp_id_col, how="left") \
                .withColumn("raw_data", F.to_json(F.struct([c for c in df.columns]))) \
                .withColumn("origin_table", F.lit(table_name)).withColumn("ingestion_time", F.current_timestamp()) \
                .select(F.col("origin_table").alias("table_name"), F.lit(str(batch_id)).alias("gx_batch_id"),
                        "violated_rules", "raw_data", "ingestion_time")
            bad_df.write.mode("append").option("mergeSchema", "true").saveAsTable(QUARANTINE_TABLE)
            
            # 将剩下的好行写入目标
            good_df = df_with_id.filter(~F.col(temp_id_col).isin(bad_row_ids)).drop(temp_id_col)
            if good_df.limit(1).count() > 0:
                good_df.write.mode("append").option("mergeSchema", "true").saveAsTable(full_target_table)
            print(f"📦 Batch {batch_id}: {table_name} Quarantined {len(set(bad_row_ids))} rows.", flush=True)

    except Exception as e:
        print(f"❌ Batch {batch_id} Write Error on {table_name}: {str(e)}", flush=True)
        df_with_id.drop(temp_id_col).write.mode("append").saveAsTable(full_target_table)
    finally:
        if df_with_id.is_cached: df_with_id.unpersist()
        gc.collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
