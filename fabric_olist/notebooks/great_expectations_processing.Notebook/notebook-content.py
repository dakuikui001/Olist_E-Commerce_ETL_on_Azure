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
from pyspark.sql.types import *
import json
import os
import threading

# --- 全局单例与并发控制 ---
_GX_GLOBAL_LOCK = threading.Lock()
_CACHED_SUITES = {}

def preload_all_suites():
    """将所有 JSON 规则文件预加载到内存字典中"""
    global _CACHED_SUITES
    _CACHED_SUITES = {}
    # 请根据实际路径调整
    base_path = "/lakehouse/default/Files/gx_config/expectations/"
    
    if not os.path.exists(base_path):
        print(f"❌ 错误: 路径不存在 {base_path}")
        return

    files = [f for f in os.listdir(base_path) if f.endswith(".json")]
    for f in files:
        suite_key = f.replace(".json", "")
        path = os.path.join(base_path, f)
        try:
            with open(path, "r", encoding='utf-8') as file:
                _CACHED_SUITES[suite_key] = json.load(file)
            print(f"✅ 成功预加载配置: {suite_key}")
        except Exception as e:
            print(f"⚠️ 加载 {f} 失败: {e}")

    print(f"\n📢 内存中已就绪的配置: {list(_CACHED_SUITES.keys())}")

# 立即执行预加载
preload_all_suites()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _execute_streaming_dispatch(df_with_id, errors_map, table_name, batch_id, temp_id_col):
    """
    内部函数：负责将数据分流写入目标表和隔离表
    """
    spark = df_with_id.sparkSession
    
    # 构造错误信息表
    error_data = [(int(k), "; ".join(v)) for k, v in errors_map.items()]
    error_schema = StructType([
        StructField(temp_id_col, LongType(), False),
        StructField("violated_rules", StringType(), True)
    ])
    error_info_df = spark.createDataFrame(error_data, error_schema)
    
    # 1. 隔离脏数据
    cols_to_json = [c for c in df_with_id.columns if c != temp_id_col]
    bad_df = df_with_id.join(error_info_df, on=temp_id_col, how="inner") \
        .withColumn("raw_data", F.to_json(F.struct(*cols_to_json))) \
        .withColumn("table_name", F.lit(table_name)) \
        .withColumn("batch_id", F.lit(batch_id).cast("long")) \
        .withColumn("ingestion_time", F.current_timestamp()) \
        .select("table_name", "batch_id", "violated_rules", "raw_data", "ingestion_time")
    
    # 写入隔离表 (前提：已手动 SQL 建表)
    bad_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("data_quality_quarantine")
    
    # 2. 写入好数据
    good_df = df_with_id.join(error_info_df, on=temp_id_col, how="left_anti").drop(temp_id_col)
    if good_df.limit(1).count() > 0:
        good_df.write.format("delta").mode("append").saveAsTable(table_name)
    
    print(f"⚠️ Batch {batch_id}: {table_name} 拦截 {len(errors_map)} 行，入库 {good_df.count()} 行")

def validate_and_insert_process_batch(df, batch_id, table_name):
    """
    foreachBatch 调用的核心验证函数
    """
    if df.limit(1).count() == 0:
        return

    # 这里的 table_name 假设是 "order_bz"
    suite_key = f"{table_name}_suite"
    temp_id_col = "_temp_row_id"
    
    # 给数据打上临时 ID
    df_with_id = df.withColumn(temp_id_col, F.monotonically_increasing_id()).persist()
    errors_map = None # 默认为 None

    # --- 独占式验证块 ---
    with _GX_GLOBAL_LOCK:
        try:
            if suite_key not in _CACHED_SUITES:
                raise ValueError(f"内存中缺少配置: {suite_key}")

            # 每次验证使用全新的隔离 Context，杜绝 I/O 冲突
            context = gx.get_context(mode="ephemeral")
            suite_data = _CACHED_SUITES[suite_key]
            
            # 构建内存 Suite
            suite = context.suites.add(
                gx.ExpectationSuite(name=suite_key, expectations=suite_data.get("expectations", []))
            )
            
            # 配置数据源
            ds_name = f"ds_{table_name}_{batch_id}"
            datasource = context.data_sources.add_spark(name=ds_name)
            asset = datasource.add_dataframe_asset(name="tmp_asset")

            # 针对日期格式的特殊处理 (strftime 补丁)
            working_df = df_with_id
            for exp in suite.expectations:
                r_type = getattr(exp, "expectation_type", str(exp))
                if "strftime" in r_type:
                    col_name = exp.configuration.kwargs.get("column")
                    working_df = working_df.withColumn(col_name, F.col(col_name).cast("string"))

            validator = context.get_validator(
                batch_request=asset.build_batch_request(options={"dataframe": working_df}),
                expectation_suite=suite
            )

            # 运行验证并收集错误
            current_errors = {}
            for exp in suite.expectations:
                rule_type = getattr(exp, "expectation_type", None)
                kwargs = exp.configuration.kwargs if hasattr(exp, "configuration") else {}
                if not rule_type or "match_set" in rule_type: continue
                
                try:
                    res = getattr(validator, rule_type)(**kwargs, result_format={
                        "result_format": "COMPLETE",
                        "unexpected_index_column_names": [temp_id_col],
                        "include_unexpected_indices": True
                    })
                    if not res.success:
                        indices = res.result.get("unexpected_index_list", [])
                        for item in indices:
                            row_id = item.get(temp_id_col) if isinstance(item, dict) else item
                            current_errors.setdefault(row_id, []).append(f"[{kwargs.get('column')}] {rule_type}")
                except: pass
            
            errors_map = current_errors # 验证成功完成

        except Exception as e:
            print(f"❌ Batch {batch_id} {table_name} 验证引擎内部失败: {str(e)}")

    # --- 分流写入块 (在锁外执行，不阻塞其他表的计算) ---
    try:
        if errors_map is None:
            # 严重失败降级：全量入库，保证业务不中断
            df_with_id.drop(temp_id_col).write.format("delta").mode("append").saveAsTable(table_name)
        elif not errors_map:
            # 验证通过
            df_with_id.drop(temp_id_col).write.format("delta").mode("append").saveAsTable(table_name)
            print(f"✅ Batch {batch_id}: {table_name} 验证通过")
        else:
            # 存在脏数据，执行分流
            _execute_streaming_dispatch(df_with_id, errors_map, table_name, batch_id, temp_id_col)
    except Exception as e:
        print(f"❌ Batch {batch_id} {table_name} 写入失败: {str(e)}")
    finally:
        df_with_id.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
