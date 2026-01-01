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
import traceback
import json
import re
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_SHARED_GX_CONTEXT = None

def validate_and_insert_single_dataframe(df, table_name):

    global _SHARED_GX_CONTEXT
    if _SHARED_GX_CONTEXT is None:
        _SHARED_GX_CONTEXT = gx.get_context()
    context = _SHARED_GX_CONTEXT
    
    suite_name = f"{table_name}_suite"
    temp_id_col = "_temp_row_id"
    ds_name = f"ds_spark_{table_name}"
    
    # 1. 基础预处理
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower())

    # 显式加 ID 并缓存
    df_with_id = df.withColumn(temp_id_col, F.monotonically_increasing_id()).cache()
    _row_count = df_with_id.count() 

    try:
        # 2. 读取并强力解析 JSON
        suite_path = f"/lakehouse/default/Files/gx_config/expectations/{suite_name}.json"
        with open(suite_path, "r", encoding='utf-8') as f:
            content = f.read()
            if '""kwargs""' in content: content = content.replace('""', '"')
            import re
            json_match = re.search(r'(\{.*\}|\[.*\])', content.replace('\n', ' '))
            data = json.loads(json_match.group(1))
            raw_exps = data.get("expectations", data) if isinstance(data, dict) else data

        # 3. 初始化 Validator
        try: context.data_sources.delete(ds_name)
        except: pass
        datasource = context.data_sources.add_spark(name=ds_name)
        asset = datasource.add_dataframe_asset(name="active_asset")
        batch_request = asset.build_batch_request(options={"dataframe": df_with_id})
        validator = context.get_validator(batch_request=batch_request)

        print(f"🚀 {table_name}: 开始逐条执行验证 (总行数: {_row_count})")
        
        errors_map = {}
        valid_rule_count = 0

        for exp in raw_exps:
            # --- 兼容性提取规则名 ---
            rule_type = exp.get("expectation_type") or exp.get("expectation") or exp.get("type")
            kwargs = exp.get("kwargs", {})
            
            if not rule_type or "match_set" in rule_type:
                continue
            
            valid_rule_count += 1
            try:
                # 显式调用方法
                method = getattr(validator, rule_type)
                # 强制要求返回完整索引
                res = method(**kwargs, result_format={
                    "result_format": "COMPLETE",
                    "unexpected_index_column_names": [temp_id_col],
                    "include_unexpected_indices": True
                })
                
                if not res.success:
                    col = kwargs.get("column", "Unknown")
                    u_count = res.result.get("unexpected_count", 0)
                    print(f"❌ 字段 [{col}] 违反规则: {rule_type} (失败行数: {u_count})")
                    
                    indices = res.result.get("unexpected_index_list", [])
                    if indices:
                        msg = f"[{col}] {rule_type}"
                        for item in indices:
                            row_id = item.get(temp_id_col) if isinstance(item, dict) else item
                            if row_id is not None:
                                errors_map.setdefault(row_id, []).append(msg)
            except Exception as e:
                print(f"⚠️ 执行规则 {rule_type} 出错: {str(e)}")

        print(f"📊 已完成 {valid_rule_count} 条规则检查")

        # 4. 根据结果分流
        if not errors_map:
            print(f"✅ {table_name}: 验证完全通过")
            df_with_id.drop(temp_id_col).write.format("delta").mode("append").saveAsTable(table_name)
        else:
            _execute_dispatch(df_with_id, errors_map, table_name, temp_id_col)

    except Exception as e:
        print(f"❌ 运行报错: {str(e)}")
        traceback.print_exc()
    finally:
        df_with_id.unpersist()

def _execute_dispatch(df_with_id, errors_map, table_name, temp_id_col):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    bad_row_ids = list(errors_map.keys())
    cols_to_json = [c for c in df_with_id.columns if c != temp_id_col]
    
    # 构建错误信息表
    error_data = [(int(k), "; ".join(v)) for k, v in errors_map.items()]
    error_info_df = spark.createDataFrame(error_data, [temp_id_col, "violated_rules"])
    
    # 隔离脏数据
    bad_df = df_with_id.join(error_info_df, on=temp_id_col, how="inner") \
        .withColumn("raw_data", F.to_json(F.struct(*cols_to_json))) \
        .withColumn("table_name", F.lit(table_name)) \
        .withColumn("ingestion_time", F.current_timestamp()) \
        .select("table_name", "violated_rules", "raw_data", "ingestion_time")
    
    bad_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("data_quality_quarantine")
    
    # 写入好数据
    good_df = df_with_id.join(error_info_df, on=temp_id_col, how="left_anti").drop(temp_id_col)
    if good_df.count() > 0:
        good_df.write.format("delta").mode("append").saveAsTable(table_name)
    
    print(f"⚠️ {table_name}: 拦截 {bad_df.count()} 行，入库 {good_df.count()} 行")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
