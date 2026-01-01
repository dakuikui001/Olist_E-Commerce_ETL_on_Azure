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
from great_expectations.core import ExpectationSuite
import great_expectations.expectations as gxe

import os
import shutil
import great_expectations as gx

gx_root_dir = "/lakehouse/default/Files/gx_config"

# 检查 yml 是否为空或不存在
yml_path = os.path.join(gx_root_dir, "great_expectations.yml")

if os.path.exists(yml_path):
    # 如果文件大小为 0，说明它是坏的，直接删掉整个目录重建
    if os.path.getsize(yml_path) == 0:
        print("检测到损坏的空配置文件，正在清理并重建...")
        shutil.rmtree(gx_root_dir)

# 重新运行初始化
# GX 发现目录不存在时，会自动创建完整的模板文件
context = gx.get_context(context_root_dir=gx_root_dir)
print(f"Context 重新初始化成功: {gx_root_dir}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with open(f"{gx_root_dir}/great_expectations.yml", "r") as f:
    print(f"File content: '{f.read()}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 定义表和对应的 Suite 名
tables_to_validate = [
    {"table": "customer_bz", "suite": "customer_bz_suite"},
    {"table": "geolocation_bz", "suite": "geolocation_bz_suite"},
    {"table": "order_bz", "suite": "order_bz_suite"},
    {"table": "order_item_bz", "suite": "order_item_bz_suite"},
    {"table": "payment_bz", "suite": "payment_bz_suite"},
    {"table": "product_bz", "suite": "product_bz_suite"},
    {"table": "product_category_bz", "suite": "product_category_bz_suite"},
    {"table": "review_bz", "suite": "review_bz_suite"},
    {"table": "seller_bz", "suite": "seller_bz_suite"},    
]

for item in tables_to_validate:
    table_name = item["table"]
    suite_name = item["suite"]
    
    # 1.10+ 获取或创建 Suite 的标准写法
    try:
        # 使用 .suites.get 获取
        suite = context.suites.get(name=suite_name)
        print(f"✅ 已加载现有 Suite: {suite_name}")
    except Exception:
        # 使用 .suites.add 和 gx.ExpectationSuite 创建
        # 这会自动在 Files/gx_config/expectations/ 下生成对应的 .json 文件
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
        print(f"✨ 已成功新建并持久化 Suite: {suite_name}")

print("\n所有 Suite 初始化完成。")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_rules_mapping = {
    "customer_bz": [
        # 2. 必填项检查
        gxe.ExpectColumnValuesToNotBeNull(column="customer_id"),

        # 3. 邮编长度检查 (注意：这里列名必须和上面 column_set 里的 prefix 保持一致)
        gxe.ExpectColumnValueLengthsToBeBetween(
            column="customer_zip_code_prefix", # 修正为 prefix
            min_value=4, 
            max_value=8
        )
    ],
    "order_bz": [
        gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="customer_id"),
        gxe.ExpectColumnValuesToBeInSet(column="order_status", value_set=["processing", "delivered", "canceled", "shipped", "invoiced", "unavailable"]),
        gxe.ExpectColumnValuesToNotBeNull(column="order_purchase_timestamp")
    ],
    "payment_bz": [
        gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
        gxe.ExpectColumnValuesToBeBetween(column="payment_value", min_value=0),
        gxe.ExpectColumnValuesToBeInSet(column="payment_type", value_set=["boleto", "credit_card", "voucher", "debit_card"])
    ],
    "product_bz": [

        gxe.ExpectColumnValuesToNotBeNull(column="product_id"),
        gxe.ExpectColumnValuesToBeBetween(column="product_weight_g", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="product_length_cm", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="product_height_cm", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="product_width_cm", min_value=0)
    ],
    "order_item_bz": [

        gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="product_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="seller_id"),
        gxe.ExpectColumnValuesToBeBetween(column="price", min_value=0),
        gxe.ExpectColumnValuesToBeBetween(column="freight_value", min_value=0)        
    ],   
    "seller_bz": [
 
        gxe.ExpectColumnValuesToNotBeNull(column="seller_id"),
        gxe.ExpectColumnValueLengthsToBeBetween(column="seller_zip_code_prefix", min_value=4, max_value=8)     
    ],  
    "review_bz": [

        gxe.ExpectColumnValuesToNotBeNull(column="review_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
        gxe.ExpectColumnValuesToBeBetween(column="review_score", min_value=1, max_value = 5)
    ], 

    "geolocation_bz": [

        gxe.ExpectColumnValuesToNotBeNull(column="geolocation_zip_code_prefix"),
        gxe.ExpectColumnValueLengthsToBeBetween(column="geolocation_zip_code_prefix", min_value=4, max_value=8)
    ], 

    "product_category_bz": [

        gxe.ExpectColumnValuesToNotBeNull(column="product_category_name"),
        gxe.ExpectColumnValuesToNotBeNull(column="product_category_name_english")
    ] 
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def initialize_all_suites(context, rules_mapping):
    for table_name, expectations in rules_mapping.items():
        suite_name = f"{table_name}_suite"
        
        # 1. 获取或创建 Suite
        try:
            suite = context.suites.get(name=suite_name)
            suite.expectations = [] 
            print(f"🔄 更新现有 Suite: {suite_name}")
        except Exception:
            suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
            print(f"✨ 创建新 Suite: {suite_name}")

        # 2. 为该表添加定义的每一条规则
        for exp in expectations:
            # add_expectation 在 1.x 中会自动去重（如果规则完全一样）
            suite.add_expectation(exp)
        
        print(f"   已添加 {len(expectations)} 条规则到 {suite_name}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 执行初始化
initialize_all_suites(context, table_rules_mapping)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
