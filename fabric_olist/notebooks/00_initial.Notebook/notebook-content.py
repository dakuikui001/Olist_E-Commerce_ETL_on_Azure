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

class SetupBronzeHelper():
    def __init__(self):
        self.initialized = True
        self.bronze = "abfss://olist_project@onelake.dfs.fabric.microsoft.com/olist_LH.Lakehouse/Files/medallion/bronze"

    def create_customer_bz(self):
        if(self.initialized):
            print(f"Creating customer_bz table...", end='')
            spark.sql(f'''CREATE TABLE IF NOT EXISTS customer_bz(
                customer_id string,
                customer_unique_id string,
                customer_zip_code_prefix string,
                customer_city string,
                customer_state string,
                first_name string,
                last_name string,
                full_name string,
                gender string,
                date_of_birth date,
                age integer,
                load_time timestamp,
                source_file string
                )
                USING DELTA
                LOCATION '{self.bronze}/customer_bz/'
            ''')
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    def create_geolocation_bz(self):
        if(self.initialized):
            print(f"Creating geolocation_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS geolocation_bz(
                geolocation_zip_code_prefix string,
                geolocation_lat integer,
                geolocation_lng integer,
                geolocation_city string,
                geolocation_state string,
                load_time timestamp,
                source_file string
                )
                USING DELTA
                LOCATION '{self.bronze}/geolocation_bz'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_product_bz(self):
        if(self.initialized):
            print(f"Creating product_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS product_bz(
                product_id string,
                product_category_name string,
                product_name_lenght integer,
                product_description_lenght integer,
                product_photos_qty integer,
                product_weight_g integer,
                product_length_cm integer,
                product_height_cm integer,
                product_width_cm integer,
                load_time timestamp,
                source_file string
                )
                USING DELTA
                LOCATION '{self.bronze}/product_bz'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_seller_bz(self):
        if(self.initialized):
            print(f"Creating seller_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS seller_bz(
                seller_id string,
                seller_zip_code_prefix string,
                seller_city string,
                seller_state string,
                load_time timestamp,
                source_file string
                )
                USING DELTA
                LOCATION '{self.bronze}/seller_bz'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_product_category_bz(self):
        if(self.initialized):
            print(f"Creating product_category_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS product_category_bz(
                product_category_name string,
                product_category_name_english string,
                load_time timestamp,
                source_file string
                )
                USING DELTA
                LOCATION '{self.bronze}/product_category_bz'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_order_item_bz(self):
        if(self.initialized):
            print(f"Creating order_item_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS order_item_bz(
                order_id string,
                order_item_id string,
                product_id string,
                seller_id string,
                shipping_limit_date TIMESTAMP,
                price DOUBLE,
                freight_value DOUBLE,
                order_purchase_timestamp TIMESTAMP,
                load_time timestamp,
                source_file string
                )
                USING DELTA
                LOCATION '{self.bronze}/order_item_bz'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_payment_bz(self):
        if(self.initialized):
            print(f"Creating payment_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS payment_bz(
                order_id string,
                payment_sequential INTEGER,
                payment_type string,
                payment_installments INTEGER,
                payment_value double,
                order_purchase_timestamp TIMESTAMP,
                load_time timestamp,
                source_file string
                )
                USING DELTA
                LOCATION '{self.bronze}/payment_bz'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    def create_review_bz(self):
        if(self.initialized):
            print(f"Creating review_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS review_bz(
                review_id string,
                order_id string,
                review_score INTEGER,
                review_comment_title string,
                review_comment_message string,
                review_creation_date TIMESTAMP,
                review_answer_timestamp TIMESTAMP,
                order_purchase_timestamp TIMESTAMP,
                load_time timestamp,
                source_file string
                )
                USING DELTA
                LOCATION '{self.bronze}/review_bz'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")


    def create_order_bz(self):
        if(self.initialized):
            print(f"Creating order_bz table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS order_bz(
                order_id string,
                customer_id string,
                order_status string,
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP,
                load_time timestamp,
                source_file string
                )
                USING DELTA
                LOCATION '{self.bronze}/order_bz'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

 
    def setup(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup ...")
        self.create_customer_bz()
        self.create_geolocation_bz()
        self.create_product_bz()
        self.create_seller_bz()
        self.create_product_category_bz()
        self.create_order_item_bz()
        self.create_payment_bz()
        self.create_review_bz()
        self.create_order_bz()
        print(f"Setup completed in {int(time.time()) - start} seconds")

    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        self.assert_table("customer_bz")
        self.assert_table("geolocation_bz")
        self.assert_table("product_bz")
        self.assert_table("seller_bz")
        self.assert_table("product_category_bz")
        self.assert_table("order_item_bz")
        self.assert_table("payment_bz")
        self.assert_table("review_bz")
        self.assert_table("order_bz")
        print(f"Setup validation completed in {int(time.time()) - start} seconds")

    def assert_table(self, table_name):
        assert spark.sql(f"SHOW TABLES") \
            .filter(f"isTemporary == false and tableName == '{table_name}'") \
            .count() == 1, f"The table {table_name} is missing"
        print(f"Found {table_name} table: Success")

    def cleanup(self):

        print(f"Deleting Tables from Catalog...", end='')
        spark.sql("USE dbo")
        tables = spark.catalog.listTables("dbo")
        
        for table in tables:
            spark.sql(f"DROP TABLE IF EXISTS dbo.{table.name}")
        
        spark.catalog.clearCache() 
            
        print("Done")

        # 2. 删除各层文件夹
        folders_to_delete = {
            "Bronze": "Files/medallion/bronze/",
            "Silver": "Files/medallion/silver/",
            "Gold": "Files/medallion/gold/",
            "Errors (JSON)": "Files/errors/",
            "Quarantine Data": "Files/data_quality/data_quality_quarantine/" 
        }

        for label, path in folders_to_delete.items():
            try:
                print(f"Deleting {label} Folder ({path})...", end='')
                # 增加检查，防止文件夹不存在时报错
                mssparkutils.fs.rm(path, True)
                print("Done")
            except Exception as e:
                print(f"Skipped (Not found or error)")

        # 3. 特别清理：如果隔离表是作为托管表创建的
        # 有时候 DROP TABLE 不一定会立即删除底层文件，可以强制清理默认仓库路径
        print("Finalizing cleanup...", end='')
        spark.sql("CLEAR CACHE") 
        print("Done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

setupBronzeHelper = SetupBronzeHelper()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

setupBronzeHelper.cleanup()
setupBronzeHelper.setup()
setupBronzeHelper.validate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS data_quality_quarantine;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS data_quality_quarantine (
# MAGIC     table_name STRING,
# MAGIC     batch_id LONG,
# MAGIC     violated_rules STRING,
# MAGIC     raw_data STRING,
# MAGIC     ingestion_time TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://olist_project@onelake.dfs.fabric.microsoft.com/olist_LH.Lakehouse/Files/data_quality/';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run great_expectations_single_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_schema = '''
                customer_id string,
                customer_unique_id string,
                customer_zip_code_prefix string,
                customer_city string,
                customer_state string,
                first_name string,
                last_name string,
                full_name string,
                gender string,
                date_of_birth date,
                age integer
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_df = spark.read.format('csv').option('header', 'true').schema(customer_schema)\
                        .load('Files/landing/olist_customers_dataset.csv')
customer_df = customer_df.withColumn('load_time', current_timestamp())\
                         .withColumn("source_file", col("_metadata.file_path"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validate_and_insert_single_dataframe(customer_df, "customer_bz")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

geolocation_schema = '''
                geolocation_zip_code_prefix string,
                geolocation_lat integer,
                geolocation_lng integer,
                geolocation_city string,
                geolocation_state string
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

geolocation_df = spark.read.format('csv').option('header', 'true').schema(geolocation_schema)\
                        .load('Files/landing/olist_geolocation_dataset.csv')
geolocation_df = geolocation_df.withColumn('load_time', current_timestamp())\
                         .withColumn("source_file", col("_metadata.file_path"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validate_and_insert_single_dataframe(geolocation_df, "geolocation_bz")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

product_schema = '''
                product_id string,
                product_category_name string,
                product_name_lenght integer,
                product_description_lenght integer,
                product_photos_qty integer,
                product_weight_g integer,
                product_length_cm integer,
                product_height_cm integer,
                product_width_cm integer
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

product_df = spark.read.format('csv').option('header', 'true').schema(product_schema)\
                        .load('Files/landing/olist_products_dataset.csv')
product_df = product_df.withColumn('load_time', current_timestamp())\
                         .withColumn("source_file", col("_metadata.file_path"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validate_and_insert_single_dataframe(product_df, "product_bz")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

seller_schema = '''
                seller_id string,
                seller_zip_code_prefix string,
                seller_city string,
                seller_state string
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

seller_df = spark.read.format('csv').option('header', 'true').schema(seller_schema)\
                        .load('Files/landing/olist_sellers_dataset.csv')
seller_df = seller_df.withColumn('load_time', current_timestamp())\
                         .withColumn("source_file", col("_metadata.file_path"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validate_and_insert_single_dataframe(seller_df, "seller_bz")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

product_category_schema = '''
                product_category_name string,
                product_category_name_english string
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

product_category_df = spark.read.format('csv').option('header', 'true').schema(product_category_schema)\
                        .load('Files/landing/product_category_name_translation.csv')
product_category_df = product_category_df.withColumn('load_time', current_timestamp())\
                         .withColumn("source_file", col("_metadata.file_path"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validate_and_insert_single_dataframe(product_category_df, "product_category_bz")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class SetupSilverHelper():
    def __init__(self):
        self.initialized = True
        self.silver = "abfss://olist_project@onelake.dfs.fabric.microsoft.com/olist_LH.Lakehouse/Files/medallion/silver"
 
    def create_order_item_sl(self):
        if(self.initialized):
            print(f"Creating order_item_sl table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS order_item_sl(
                order_id string,
                order_item_id string,
                product_id string,
                seller_id string,
                shipping_limit_date TIMESTAMP,
                price DOUBLE,
                freight_value DOUBLE,
                order_purchase_timestamp TIMESTAMP,
                load_time timestamp,
                source_file string,
                update_time TIMESTAMP
                )
                USING DELTA
                LOCATION '{self.silver}/order_item_sl'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_payment_sl(self):
        if(self.initialized):
            print(f"Creating payment_sl table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS payment_sl(
                order_id string,
                payment_sequential INTEGER,
                payment_type string,
                payment_installments INTEGER,
                payment_value double,
                order_purchase_timestamp TIMESTAMP,
                load_time timestamp,
                source_file string,
                update_time TIMESTAMP
                )
                USING DELTA
                LOCATION '{self.silver}/payment_sl'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    
    def create_review_sl(self):
        if(self.initialized):
            print(f"Creating review_sl table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS review_sl(
                review_id string,
                order_id string,
                review_score INTEGER,
                review_comment_title string,
                review_comment_message string,
                review_creation_date TIMESTAMP,
                review_answer_timestamp TIMESTAMP,
                order_purchase_timestamp TIMESTAMP,
                load_time timestamp,
                source_file string,
                update_time TIMESTAMP
                )
                USING DELTA
                LOCATION '{self.silver}/review_sl'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")


    def create_order_sl(self):
        if(self.initialized):
            print(f"Creating order_sl table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS order_sl(
                order_id string,
                customer_id string,
                order_status string,
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP,
                load_time timestamp,
                source_file string,
                update_time TIMESTAMP
                )
                USING DELTA
                LOCATION '{self.silver}/order_sl'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

 
    def setup(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup ...")
        self.create_order_item_sl()
        self.create_payment_sl()
        self.create_review_sl()
        self.create_order_sl()
        print(f"Setup completed in {int(time.time()) - start} seconds")

    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        self.assert_table("order_item_sl")
        self.assert_table("payment_sl")
        self.assert_table("review_sl")
        self.assert_table("order_sl")
        print(f"Setup validation completed in {int(time.time()) - start} seconds")

    def assert_table(self, table_name):
        assert spark.sql(f"SHOW TABLES") \
            .filter(f"isTemporary == false and tableName == '{table_name}'") \
            .count() == 1, f"The table {table_name} is missing"
        print(f"Found {table_name} table: Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

setupSilverHelper = SetupSilverHelper()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

setupSilverHelper.setup()
setupSilverHelper.validate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class SetupGoldHelper():
    def __init__(self):
        self.initialized = True
        self.gold = "abfss://olist_project@onelake.dfs.fabric.microsoft.com/olist_LH.Lakehouse/Files/medallion/gold"
 
    def create_order_item_gl(self):
        if(self.initialized):
            print(f"Creating order_item_gl table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS order_item_gl(
                order_id string,
                order_item_id string,
                product_id string,
                seller_id string,
                shipping_limit_date TIMESTAMP,
                price DOUBLE,
                freight_value DOUBLE,
                order_purchase_timestamp TIMESTAMP,
                load_time timestamp,
                source_file string,
                update_time TIMESTAMP,
                order_purchase_date date,
                order_purchase_time string,
                total_value DOUBLE
                )
                USING DELTA
                LOCATION '{self.gold}/order_item_gl'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

    def create_payment_gl(self):
        if(self.initialized):
            print(f"Creating payment_gl table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS payment_gl(
                order_id string,
                payment_sequential INTEGER,
                payment_type string,
                payment_installments INTEGER,
                payment_value double,
                order_purchase_timestamp TIMESTAMP,
                load_time timestamp,
                source_file string,
                update_time TIMESTAMP,
                order_purchase_date date,
                order_purchase_time string
                )
                USING DELTA
                LOCATION '{self.gold}/payment_gl'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")
    

    def create_order_gl(self):
        if(self.initialized):
            print(f"Creating order_gl table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS order_gl(
                order_id string,
                customer_id string,
                order_status string,
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP,
                load_time timestamp,
                source_file string,
                update_time TIMESTAMP,
                order_purchase_date date,
                order_purchase_time string,
                delivery_duration integer
                )
                USING DELTA
                LOCATION '{self.gold}/order_gl'
            """)
            print("Done")
        else:
            raise ReferenceError("Application database is not defined. Cannot create table in default database.")

 
    def setup(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup ...")
        self.create_order_item_gl()
        self.create_payment_gl()
        self.create_order_gl()
        print(f"Setup completed in {int(time.time()) - start} seconds")

    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        self.assert_table("order_item_gl")
        self.assert_table("payment_gl")
        self.assert_table("order_gl")
        print(f"Setup validation completed in {int(time.time()) - start} seconds")

    def assert_table(self, table_name):
        assert spark.sql(f"SHOW TABLES") \
            .filter(f"isTemporary == false and tableName == '{table_name}'") \
            .count() == 1, f"The table {table_name} is missing"
        print(f"Found {table_name} table: Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

setupGoldHelper = SetupGoldHelper()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

setupGoldHelper.setup()
setupGoldHelper.validate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
