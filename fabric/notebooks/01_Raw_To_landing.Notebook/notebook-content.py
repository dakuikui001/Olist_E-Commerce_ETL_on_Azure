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

file_name = 'default'
processing_date = 'default'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
account_name = "dataprojectsforhuilu"
container_name = "olist-project"
relative_source_path = "raw"
relative_target_path = 'landing'
source_path = f'abfss://{container_name}@{account_name}.dfs.core.windows.net/{relative_source_path}'
target_path = f'abfss://{container_name}@{account_name}.dfs.core.windows.net/{relative_target_path}'
print(f'source_path: {source_path}\ntarget_path: {target_path}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from pyspark.sql.functions import lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit
def proprecessing_dataset(source_file, target_file, file_name):
    df = spark.read.format('csv').option('header', 'true')\
                                 .option('inferSchema', 'true')\
                                 .option("multiLine", "true")\
                                 .option("escape", '"')\
                                 .option("quote", '"')\
                                 .load(source_file)
    df_new = df.withColumn('processing_date', lit(processing_date))
    df_new.write.format('csv').option('header','true')\
                .partitionBy('processing_date')\
                .mode('append')\
                .save(target_file)
    print(f'successfully loaded the file : {file_name}')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_file = os.path.join(source_path, file_name)
new_file_name = '_'.join(file_name.split('_')[:-1])
target_file = os.path.join(target_path, new_file_name)
proprecessing_dataset(source_file, target_file, new_file_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.rm(source_file)
print(f'successfully delete the source file : {source_file}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
