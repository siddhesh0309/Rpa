from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, max as spark_max
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder.appName("QlikViewProcessing").getOrCreate()

# File path (Update with actual filename)
input_file = "qlikview_data.xlsx"  # Change this to your actual file

# Read Excel file using Pandas (since PySpark does not directly support reading Excel)
df_open_pd = pd.read_excel(input_file, sheet_name="Open")   # Reads Open sheet
df_closed_pd = pd.read_excel(input_file, sheet_name="Closed")  # Reads Closed sheet

# Convert Pandas DataFrames to PySpark DataFrames
df_open = spark.createDataFrame(df_open_pd)
df_closed = spark.createDataFrame(df_closed_pd)

# Merge Open and Closed data
df_merged = df_open.union(df_closed)

# Define required output columns (from your provided image)
output_columns = [
    "Customer Number", "Customer Name", "Lead Market", "Customer Type", "Review Reason",
    "Risk Rating", "Task Status", "Assigned to Role", "Initiated Date", "Overall Age",
    "Age Bucket", "Review Status", "Approval/Cancel Date", "Trigger Description",
    "Latest PAQC Status", "PT Bucket", "Cancellation Reason", "Cancellation Comments",
    "Final Status", "Final Date"
]

# Ensure all required columns exist
for col_name in output_columns:
    if col_name not in df_merged.columns:
        df_merged = df_merged.withColumn(col_name, lit(None))  # Add missing columns

# Step 4: Set "Final Status" = "Review Status"
df_merged = df_merged.withColumn("Final Status", col("Review Status"))

# Step 5: Assign "Final Date"
df_merged = df_merged.withColumn(
    "Final Date",
    when(col("Approval/Cancel Date").isNull(), col("Initiated Date"))
    .otherwise(col("Approval/Cancel Date"))
)

# Step 6: Keep only the latest record per "Customer Number"
df_latest = df_merged.groupBy("Customer Number").agg(
    spark_max("Final Date").alias("Final Date")
)

# Join back to get the latest records only
df_merged = df_merged.join(df_latest, ["Customer Number", "Final Date"], "inner")

# Step 7: Select only required columns
df_final = df_merged.select(output_columns)

# Step 8: Save the processed file (CSV & Excel options)
output_csv = f"merged_qlikview_{pd.Timestamp.today().strftime('%Y%m%d')}.csv"
output_xlsx = f"merged_qlikview_{pd.Timestamp.today().strftime('%Y%m%d')}.xlsx"

# Save as CSV
df_final.coalesce(1).write.csv(output_csv, header=True, mode="overwrite")

# Convert to Pandas for Excel saving (since PySpark cannot write Excel directly)
df_final_pd = df_final.toPandas()
df_final_pd.to_excel(output_xlsx, index=False, engine="openpyxl")

print(f"Processed files saved as:\n- {output_csv}\n- {output_xlsx}")

# Stop Spark session
spark.stop()
