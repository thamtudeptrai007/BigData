from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, round, expr, desc, sum, count, explode, isnan
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, ArrayType
from update_top import update
# Khởi tạo một phiên Spark
spark = SparkSession.builder \
    .appName("Process JSON Data") \
    .getOrCreate()

category_element_schema = StructType([
    StructField("name", StringType(), True)
])

category_schema = ArrayType(category_element_schema)

your_json_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("goal", DoubleType(), True),
    StructField("pledged", DoubleType(), True),
    StructField("percent_funded", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("launched_at", IntegerType(), True),
    StructField("deadline", IntegerType(), True),
    StructField("backers_count", StringType(), False),
    StructField("web", StringType(), True),
    StructField("category", category_schema, True),
    StructField("timestamp", TimestampType(), False)
])

# Đường dẫn đến tập tin JSON
json_file_path = "C:/Users/admin/Desktop/BigData/spark_process/data.json"

# Đọc dữ liệu từ tập tin JSON vào DataFrame
json_data = spark.read.schema(your_json_schema).json(json_file_path)

# Chọn các trường cho phép
allowed_fields = ["id", "name", "goal", "pledged", "percent_funded", "country", "launched_at", "deadline",
                  "backers_count", "web", "category", "timestamp"]

selected_data = json_data.select(*allowed_fields)

# Hiển thị schema của DataFrame
json_data.printSchema()

# Tiến hành xử lý dữ liệu
processDF = json_data.filter(size("category") > 0).filter(col("country") != "").filter(~isnan(col("name"))).\
        filter(col("goal") != 0.0).withColumn("backers_count", col("backers_count").cast("integer")).\
        withColumn("percent_funded", round(expr("pledged / goal"), 1)).na.fill({"backers_count" : 0})
processDF = processDF.withColumn("pledged", round("pledged", 1)).withColumn("goal", round("goal", 1))
processDF = processDF.orderBy(desc("timestamp")).dropDuplicates(["name"])
processDF.drop("timestamp")
#list_project and category
categoryCountDF = processDF.drop("timestamp")
categoryCountDF = categoryCountDF.withColumn("category", explode("category").alias("category")).drop("timestamp")
categoryCountDF = categoryCountDF.withColumn("category", col("category.name").cast("string"))



categoryCountDF.toPandas().to_csv("C:/Users/admin/Desktop/BigData/spark_process/output/project.csv", index=False)
#category
categoryCountDF = processDF.groupby("category").agg(count("*").alias("count"), sum("pledged").alias("pledged"),
                                                 sum("backers_count").alias("backers_count"))

top_20_category = categoryCountDF.withColumn("category", explode("category").alias("category")).drop("timestamp")
top_20_category = top_20_category.withColumn("category", col("category.name").cast("string"))
top_20_category = top_20_category.orderBy("pledged", ascending=False).limit(20)
top_20_category.toPandas().to_csv("C:/Users/admin/Desktop/BigData/spark_process/output/top_20_category.csv", index=False)

#country
countryCountDF = processDF.groupby("country").agg(count("*").alias("count"),
                                               sum("backers_count").alias("backers_count"),
                                               sum("pledged").alias("pledged"))
top_20_coutry = countryCountDF.orderBy("count", ascending=False).limit(20)
top_20_coutry.toPandas().to_csv("C:/Users/admin/Desktop/BigData/spark_process/output/top_20_country.csv", index=False)


update()

# Dừng phiên Spark
spark.stop()