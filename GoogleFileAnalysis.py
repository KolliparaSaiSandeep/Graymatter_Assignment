# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

print("hello")

# COMMAND ----------

df_01=spark.read.csv("/FileStore/gmde/googleplaystore.csv")

# COMMAND ----------

df_01.show()

# COMMAND ----------

df_001=spark.read.csv("/FileStore/gmde/googleplaystore_user_reviews-1.csv")
df_001.show()

# COMMAND ----------

df_001.display()

# COMMAND ----------

df_01.printSchema()

# COMMAND ----------

df1=spark.read.option("header",True).csv("/FileStore/gmde/googleplaystore.csv")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df2=spark.read.option("header",True).option("inferschema",True).csv("/FileStore/gmde/googleplaystore_user_reviews.csv")

# COMMAND ----------

df2.show()

# COMMAND ----------

sch=StructType().add("App",StringType(),True)\
                .add("Category",StringType(),True)\
                .add("Rating",DoubleType(),True)\
                .add("Reviews",IntegerType(),True)\
                .add("Size",StringType(),True)\
                .add("Installs",StringType(),True)\
                .add("Type",StringType(),True)\
                .add("Price",StringType(),True)\
                .add("Content Rating",StringType(),True)\
                .add("Genres",StringType(),True)\
                .add("Last Updated",StringType(),True)\
                .add("Current Ver",StringType(),True)\
                .add("Android Ver",StringType(),True)

# COMMAND ----------

dfgoogleplaystore=spark.read.option("header",True).schema(sch).csv("/FileStore/gmde/googleplaystore.csv")

# COMMAND ----------

dfgoogleplaystore.display()

# COMMAND ----------

dfgoogleplaystore_newcol=dfgoogleplaystore.withColumn("newcol",col("Category"))
dfgoogleplaystore_newcol.display()

# COMMAND ----------

dfgoogleplaystore_newcol_agg=dfgoogleplaystore_newcol.withColumn("Appscore",col("Reviews")*col("Rating"))
dfgoogleplaystore_newcol_agg.display()

# COMMAND ----------

dfgoogleplaystore_newcol_lit=dfgoogleplaystore_newcol_agg.withColumn("name_const",lit("Harun"))
dfgoogleplaystore_newcol_lit.display()

# COMMAND ----------

dfgoogleplaystore_final=dfgoogleplaystore.withColumn("newcol",col("Category")).withColumn("Appscore",col("Reviews")*col("Rating")).withColumn("name_const",lit("Harun"))
dfgoogleplaystore_final.display()

# COMMAND ----------

df_select=dfgoogleplaystore_final.select("App","Rating")
df_select.display()

# COMMAND ----------

df_select=dfgoogleplaystore_final.selectExpr('cast(Rating as Integer) as int_Rating')
df_select.display() 

# COMMAND ----------

new_df_sort=dfgoogleplaystore_final.sort(col('Rating').desc())
new_df_sort.display()

# COMMAND ----------

new_df_orderby=dfgoogleplaystore_final.orderBy(col('Genres').desc())
new_df_orderby.display()

# COMMAND ----------

df_distinct=dfgoogleplaystore_final.distinct()
df_distinct.display()

# COMMAND ----------

dfgoogleplaystore_final.count()


# COMMAND ----------

dropdup=dfgoogleplaystore_final.dropDuplicates(["Category","Rating"])
dropdup.display()
dropdup.count()

# COMMAND ----------

nnew = dropdup.withColumn(
    "Rating value",
    when(col("Rating") >= 4.0, "Excellent")
    .when((col("Rating") >= 3.0) & (col("Rating") < 4.0), "Good")
    .otherwise(col("Rating")) 
)
nnew.display()

# COMMAND ----------

sch2=StructType().add("App",StringType(),True)\
                .add("Translated_Review",StringType(),True)\
                .add("Sentiment",StringType(),True)\
                .add("Sentiment_Polarity",DoubleType(),True)\
                .add("Sentiment_Subjectivity",DoubleType(),True)

dfgoogleplaystore_reviews=spark.read.option("header",True).schema(sch2).csv("/FileStore/gmde/googleplaystore_user_reviews.csv")



# COMMAND ----------

df_new=dfgoogleplaystore_final.join(dfgoogleplaystore_reviews,dfgoogleplaystore_final.App==dfgoogleplaystore_reviews.App,how="inner")
df_new.display()

# COMMAND ----------

df_new.printSchema()

# COMMAND ----------

#App column is ambiguity so select only required columns
df_new_final=dfgoogleplaystore_final.join(dfgoogleplaystore_reviews,dfgoogleplaystore_final.App==dfgoogleplaystore_reviews.App,how="inner").select(dfgoogleplaystore_final['*'],dfgoogleplaystore_reviews['Sentiment'])
df_new_final.display()

# COMMAND ----------

df_new_final.groupBy('App').avg('Rating').display()

# COMMAND ----------


