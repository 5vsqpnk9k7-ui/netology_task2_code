import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Пришлось манипуляции некоторые произвести, иначе спарк не хотел запускаться
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED" '
    'pyspark-shell'
)

# Решение задачи Task1
spark = SparkSession.builder \
    .appName("Task2") \
    .master("local[*]") \
    .getOrCreate()
df = spark.read.csv('covid-data.csv', header=True, inferSchema=True)
"""
1. Выберите 15 стран с наибольшим процентом переболевших на 31 марта 
(в выходящем датасете необходимы колонки: iso_code, страна, процент переболевших)

"""
df_task2_part1 = (df
    .filter((F.col('date') == '2021-03-31') & (F.col('continent').isNotNull()))
    .withColumn('процент переболевших', (F.col('total_cases') / F.col('population')) * 100)
    .select(
        F.col("iso_code"),
        F.col("location").alias('страна'),
        "процент переболевших"
    )
    .orderBy(F.col('процент переболевших').desc())
    .limit(15)
)
"""
2. Top 10 стран с максимальным зафиксированным кол-вом новых случаев за последнюю неделю марта 2021 в отсортированном порядке по убыванию
(в выходящем датасете необходимы колонки: число, страна, кол-во новых случаев)
"""
df_task2_part2 = (df
    .filter(F.col("continent").isNotNull())
    .filter(F.col("date").between("2021-03-25", "2021-03-31"))
    .select(F
            .col("date").alias("число"),
            F.col("location").alias("страна"),
            F.col("new_cases").alias("кол-во новых случаев")
    )
    .orderBy(F.col("кол-во новых случаев").desc())
    .limit(10)
)

"""
3. Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021. 
(например: в россии вчера было 9150 , сегодня 8763, итог: -387) (в выходящем датасете необходимы колонки: 
число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта).
"""
windowSpec = Window.partitionBy("location").orderBy("date")

df_task2_part3 = (df
    .filter(F.col("location") == "Russia")
    .filter(F.col("date").between("2021-03-24", "2021-03-31"))
    .withColumn("вчера", F.lag("new_cases").over(windowSpec))
    .withColumn("дельта", F.col("new_cases") - F.col("вчера"))
    .filter(F.col("date") >= "2021-03-25")
    .select(
        F.col("date").alias("число"),
        F.col("вчера").alias("кол-во новых случаев вчера"),
        F.col("new_cases").alias("кол-во новых случаев сегодня"),
        "дельта"
    )
    .orderBy("число")
)

df_task2_part1.show()
df_task2_part2.show()
df_task2_part3.show()
df_task2_part1.write.mode("overwrite").csv("df_task2_part1_result.csv")
df_task2_part2.write.mode("overwrite").csv("df_task2_part2_result.csv")
df_task2_part3.write.mode("overwrite").csv("df_task2_part3_result.csv")