import pyspark
import datetime
import time
import requests
import traceback
import bs4
import random
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

spark = pyspark.sql.SparkSession.builder.getOrCreate()

dt = datetime.datetime.now()
df = spark.read.json(f'/lake/red/korea_racing_authority/raceresult_1').drop("year")
df = df.na.replace('-', None)

df = df.withColumn("age", col("age").cast(ShortType()))
df = df.withColumn("ageCond", col("ageCond").cast(StringType()))
# df = df.drop("age")
# df = df.drop("ageCond")
df = df.withColumn("budam", col("budam").cast(StringType()))
df = df.withColumn("buga1", col("buga1").cast(LongType()))
df = df.withColumn("buga2", col("buga2").cast(LongType()))
df = df.withColumn("buga3", col("buga3").cast(LongType()))
df = df.withColumn("chaksun1", col("chaksun1").cast(LongType()))
df = df.withColumn("chaksun2", col("chaksun2").cast(LongType()))
df = df.withColumn("chaksun3", col("chaksun3").cast(LongType()))
df = df.withColumn("chaksun4", col("chaksun4").cast(LongType()))
df = df.withColumn("chaksun5", col("chaksun5").cast(LongType()))
df = df.withColumn("chulNo", col("chulNo").cast(StringType()))
df = df.withColumn("diffUnit", col("diffUnit").cast(StringType()))
df = df.withColumn("g2f", col("g2f").cast(StringType()))
df = df.withColumn("g3f_4c", col("g3f_4c").cast(StringType()))
df = df.withColumn("g4f_3c", col("g4f_3c").cast(StringType()))
df = df.withColumn("g6f_2c", col("g6f_2c").cast(StringType()))
df = df.withColumn("g8f_1c", col("g8f_1c").cast(StringType()))
df = df.withColumn("hrName", col("hrName").cast(StringType()))
df = df.withColumn("hrNo", col("hrNo").cast(StringType()))
df = df.withColumn("ilsu", col("ilsu").cast(ShortType()))
df = df.withColumn("jkName", col("jkName").cast(StringType()))
df = df.withColumn("jkNo", col("jkNo").cast(StringType()))
df = df.withColumn("meet", col("meet").cast(StringType()))
df = df.withColumn("name", col("name").cast(StringType()))
df = df.withColumn("ord", col("ord").cast(ShortType()))
df = df.withColumn("ordG1f", col("ordG1f").cast(ShortType()))
df = df.withColumn("ordS1f", col("ordS1f").cast(ShortType()))
df = df.withColumn("owName", col("owName").cast(StringType()))
df = df.withColumn("owNo", col("owNo").cast(StringType()))
df = df.withColumn("plcOdds", col("plcOdds").cast(DoubleType()))
df = df.withColumn("prizeCond", col("prizeCond").cast(StringType()))
df = df.withColumn("rank", col("rank").cast(StringType()))
df = df.withColumn("rating", col("rating").cast(ShortType()))
df = df.withColumn(
    "rcDate", 
    to_date(col("rcDate").cast("string"), "yyyyMMdd")
)
df = df.withColumn("rcDay", col("rcDay").cast(StringType()))
df = df.withColumn("rcDist", col("rcDist").cast(IntegerType()))
df = df.withColumn("rcName", col("rcName").cast(StringType()))
df = df.withColumn("rcNo", col("rcNo").cast(StringType()))
df = df.withColumn("rcTime", col("rcTime").cast(DoubleType()))
df = df.withColumn("rcTimeG1f", col("rcTimeG1f").cast(DoubleType()))
df = df.withColumn("rcTimeG2f", col("rcTimeG2f").cast(DoubleType()))
df = df.withColumn("rcTimeG3f", col("rcTimeG3f").cast(DoubleType()))
df = df.withColumn("rcTimeS1f", col("rcTimeS1f").cast(DoubleType()))
df = df.withColumn("rcTime_1c", col("rcTime_1c").cast(DoubleType()))
df = df.withColumn("rcTime_2c", col("rcTime_2c").cast(DoubleType()))
df = df.withColumn("rcTime_3c", col("rcTime_3c").cast(DoubleType()))
df = df.withColumn("rcTime_400", col("rcTime_400").cast(DoubleType()))
df = df.withColumn("rcTime_4c", col("rcTime_4c").cast(DoubleType()))
df = df.withColumn("sex", col("sex").cast(StringType()))
df = df.withColumn("trName", col("trName").cast(StringType()))
df = df.withColumn("trNo", col("trNo").cast(StringType()))
df = df.withColumn("track", col("track").cast(StringType()))
df = df.withColumn("weather", col("weather").cast(StringType()))
df = df.withColumn("wgBudam", col("wgBudam").cast(DoubleType()))
df = df.withColumn("wgHr", col("wgHr").cast(StringType()))
df = df.withColumn("winOdds", col("winOdds").cast(DoubleType()))

df = df.withColumnRenamed("age", "age")
df = df.withColumnRenamed("ageCond", "age_condition")
df = df.withColumnRenamed("budam", "carry_weight_sortation")
df = df.withColumnRenamed("buga1", "added_money_1")
df = df.withColumnRenamed("buga2", "added_money_2")
df = df.withColumnRenamed("buga3", "added_money_3")
df = df.withColumnRenamed("chaksun1", "first_prize_money")
df = df.withColumnRenamed("chaksun2", "second_prize_money")
df = df.withColumnRenamed("chaksun3", "third_prize_money")
df = df.withColumnRenamed("chaksun4", "fourth_prize_money")
df = df.withColumnRenamed("chaksun5", "fifth_prize_money")
df = df.withColumnRenamed("chulNo", "run_number")
df = df.withColumnRenamed("diffUnit", "margin")
df = df.withColumnRenamed("g2f", "g2f")
df = df.withColumnRenamed("g3f_4c", "g3f_4c")
df = df.withColumnRenamed("g4f_3c", "g4f_3c")
df = df.withColumnRenamed("g6f_2c", "g6f_2c")
df = df.withColumnRenamed("g8f_1c", "g8f_1c")
df = df.withColumnRenamed("hrName", "horse_name")
df = df.withColumnRenamed("hrNo", "horse_number")
df = df.withColumnRenamed("ilsu", "race_days")
df = df.withColumnRenamed("jkName", "jockey_name")
df = df.withColumnRenamed("jkNo", "jockey_number")
df = df.withColumnRenamed("meet", "meet")
df = df.withColumnRenamed("name", "nationality")
df = df.withColumnRenamed("ord", "order")
df = df.withColumnRenamed("ordG1f", "g1f_order")
df = df.withColumnRenamed("ordS1f", "s1f_order")
df = df.withColumnRenamed("owName", "owner_name")
df = df.withColumnRenamed("owNo", "owner_number")
df = df.withColumnRenamed("plcOdds", "quinella_odds")
df = df.withColumnRenamed("prizeCond", "prize_condition")
df = df.withColumnRenamed("rank", "class_condition")
df = df.withColumnRenamed("rating", "rating")
df = df.withColumnRenamed("rcDate", "racing_date")
df = df.withColumnRenamed("rcDay", "racing_day")
df = df.withColumnRenamed("rcDist", "racing_distance")
df = df.withColumnRenamed("rcName", "racing_name")
df = df.withColumnRenamed("rcNo", "racing_number")
df = df.withColumnRenamed("rcTime", "racing_time_record")
df = df.withColumnRenamed("rcTimeG1f", "G1f_racing_time_record")
df = df.withColumnRenamed("rcTimeG2f", "G2f_racing_time_record")
df = df.withColumnRenamed("rcTimeG3f", "G3f_racing_time_record")
df = df.withColumnRenamed("rcTimeS1f", "S1f_racing_time_record")
df = df.withColumnRenamed("rcTime_1c", "1c_racing_time_record")
df = df.withColumnRenamed("rcTime_2c", "2c_racing_time_record")
df = df.withColumnRenamed("rcTime_3c", "3c_racing_time_record")
df = df.withColumnRenamed("rcTime_400", "400_racing_time_record")
df = df.withColumnRenamed("rcTime_4c", "4c_racing_time_record")
df = df.withColumnRenamed("sex", "gender")
df = df.withColumnRenamed("trName", "trainer_name")
df = df.withColumnRenamed("trNo", "trainer_number")
df = df.withColumnRenamed("track", "track_condtion")
df = df.withColumnRenamed("weather", "weather_condition")
df = df.withColumnRenamed("wgBudam", "carry_weight")
df = df.withColumnRenamed("wgHr", "weight_of_horse")
df = df.withColumnRenamed("winOdds", "win_odds")


df2 = df.coalesce(5) 

df2.printSchema()
df2.show()

df2.write.mode('overwrite').option('compression', 'snappy').parquet(f'/lake/yellow/korea_racing_authority/raceresult_1')

# << raceresult_1 >>

# root
#  |-- age: long (nullable = true)              연령
#  |-- ageCond: string (nullable = true)        연령조건
#  |-- budam: string (nullable = true)          부담구분
#  |-- buga1: long (nullable = true)            부가상금1
#  |-- buga2: long (nullable = true)            부가상금2
#  |-- buga3: long (nullable = true)            부가상금3
#  |-- chaksun1: long (nullable = true)         1착상금
#  |-- chaksun2: long (nullable = true)         2착상금
#  |-- chaksun3: long (nullable = true)         3
#  |-- chaksun4: long (nullable = true)         4
#  |-- chaksun5: long (nullable = true)         5
#  |-- chulNo: string (nullable = true)         출주번호
#  |-- diffUnit: string (nullable = true)       착차
#  |-- g2f: long (nullable = true)              
#  |-- g3f_4c: long (nullable = true)
#  |-- g4f_3c: long (nullable = true)
#  |-- g6f_2c: long (nullable = true)
#  |-- g8f_1c: long (nullable = true)
#  |-- hrName: string (nullable = true)         마명
#  |-- hrNo: string (nullable = true)           마번
#  |-- ilsu: long (nullable = true)             일수
#  |-- jkName: string (nullable = true)         기수명
#  |-- jkNo: string (nullable = true)           기수번호
#  |-- meet: string (nullable = true)           시행경마장명
#  |-- name: string (nullable = true)           국적
#  |-- ord: long (nullable = true)              순위
#  |-- ordG1f: long (nullable = true)           g1f순위
#  |-- ordS1f: long (nullable = true)           s1f순위
#  |-- owName: string (nullable = true)         마주명
#  |-- owNo: string (nullable = true)           마주번호
#  |-- plcOdds: string (nullable = true)        복승식 배당율
#  |-- prizeCond: string (nullable = true)      경주조건
#  |-- rank: string (nullable = true)           등급조건
#  |-- rating: string (nullable = true)         레이팅(등급)
#  |-- rcDate: long (nullable = true)           경주일자     ### datetype
#  |-- rcDay: string (nullable = true)          경주요일
#  |-- rcDist: long (nullable = true)           경주거리
#  |-- rcName: string (nullable = true)         경주명
#  |-- rcNo: long (nullable = true)             경주번호     ### string
#  |-- rcTime: double (nullable = true)         경주기록
#  |-- rcTimeG1f: long (nullable = true)        g1f기록     ### double
#  |-- rcTimeG2f: long (nullable = true)        g2f기록     ### double
#  |-- rcTimeG3f: double (nullable = true)      g3f기록     ### double
#  |-- rcTimeS1f: long (nullable = true)        s1f기록     ### double
#  |-- rcTime_1c: double (nullable = true)      1c기록      ### double
#  |-- rcTime_2c: double (nullable = true)      2c기록      ### dobule
#  |-- rcTime_3c: double (nullable = true)      3c기록      ### double
#  |-- rcTime_400: long (nullable = true)       400(부경)   ### double
#  |-- rcTime_4c: double (nullable = true)      4c기록      ### double
#  |-- sex: string (nullable = true)            성별
#  |-- trName: string (nullable = true)         조교사명
#  |-- trNo: string (nullable = true)           조교사번호
#  |-- track: string (nullable = true)          주로
#  |-- weather: string (nullable = true)        날씨
#  |-- wgBudam: long (nullable = true)          부담중량
#  |-- wgHr: string (nullable = true)           마체중
#  |-- winOdds: string (nullable = true)        단승식 배당율  ### double