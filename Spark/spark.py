from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
import pandas as pd
spark = SparkSession.builder.appName("test").getOrCreate()
# 合併資料
df = spark.read.csv('A_lvr_land_A.csv', header=True, inferSchema=True)
df = df.union(spark.read.csv('B_lvr_land_A.csv', header=True, inferSchema=True)) 
df = df.union(spark.read.csv('E_lvr_land_A.csv', header=True, inferSchema=True))
df = df.union(spark.read.csv('F_lvr_land_A.csv', header=True, inferSchema=True))
df = df.union(spark.read.csv('H_lvr_land_A.csv', header=True, inferSchema=True))
#篩選資料
df = df.filter(df['主要用途'].contains('住家用')).filter(df['建物型態'].contains('住宅大樓')).filter(~df['總樓層數'].isin(['一層','二層','三層','四層','五層','六層','七層','八層','九層','十層','十一層','十二層']))
df = df.sort(desc('交易年月日'))

# def digital_conversion(n): 將中文轉換數字
#     number_chinese = {
#         '一': 1,
#         '二': 2,
#         '三': 3,
#         '四': 4,
#         '五': 5,
#         '六': 6,
#         '七': 7,
#         '八': 8,
#         '九': 9,
#         '十': 10,
#     }
#     if len(n) == 1 :
#         return number_chinese[n]
#     if len(n) == 2 :
#         return 10 + number_chinese[n[1]]
#     else :
#         return number_chinese[n[0]] * 10 + number_chinese[n[-1]]
# print(digital_conversion('二十一'))

# def chinese_to_date(x): # 將民國轉換成西元
#     return str(int(x)+19110000)[0:4]+'-' + str(int(x)+19110000)[4:6] + '-' + str(int(x)+19110000)[6:8]
# print(chinese_to_date('1080501'))

# df = df.toPandas() 轉成pandas 輸出json但格式不對
# df.to_json('aa.json', orient='table', force_ascii=False)
