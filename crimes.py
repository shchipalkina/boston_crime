from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import percentile_approx

spark = SparkSession.builder.appName("csv").getOrCreate()

crimes = spark.read.csv(
        path="crime.csv",
        sep=",",
        header=True,
        quote='"',
        inferSchema=True
        )

codes = spark.read.csv(
        path="offense_codes.csv",
        sep=",",
        header=True,
        quote='"',
        inferSchema=True
        )

offense_codes_broadcast = broadcast(codes)

crimes_join_broadcast = crimes.join(offense_codes_broadcast, crimes.OFFENSE_CODE==offense_codes_broadcast.CODE)

crimes_join_broadcast.createOrReplaceTempView("crcodes")

crime_total = spark.sql("select DISTRICT, count(*) as CRIMES_TOTAL, avg(Lat) as LAT_AVG, avg(Long) as LONG_AVG from crcodes group by DISTRICT order by crimes_total desc")

crimes_monthly = spark.sql("select DISTRICT, percentile_approx(monthly_sum, 0.5) as MEDIANA from (select DISTRICT, YEAR, MONTH, count(INCIDENT_NUMBER) as monthly_sum from crcodes group by DISTRICT, YEAR, MONTH) group by DISTRICT")

frequent_crime_types = spark.sql("select DISTRICT, collect_set(C_T) as top_types from (select DISTRICT, C_T from (select DISTRICT, C_T, count(C_T) as cnt from (select DISTRICT, substring_index(NAME, '-', 1) AS C_T from crcodes) group by DISTRICT, C_T) order by cnt desc limit (3)) group by DISTRICT")

first_join = crime_total.join(crimes_monthly,['DISTRICT'],"outer")

result = first_join.join(frequent_crime_types,['DISTRICT'],"outer")

result.write.parquet("result.parquet")
