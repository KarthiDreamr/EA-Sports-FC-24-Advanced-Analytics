karthi@hiwatari-fedora:~$ docker run -v /home/karthi/Documents/Spark:/data -it spark /opt/spark/bin/spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/04/14 10:51:24 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://31378f165e27:4040
Spark context available as 'sc' (master = local[*], app id = local-1713091884700).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.1
      /_/
         
Using Scala version 2.12.18 (OpenJDK 64-Bit Server VM, Java 11.0.22)
Type in expressions to have them evaluated.
Type :help for more information.

scala> val df = spark.read.format("csv").option("header", "true").load("/data/BDA_File.csv")

df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> 
scala> df.show()
24/04/14 10:53:15 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.

+--------------+----------------+-------+---------+---+------------+-------------------+---------+-------------------+--------------+----------------+--------------+---------+-----------+------------------------+-------------+----+--------+-------+---------+---------+------+------------------+--------------------+-------------------+-----------------------+--------------------+-----------------+
|    short_name|player_positions|overall|potential|age|club_team_id|          club_name|league_id|        league_name|nationality_id|nationality_name|preferred_foot|weak_foot|skill_moves|international_reputation|    work_rate|pace|shooting|passing|dribbling|defending|physic|goalkeeping_diving|goalkeeping_handling|goalkeeping_kicking|goalkeeping_positioning|goalkeeping_reflexes|goalkeeping_speed|
+--------------+----------------+-------+---------+---+------------+-------------------+---------+-------------------+--------------+----------------+--------------+---------+-----------+------------------------+-------------+----+--------+-------+---------+---------+------+------------------+--------------------+-------------------+-----------------------+--------------------+-----------------+
|    K. MbappÃ©|          ST, LW|     91|       94| 24|          73|Paris Saint Germain|       16|            Ligue 1|            18|          France|         Right|        4|          5|                       5|     High/Low|  97|      90|     80|       92|       36|    78|                13|                   5|                  7|                     11|                   6|             NULL|
|    E. Haaland|              ST|     91|       94| 22|          10|    Manchester City|       13|     Premier League|            36|          Norway|          Left|        3|          3|                       5|  High/Medium|  89|      93|     66|       80|       45|    88|                 7|                  14|                 13|                     11|                   7|             NULL|
|  K. De Bruyne|         CM, CAM|     91|       91| 32|          10|    Manchester City|       13|     Premier League|             7|         Belgium|         Right|        5|          4|                       5|  High/Medium|  72|      88|     94|       87|       65|    78|                15|                  13|                  5|                     10|                  13|             NULL|
|      L. Messi|         CF, CAM|     90|       90| 36|      112893|        Inter Miami|       39|Major League Soccer|            52|       Argentina|          Left|        4|          4|                       5|      Low/Low|  80|      87|     90|       94|       33|    64|                 6|                  11|                 15|                     14|                   8|             NULL|
|    K. Benzema|          CF, ST|     90|       90| 35|         607|         Al Ittihad|      350|         Pro League|            18|          France|         Right|        4|          4|                       5|Medium/Medium|  79|      88|     83|       87|       39|    78|                13|                  11|                  5|                      5|                   7|             NULL|
|R. Lewandowski|              ST|     90|       90| 34|         241|       FC Barcelona|       53|            La Liga|            37|          Poland|         Right|        4|          4|                       5|  High/Medium|  75|      91|     80|       87|       44|    84|                15|                   6|                 12|                      8|                  10|             NULL|
|   T. Courtois|              GK|     90|       90| 31|         243|        Real Madrid|       53|            La Liga|             7|         Belgium|          Left|        3|          1|                       5|Medium/Medium|NULL|    NULL|   NULL|     NULL|     NULL|  NULL|                85|                  89|                 76|                     90|                  93|               46|
|       H. Kane|              ST|     90|       90| 29|          21| FC Bayern MÃ¼nchen|       19|         Bundesliga|            14|         England|         Right|        5|          3|                       5|    High/High|  69|      93|     84|       83|       49|    83|                 8|                  10|                 11|                     14|                  11|             NULL|
|      Vini Jr.|              LW|     89|       94| 22|         243|        Real Madrid|       53|            La Liga|            54|          Brazil|         Right|        4|          5|                       5|    High/High|  95|      82|     78|       90|       29|    68|                 5|                   7|                  7|                      7|                  10|             NULL|
|       Alisson|              GK|     89|       90| 30|           9|          Liverpool|       13|     Premier League|            54|          Brazil|         Right|        3|          1|                       5|Medium/Medium|NULL|    NULL|   NULL|     NULL|     NULL|  NULL|                86|                  85|                 85|                     90|                  89|               56|
|         Rodri|         CDM, CM|     89|       90| 27|          10|    Manchester City|       13|     Premier League|            45|           Spain|         Right|        4|          3|                       4|  Medium/High|  58|      73|     80|       80|       85|    84|                10|                  10|                  7|                     14|                   8|             NULL|
|   RÃºben Dias|              CB|     89|       90| 26|          10|    Manchester City|       13|     Premier League|            38|        Portugal|         Right|        4|          2|                       4|  Medium/High|  62|      39|     66|       69|       89|    87|                 7|                   8|                 13|                      7|                  12|             NULL|
|     Neymar Jr|              LW|     89|       89| 31|         605|           Al Hilal|      350|         Pro League|            54|          Brazil|         Right|        5|          5|                       5|  High/Medium|  86|      83|     85|       93|       37|    61|                 9|                   9|                 15|                     15|                  11|             NULL|
| M. ter Stegen|              GK|     89|       89| 31|         241|       FC Barcelona|       53|            La Liga|            21|         Germany|         Right|        4|          1|                       5|Medium/Medium|NULL|    NULL|   NULL|     NULL|     NULL|  NULL|                86|                  85|                 89|                     86|                  91|               47|
|      Casemiro|             CDM|     89|       89| 31|          11|  Manchester United|       13|     Premier League|            54|          Brazil|         Right|        3|          2|                       4|  Medium/High|  63|      75|     79|       73|       89|    88|                13|                  14|                 16|                     12|                  12|             NULL|
|   V. van Dijk|              CB|     89|       89| 31|           9|          Liverpool|       13|     Premier League|            34|     Netherlands|         Right|        3|          2|                       4|  Medium/High|  78|      60|     71|       72|       89|    86|                13|                  10|                 13|                     11|                  11|             NULL|
|      M. Salah|              RW|     89|       89| 31|           9|          Liverpool|       13|     Premier League|           111|           Egypt|          Left|        3|          4|                       5|  High/Medium|  89|      87|     81|       88|       45|    76|                14|                  14|                  9|                     11|                  14|             NULL|
|   F. Valverde|          CM, RW|     88|       92| 24|         243|        Real Madrid|       53|            La Liga|            60|         Uruguay|         Right|        4|          3|                       3|    High/High|  88|      82|     84|       84|       80|    82|                 6|                  10|                  6|                     15|                   8|             NULL|
|    V. Osimhen|              ST|     88|       91| 24|          48|             Napoli|       31|            Serie A|           133|         Nigeria|         Right|        4|          3|                       3|  High/Medium|  90|      86|     66|       83|       42|    82|                14|                  14|                 10|                      9|                   6|             NULL|
|       Ederson|              GK|     88|       89| 29|          10|    Manchester City|       13|     Premier League|            54|          Brazil|          Left|        3|          1|                       3|Medium/Medium|NULL|    NULL|   NULL|     NULL|     NULL|  NULL|                86|                  82|                 91|                     86|                  86|               64|
+--------------+----------------+-------+---------+---+------------+-------------------+---------+-------------------+--------------+----------------+--------------+---------+-----------+------------------------+-------------+----+--------+-------+---------+---------+------+------------------+--------------------+-------------------+-----------------------+--------------------+-----------------+
only showing top 20 rows


scala> 
scala> ^Z^Z^Z^Z                                                       

scala> df.dtypes.foreach(f => println(f._1 + ", " + f._2))
short_name, StringType
player_positions, StringType
overall, StringType
potential, StringType
age, StringType
club_team_id, StringType
club_name, StringType
league_id, StringType
league_name, StringType
nationality_id, StringType
nationality_name, StringType
preferred_foot, StringType
weak_foot, StringType
skill_moves, StringType
international_reputation, StringType
work_rate, StringType
pace, StringType
shooting, StringType
passing, StringType
dribbling, StringType
defending, StringType
physic, StringType
goalkeeping_diving, StringType
goalkeeping_handling, StringType
goalkeeping_kicking, StringType
goalkeeping_positioning, StringType
goalkeeping_reflexes, StringType
goalkeeping_speed, StringType

scala> 

scala> import org.apache.spark.sql.types.IntegerType

val df2 = df.withColumn("overall", df("overall").cast(IntegerType))
             .withColumn("potential", df("potential").cast(IntegerType))
             .withColumn("age", df("age").cast(IntegerType))
             .withColumn("club_team_id", df("club_team_id").cast(IntegerType))
             .withColumn("league_id", df("league_id").cast(IntegerType))
             .withColumn("nationality_id", df("nationality_id").cast(IntegerType))
             .withColumn("weak_foot", df("weak_foot").cast(IntegerType))
             .withColumn("skill_moves", df("skill_moves").cast(IntegerType))
             .withColumn("international_reputation", df("international_reputation").cast(IntegerType))
             .withColumn("pace", df("pace").cast(IntegerType))
             .withColumn("shooting", df("shooting").cast(IntegerType))
             .withColumn("passing", df("passing").cast(IntegerType))
             .withColumn("dribbling", df("dribbling").cast(IntegerType))
             .withColumn("defending", df("defending").cast(IntegerType))
             .withColumn("physic", df("physic").cast(IntegerType))
             .withColumn("goalkeeping_diving", df("goalkeeping_diving").cast(IntegerType))
             .withColumn("goalkeeping_handling", df("goalkeeping_handling").cast(IntegerType))
             .withColumn("goalkeeping_kicking", df("goalkeeping_kicking").cast(IntegerType))
             .withColumn("goalkeeping_positioning", df("goalkeeping_positioning").cast(IntegerType))
             .withColumn("goalkeeping_reflexes", df("goalkeeping_reflexes").cast(IntegerType))
             .withColumn("goalkeeping_speed", df("goalkeeping_speed").cast(IntegerType))
import org.apache.spark.sql.types.IntegerType

scala> 
scala> df2: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res2: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res3: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res4: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res5: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res6: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res7: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res8: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res9: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res10: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res11: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res12: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res13: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res14: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res15: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res16: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res17: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res18: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res19: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res20: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> res21: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> df.schema.fields.foreach(f => println(f.name + ", " + f.dataType))
short_name, StringType
player_positions, StringType
overall, StringType
potential, StringType
age, StringType
club_team_id, StringType
club_name, StringType
league_id, StringType
league_name, StringType
nationality_id, StringType
nationality_name, StringType
preferred_foot, StringType
weak_foot, StringType
skill_moves, StringType
international_reputation, StringType
work_rate, StringType
pace, StringType
shooting, StringType
passing, StringType
dribbling, StringType
defending, StringType
physic, StringType
goalkeeping_diving, StringType
goalkeeping_handling, StringType
goalkeeping_kicking, StringType
goalkeeping_positioning, StringType
goalkeeping_reflexes, StringType
goalkeeping_speed, StringType

scala> df2.schema.fields.foreach(f => println(f.name + ", " + f.dataType))
short_name, StringType
player_positions, StringType
overall, IntegerType
potential, StringType
age, StringType
club_team_id, StringType
club_name, StringType
league_id, StringType
league_name, StringType
nationality_id, StringType
nationality_name, StringType
preferred_foot, StringType
weak_foot, StringType
skill_moves, StringType
international_reputation, StringType
work_rate, StringType
pace, StringType
shooting, StringType
passing, StringType
dribbling, StringType
defending, StringType
physic, StringType
goalkeeping_diving, StringType
goalkeeping_handling, StringType
goalkeeping_kicking, StringType
goalkeeping_positioning, StringType
goalkeeping_reflexes, StringType
goalkeeping_speed, StringType

scala> val df2 = df.withColumn("club_team_id", df("club_team_id").cast(IntegerType))
df2: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> df2.schema.fields.foreach(f => println(f.name + ", " + f.dataType))
short_name, StringType
player_positions, StringType
overall, StringType
potential, StringType
age, StringType
club_team_id, IntegerType
club_name, StringType
league_id, StringType
league_name, StringType
nationality_id, StringType
nationality_name, StringType
preferred_foot, StringType
weak_foot, StringType
skill_moves, StringType
international_reputation, StringType
work_rate, StringType
pace, StringType
shooting, StringType
passing, StringType
dribbling, StringType
defending, StringType
physic, StringType
goalkeeping_diving, StringType
goalkeeping_handling, StringType
goalkeeping_kicking, StringType
goalkeeping_positioning, StringType
goalkeeping_reflexes, StringType
goalkeeping_speed, StringType

scala> 

scala> df2 = df.withColumn("overall", df("overall").cast(IntegerType))
df2.schema.fields.foreach(f => println(f.name + ", " + f.dataType))<console>:25: error: reassignment to val
       df2 = df.withColumn("overall", df("overall").cast(IntegerType))
           ^

scala> var fifa_df = df.withColumn("overall", df("overall").cast(IntegerType))
fifa_df.schema.fields.foreach(f => println(f.name + ", " + f.dataType))<console>:1: error: ';' expected but 'var' found.
       df2.schema.fields.foreach(f => println(f.name + ", " + f.dataType))var fifa_df = df.withColumn("overall", df("overall").cast(IntegerType))
                                                                          ^

scala> var fifa_df = df.withColumn("overall", df("overall").cast(IntegerType))
fifa_df.schema.fields.foreach(f => println(f.name + ", " + f.dataType))<console>:1: error: ';' expected but 'var' found.
       fifa_df.schema.fields.foreach(f => println(f.name + ", " + f.dataType))var fifa_df = df.withColumn("overall", df("overall").cast(IntegerType))
                                                                              ^

scala> var fifa_df = df.withColumn("overall", df("overall").cast(IntegerType))
<console>:1: error: ';' expected but 'var' found.
       fifa_df.schema.fields.foreach(f => println(f.name + ", " + f.dataType))var fifa_df = df.withColumn("overall", df("overall").cast(IntegerType))
                                                                              ^

scala> var fifa_df = df.withColumn("overall", df("overall").cast(IntegerType))
fifa_df.schema.fields.foreach(f => println(f.name + ", " + f.dataType))fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df.dtypes.foreach(f => println(f._1 + ", " + f._2))
<console>:1: error: ';' expected but '.' found.
       fifa_df.schema.fields.foreach(f => println(f.name + ", " + f.dataType))fifa_df.dtypes.foreach(f => println(f._1 + ", " + f._2))
                                                                                     ^

scala> fifa_df.dtypes.foreach(f => println(f._1 + ", " + f._2))
short_name, StringType
player_positions, StringType
overall, IntegerType
potential, StringType
age, StringType
club_team_id, StringType
club_name, StringType
league_id, StringType
league_name, StringType
nationality_id, StringType
nationality_name, StringType
preferred_foot, StringType
weak_foot, StringType
skill_moves, StringType
international_reputation, StringType
work_rate, StringType
pace, StringType
shooting, StringType
passing, StringType
dribbling, StringType
defending, StringType
physic, StringType
goalkeeping_diving, StringType
goalkeeping_handling, StringType
goalkeeping_kicking, StringType
goalkeeping_positioning, StringType
goalkeeping_reflexes, StringType
goalkeeping_speed, StringType

scala> Certainly! Here are the commands without the Scala code blocks:

```python
fifa_df = fifa_df.withColumn("potential", fifa_df("potential").cast(IntegerType))
fifa_df = fifa_df.withColumn("age", fifa_df("age").cast(IntegerType))
fifa_df = fifa_df.withColumn("club_team_id", fifa_df("club_team_id").cast(IntegerType))
fifa_df = fifa_df.withColumn("league_id", fifa_df("league_id").cast(IntegerType))
fifa_df = fifa_df.withColumn("nationality_id", fifa_df("nationality_id").cast(IntegerType))
fifa_df = fifa_df.withColumn("weak_foot", fifa_df("weak_foot").cast(IntegerType))
fifa_df = fifa_df.withColumn("skill_moves", fifa_df("skill_moves").cast(IntegerType))
fifa_df = fifa_df.withColumn("international_reputation", fifa_df("international_reputation").cast(IntegerType))
fifa_df = fifa_df.withColumn("pace", fifa_df("pace").cast(IntegerType))
fifa_df = fifa_df.withColumn("shooting", fifa_df("shooting").cast(IntegerType))
fifa_df = fifa_df.withColumn("passing", fifa_df("passing").cast(IntegerType))
fifa_df = fifa_df.withColumn("dribbling", fifa_df("dribbling").cast(IntegerType))
fifa_df = fifa_df.withColumn("defending", fifa_df("defending").cast(IntegerType))
fifa_df = fifa_df.withColumn("physic", fifa_df("physic").cast(IntegerType))
fifa_df = fifa_df.withColumn("goalkeeping_diving", fifa_df("goalkeeping_diving").cast(IntegerType))
fifa_df = fifa_df.withColumn("goalkeeping_handling", fifa_df("goalkeeping_handling").cast(IntegerType))
fifa_df = fifa_df.withColumn("goalkeeping_kicking", fifa_df("goalkeeping_kicking").cast(IntegerType))
fifa_df = fifa_df.withColumn("goalkeeping_positioning", fifa_df("goalkeeping_positioning").cast(IntegerType))
fifa_df = fifa_df.withColumn("goalkeeping_reflexes", fifa_df("goalkeeping_reflexes").cast(IntegerType))
fifa_df = fifa_df.withColumn("goalkeeping_speed", fifa_df("goalkeeping_speed").cast(IntegerType))
```     |      | <console>:3: error: empty quoted identifier
       ```python
       ^
<console>:3: error: unclosed quoted identifier
       ```python
         ^

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("potential", fifa_df("potential").cast(IntegerType))
fifa_df = fifa_df.withColumn("age", fifa_df("age").cast(IntegerType))
fifa_df = fifa_df.withColumn("club_team_id", fifa_df("club_team_id").cast(IntegerType))
fifa_df = fifa_df.withColumn("league_id", fifa_df("league_id").cast(IntegerType))
fifa_df = fifa_df.withColumn("nationality_id", fifa_df("nationality_id").cast(IntegerType))
fifa_df = fifa_df.withColumn("weak_foot", fifa_df("weak_foot").cast(IntegerType))
fifa_df = fifa_df.withColumn("skill_moves", fifa_df("skill_moves").cast(IntegerType))
fifa_df = fifa_df.withColumn("international_reputation", fifa_df("international_reputation").cast(IntegerType))
fifa_df = fifa_df.withColumn("pace", fifa_df("pace").cast(IntegerType))
fifa_df = fifa_df.withColumn("shooting", fifa_df("shooting").cast(IntegerType))
fifa_df = fifa_df.withColumn("passing", fifa_df("passing").cast(IntegerType))
fifa_df = fifa_df.withColumn("dribbling", fifa_df("dribbling").cast(IntegerType))
fifa_df = fifa_df.withColumn("defending", fifa_df("defending").cast(IntegerType))
fifa_df = fifa_df.withColumn("physic", fifa_df("physic").cast(IntegerType))
fifa_df = fifa_df.withColumn("goalkeeping_diving", fifa_df("goalkeeping_diving").cast(IntegerType))
fifa_df = fifa_df.withColumn("goalkeeping_handling", fifa_df("goalkeeping_handling").cast(IntegerType))
fifa_df = fifa_df.withColumn("goalkeeping_kicking", fifa_df("goalkeeping_kicking").cast(IntegerType))
fifa_df = fifa_df.withColumn("goalkeeping_positioning", fifa_df("goalkeeping_positioning").cast(IntegerType))
fifa_df = fifa_df.withColumn("goalkeeping_reflexes", fifa_df("goalkeeping_reflexes").cast(IntegerType))
fifa_df = fifa_df.withColumn("goalkeeping_speed", fifa_df("goalkeeping_speed").cast(IntegerType))<console>:1: error: empty quoted identifier
       ```fifa_df = fifa_df.withColumn("potential", fifa_df("potential").cast(IntegerType))
       ^
<console>:1: error: unclosed quoted identifier
       ```fifa_df = fifa_df.withColumn("potential", fifa_df("potential").cast(IntegerType))
         ^

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("potential", fifa_df("potential").cast(IntegerType))
<console>:24: error: value fifa_df is not a member of org.apache.spark.sql.DataFrame
       fifa_df = fifa_df.withColumn("goalkeeping_speed", fifa_df("goalkeeping_speed").cast(IntegerType))fifa_df = fifa_df.withColumn("potential", fifa_df("potential").cast(IntegerType))
                                                                                                        ^

scala> 

scala> fifa_df = fifa_df.withColumn("potential", fifa_df("potential").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("age", fifa_df("age").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("club_team_id", fifa_df("club_team_id").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("league_id", fifa_df("league_id").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("nationality_id", fifa_df("nationality_id").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("weak_foot", fifa_df("weak_foot").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("skill_moves", fifa_df("skill_moves").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("international_reputation", fifa_df("international_reputation").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("international_reputation", fifa_df("international_reputation").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("pace", fifa_df("pace").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("shooting", fifa_df("shooting").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("dribbling", fifa_df("dribbling").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("passing", fifa_df("passing").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("defending", fifa_df("defending").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("physic", fifa_df("physic").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("goalkeeping_diving", fifa_df("goalkeeping_diving").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("goalkeeping_handling", fifa_df("goalkeeping_handling").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("goalkeeping_handling", fifa_df("goalkeeping_handling").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("goalkeeping_positioning", fifa_df("goalkeeping_positioning").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("goalkeeping_reflexes", fifa_df("goalkeeping_reflexes").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df = fifa_df.withColumn("goalkeeping_speed", fifa_df("goalkeeping_speed").cast(IntegerType))
fifa_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 26 more fields]

scala> fifa_df.dtypes.foreach(f => println(f._1 + ", " + f._2))
short_name, StringType
player_positions, StringType
overall, IntegerType
potential, IntegerType
age, IntegerType
club_team_id, IntegerType
club_name, StringType
league_id, IntegerType
league_name, StringType
nationality_id, IntegerType
nationality_name, StringType
preferred_foot, StringType
weak_foot, IntegerType
skill_moves, IntegerType
international_reputation, IntegerType
work_rate, StringType
pace, IntegerType
shooting, IntegerType
passing, IntegerType
dribbling, IntegerType
defending, IntegerType
physic, IntegerType
goalkeeping_diving, IntegerType
goalkeeping_handling, IntegerType
goalkeeping_kicking, IntegerType
goalkeeping_positioning, IntegerType
goalkeeping_reflexes, IntegerType
goalkeeping_speed, IntegerType

scala> import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

scala> val versatility = udf((positions: String) => {
  val commas = positions.count(_ == ',')
  if (commas == 0) "not versatile"
  else if (commas == 1) "versatile"
  else "very versatile"
})     |      |      |      |      | 
versatility: org.apache.spark.sql.expressions.UserDefinedFunction = SparkUserDefinedFunction($Lambda$4231/0x000000084012f040@7cd98d63,StringType,List(Some(class[value[0]: string])),Some(class[value[0]: string]),None,true,true)

scala> // Create a new DataFrame with short_name and versatility
val new_df = fifa_df.withColumn("versatility", versatility(fifa_df("player_positions")))
                    .select("short_name", "versatility")
scala> new_df: org.apache.spark.sql.DataFrame = [short_name: string, player_positions: string ... 27 more fields]

scala> new_df.show(5)
<console>:1: error: ';' expected but '.' found.
       new_df                    .select("short_name", "versatility")new_df.show(5)
                                                                           ^

scala> new_df.show(15)   
+--------------+----------------+-------+---------+---+------------+-------------------+---------+-------------------+--------------+----------------+--------------+---------+-----------+------------------------+-------------+----+--------+-------+---------+---------+------+------------------+--------------------+-------------------+-----------------------+--------------------+-----------------+-------------+
|    short_name|player_positions|overall|potential|age|club_team_id|          club_name|league_id|        league_name|nationality_id|nationality_name|preferred_foot|weak_foot|skill_moves|international_reputation|    work_rate|pace|shooting|passing|dribbling|defending|physic|goalkeeping_diving|goalkeeping_handling|goalkeeping_kicking|goalkeeping_positioning|goalkeeping_reflexes|goalkeeping_speed|  versatility|
+--------------+----------------+-------+---------+---+------------+-------------------+---------+-------------------+--------------+----------------+--------------+---------+-----------+------------------------+-------------+----+--------+-------+---------+---------+------+------------------+--------------------+-------------------+-----------------------+--------------------+-----------------+-------------+
|    K. MbappÃ©|          ST, LW|     91|       94| 24|          73|Paris Saint Germain|       16|            Ligue 1|            18|          France|         Right|        4|          5|                       5|     High/Low|  97|      90|     80|       92|       36|    78|                13|                   5|                  7|                     11|                   6|             NULL|    versatile|
|    E. Haaland|              ST|     91|       94| 22|          10|    Manchester City|       13|     Premier League|            36|          Norway|          Left|        3|          3|                       5|  High/Medium|  89|      93|     66|       80|       45|    88|                 7|                  14|                 13|                     11|                   7|             NULL|not versatile|
|  K. De Bruyne|         CM, CAM|     91|       91| 32|          10|    Manchester City|       13|     Premier League|             7|         Belgium|         Right|        5|          4|                       5|  High/Medium|  72|      88|     94|       87|       65|    78|                15|                  13|                  5|                     10|                  13|             NULL|    versatile|
|      L. Messi|         CF, CAM|     90|       90| 36|      112893|        Inter Miami|       39|Major League Soccer|            52|       Argentina|          Left|        4|          4|                       5|      Low/Low|  80|      87|     90|       94|       33|    64|                 6|                  11|                 15|                     14|                   8|             NULL|    versatile|
|    K. Benzema|          CF, ST|     90|       90| 35|         607|         Al Ittihad|      350|         Pro League|            18|          France|         Right|        4|          4|                       5|Medium/Medium|  79|      88|     83|       87|       39|    78|                13|                  11|                  5|                      5|                   7|             NULL|    versatile|
|R. Lewandowski|              ST|     90|       90| 34|         241|       FC Barcelona|       53|            La Liga|            37|          Poland|         Right|        4|          4|                       5|  High/Medium|  75|      91|     80|       87|       44|    84|                15|                   6|                 12|                      8|                  10|             NULL|not versatile|
|   T. Courtois|              GK|     90|       90| 31|         243|        Real Madrid|       53|            La Liga|             7|         Belgium|          Left|        3|          1|                       5|Medium/Medium|NULL|    NULL|   NULL|     NULL|     NULL|  NULL|                85|                  89|                 76|                     90|                  93|               46|not versatile|
|       H. Kane|              ST|     90|       90| 29|          21| FC Bayern MÃ¼nchen|       19|         Bundesliga|            14|         England|         Right|        5|          3|                       5|    High/High|  69|      93|     84|       83|       49|    83|                 8|                  10|                 11|                     14|                  11|             NULL|not versatile|
|      Vini Jr.|              LW|     89|       94| 22|         243|        Real Madrid|       53|            La Liga|            54|          Brazil|         Right|        4|          5|                       5|    High/High|  95|      82|     78|       90|       29|    68|                 5|                   7|                  7|                      7|                  10|             NULL|not versatile|
|       Alisson|              GK|     89|       90| 30|           9|          Liverpool|       13|     Premier League|            54|          Brazil|         Right|        3|          1|                       5|Medium/Medium|NULL|    NULL|   NULL|     NULL|     NULL|  NULL|                86|                  85|                 85|                     90|                  89|               56|not versatile|
|         Rodri|         CDM, CM|     89|       90| 27|          10|    Manchester City|       13|     Premier League|            45|           Spain|         Right|        4|          3|                       4|  Medium/High|  58|      73|     80|       80|       85|    84|                10|                  10|                  7|                     14|                   8|             NULL|    versatile|
|   RÃºben Dias|              CB|     89|       90| 26|          10|    Manchester City|       13|     Premier League|            38|        Portugal|         Right|        4|          2|                       4|  Medium/High|  62|      39|     66|       69|       89|    87|                 7|                   8|                 13|                      7|                  12|             NULL|not versatile|
|     Neymar Jr|              LW|     89|       89| 31|         605|           Al Hilal|      350|         Pro League|            54|          Brazil|         Right|        5|          5|                       5|  High/Medium|  86|      83|     85|       93|       37|    61|                 9|                   9|                 15|                     15|                  11|             NULL|not versatile|
| M. ter Stegen|              GK|     89|       89| 31|         241|       FC Barcelona|       53|            La Liga|            21|         Germany|         Right|        4|          1|                       5|Medium/Medium|NULL|    NULL|   NULL|     NULL|     NULL|  NULL|                86|                  85|                 89|                     86|                  91|               47|not versatile|
|      Casemiro|             CDM|     89|       89| 31|          11|  Manchester United|       13|     Premier League|            54|          Brazil|         Right|        3|          2|                       4|  Medium/High|  63|      75|     79|       73|       89|    88|                13|                  14|                 16|                     12|                  12|             NULL|not versatile|
+--------------+----------------+-------+---------+---+------------+-------------------+---------+-------------------+--------------+----------------+--------------+---------+-----------+------------------------+-------------+----+--------+-------+---------+---------+------+------------------+--------------------+-------------------+-----------------------+--------------------+-----------------+-------------+
only showing top 15 rows


scala> val final_df = new_df.select("short_name", "versatility")
final_df: org.apache.spark.sql.DataFrame = [short_name: string, versatility: string]

scala> final_df.show()
+--------------+-------------+
|    short_name|  versatility|
+--------------+-------------+
|    K. MbappÃ©|    versatile|
|    E. Haaland|not versatile|
|  K. De Bruyne|    versatile|
|      L. Messi|    versatile|
|    K. Benzema|    versatile|
|R. Lewandowski|not versatile|
|   T. Courtois|not versatile|
|       H. Kane|not versatile|
|      Vini Jr.|not versatile|
|       Alisson|not versatile|
|         Rodri|    versatile|
|   RÃºben Dias|not versatile|
|     Neymar Jr|not versatile|
| M. ter Stegen|not versatile|
|      Casemiro|not versatile|
|   V. van Dijk|not versatile|
|      M. Salah|not versatile|
|   F. Valverde|    versatile|
|    V. Osimhen|not versatile|
|       Ederson|not versatile|
+--------------+-------------+
only showing top 20 rows


scala> final_df.coalesce(1)
        .write
        .option("header", "true")
        .csv("/path/to/your/file.csv")
res29: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [short_name: string, versatility: string]

scala> res30: org.apache.spark.sql.DataFrameWriter[org.apache.spark.sql.Row] = org.apache.spark.sql.DataFrameWriter@287d30ad

scala> res31: org.apache.spark.sql.DataFrameWriter[org.apache.spark.sql.Row] = org.apache.spark.sql.DataFrameWriter@287d30ad

scala> 24/04/14 11:48:39 ERROR FileOutputCommitter: Mkdirs failed to create file:/path/to/your/file.csv/_temporary/0
24/04/14 11:48:39 ERROR Executor: Exception in task 0.0 in stage 4.0 (TID 4)
java.io.IOException: Mkdirs failed to create file:/path/to/your/file.csv/_temporary/0/_temporary/attempt_202404141148395855329347615099816_0004_m_000000_4 (exists=false, cwd=file:/opt/spark/work-dir)
	at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:515)
	at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:500)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1195)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1175)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1064)
	at org.apache.spark.sql.execution.datasources.CodecStreams$.createOutputStream(CodecStreams.scala:81)
	at org.apache.spark.sql.execution.datasources.CodecStreams$.createOutputStreamWriter(CodecStreams.scala:92)
	at org.apache.spark.sql.execution.datasources.csv.CsvOutputWriter.<init>(CsvOutputWriter.scala:38)
	at org.apache.spark.sql.execution.datasources.csv.CSVFileFormat$$anon$1.newInstance(CSVFileFormat.scala:84)
	at org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter.newOutputWriter(FileFormatDataWriter.scala:161)
	at org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter.<init>(FileFormatDataWriter.scala:146)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:389)
	at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
	at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
	at org.apache.spark.scheduler.Task.run(Task.scala:141)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)
	at java.base/java.lang.Thread.run(Unknown Source)
24/04/14 11:48:39 WARN TaskSetManager: Lost task 0.0 in stage 4.0 (TID 4) (31378f165e27 executor driver): java.io.IOException: Mkdirs failed to create file:/path/to/your/file.csv/_temporary/0/_temporary/attempt_202404141148395855329347615099816_0004_m_000000_4 (exists=false, cwd=file:/opt/spark/work-dir)
	at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:515)
	at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:500)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1195)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1175)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1064)
	at org.apache.spark.sql.execution.datasources.CodecStreams$.createOutputStream(CodecStreams.scala:81)
	at org.apache.spark.sql.execution.datasources.CodecStreams$.createOutputStreamWriter(CodecStreams.scala:92)
	at org.apache.spark.sql.execution.datasources.csv.CsvOutputWriter.<init>(CsvOutputWriter.scala:38)
	at org.apache.spark.sql.execution.datasources.csv.CSVFileFormat$$anon$1.newInstance(CSVFileFormat.scala:84)
	at org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter.newOutputWriter(FileFormatDataWriter.scala:161)
	at org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter.<init>(FileFormatDataWriter.scala:146)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:389)
	at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
	at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
	at org.apache.spark.scheduler.Task.run(Task.scala:141)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)
	at java.base/java.lang.Thread.run(Unknown Source)

24/04/14 11:48:39 ERROR TaskSetManager: Task 0 in stage 4.0 failed 1 times; aborting job
24/04/14 11:48:39 ERROR FileFormatWriter: Aborting job 8df9fcf4-7255-4959-9b68-504309885fb9.
org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 4.0 failed 1 times, most recent failure: Lost task 0.0 in stage 4.0 (TID 4) (31378f165e27 executor driver): java.io.IOException: Mkdirs failed to create file:/path/to/your/file.csv/_temporary/0/_temporary/attempt_202404141148395855329347615099816_0004_m_000000_4 (exists=false, cwd=file:/opt/spark/work-dir)
	at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:515)
	at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:500)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1195)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1175)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1064)
	at org.apache.spark.sql.execution.datasources.CodecStreams$.createOutputStream(CodecStreams.scala:81)
	at org.apache.spark.sql.execution.datasources.CodecStreams$.createOutputStreamWriter(CodecStreams.scala:92)
	at org.apache.spark.sql.execution.datasources.csv.CsvOutputWriter.<init>(CsvOutputWriter.scala:38)
	at org.apache.spark.sql.execution.datasources.csv.CSVFileFormat$$anon$1.newInstance(CSVFileFormat.scala:84)
	at org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter.newOutputWriter(FileFormatDataWriter.scala:161)
	at org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter.<init>(FileFormatDataWriter.scala:146)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:389)
	at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
	at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
	at org.apache.spark.scheduler.Task.run(Task.scala:141)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)
	at java.base/java.lang.Thread.run(Unknown Source)

Driver stacktrace:
	at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2856)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2792)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2791)
	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2791)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1247)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1247)
	at scala.Option.foreach(Option.scala:407)
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1247)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:3060)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2994)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2983)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:989)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2398)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$executeWrite$4(FileFormatWriter.scala:307)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.writeAndCommit(FileFormatWriter.scala:271)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeWrite(FileFormatWriter.scala:304)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:190)
	at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:190)
	at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:113)
	at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:111)
	at org.apache.spark.sql.execution.command.DataWritingCommandExec.executeCollect(commands.scala:125)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:107)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:125)
	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:107)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:461)
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:76)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:461)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:32)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:437)
	at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:98)
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:85)
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:83)
	at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:142)
	at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:859)
	at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:388)
	at org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:361)
	at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:240)
	at org.apache.spark.sql.DataFrameWriter.csv(DataFrameWriter.scala:850)
	at $line116.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:28)
	at $line116.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:32)
	at $line116.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:34)
	at $line116.$read$$iw$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:36)
	at $line116.$read$$iw$$iw$$iw$$iw$$iw$$iw.<init>(<console>:38)
	at $line116.$read$$iw$$iw$$iw$$iw$$iw.<init>(<console>:40)
	at $line116.$read$$iw$$iw$$iw$$iw.<init>(<console>:42)
	at $line116.$read$$iw$$iw$$iw.<init>(<console>:44)
	at $line116.$read$$iw$$iw.<init>(<console>:46)
	at $line116.$read$$iw.<init>(<console>:48)
	at $line116.$read.<init>(<console>:50)
	at $line116.$read$.<init>(<console>:54)
	at $line116.$read$.<clinit>(<console>)
	at $line116.$eval$.$print$lzycompute(<console>:7)
	at $line116.$eval$.$print(<console>:6)
	at $line116.$eval.$print(<console>)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(Unknown Source)
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(Unknown Source)
	at java.base/java.lang.reflect.Method.invoke(Unknown Source)
	at scala.tools.nsc.interpreter.IMain$ReadEvalPrint.call(IMain.scala:747)
	at scala.tools.nsc.interpreter.IMain$Request.loadAndRun(IMain.scala:1020)
	at scala.tools.nsc.interpreter.IMain.$anonfun$interpret$1(IMain.scala:568)
	at scala.reflect.internal.util.ScalaClassLoader.asContext(ScalaClassLoader.scala:36)
	at scala.reflect.internal.util.ScalaClassLoader.asContext$(ScalaClassLoader.scala:116)
	at scala.reflect.internal.util.AbstractFileClassLoader.asContext(AbstractFileClassLoader.scala:41)
	at scala.tools.nsc.interpreter.IMain.loadAndRunReq$1(IMain.scala:567)
	at scala.tools.nsc.interpreter.IMain.interpret(IMain.scala:594)
	at scala.tools.nsc.interpreter.IMain.interpret(IMain.scala:564)
	at scala.tools.nsc.interpreter.ILoop.interpretStartingWith(ILoop.scala:865)
	at scala.tools.nsc.interpreter.ILoop.command(ILoop.scala:733)
	at scala.tools.nsc.interpreter.ILoop.processLine(ILoop.scala:435)
	at scala.tools.nsc.interpreter.ILoop.loop(ILoop.scala:456)
	at org.apache.spark.repl.SparkILoop.process(SparkILoop.scala:239)
	at org.apache.spark.repl.Main$.doMain(Main.scala:78)
	at org.apache.spark.repl.Main$.main(Main.scala:58)
	at org.apache.spark.repl.Main.main(Main.scala)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(Unknown Source)
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(Unknown Source)
	at java.base/java.lang.reflect.Method.invoke(Unknown Source)
	at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
	at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:1029)
	at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:194)
	at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:217)
	at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:91)
	at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1120)
	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1129)
	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
Caused by: java.io.IOException: Mkdirs failed to create file:/path/to/your/file.csv/_temporary/0/_temporary/attempt_202404141148395855329347615099816_0004_m_000000_4 (exists=false, cwd=file:/opt/spark/work-dir)
	at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:515)
	at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:500)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1195)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1175)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1064)
	at org.apache.spark.sql.execution.datasources.CodecStreams$.createOutputStream(CodecStreams.scala:81)
	at org.apache.spark.sql.execution.datasources.CodecStreams$.createOutputStreamWriter(CodecStreams.scala:92)
	at org.apache.spark.sql.execution.datasources.csv.CsvOutputWriter.<init>(CsvOutputWriter.scala:38)
	at org.apache.spark.sql.execution.datasources.csv.CSVFileFormat$$anon$1.newInstance(CSVFileFormat.scala:84)
	at org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter.newOutputWriter(FileFormatDataWriter.scala:161)
	at org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter.<init>(FileFormatDataWriter.scala:146)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:389)
	at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
	at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
	at org.apache.spark.scheduler.Task.run(Task.scala:141)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)
	at java.base/java.lang.Thread.run(Unknown Source)
org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 4.0 failed 1 times, most recent failure: Lost task 0.0 in stage 4.0 (TID 4) (31378f165e27 executor driver): java.io.IOException: Mkdirs failed to create file:/path/to/your/file.csv/_temporary/0/_temporary/attempt_202404141148395855329347615099816_0004_m_000000_4 (exists=false, cwd=file:/opt/spark/work-dir)
	at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:515)
	at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:500)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1195)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1175)
	at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1064)
	at org.apache.spark.sql.execution.datasources.CodecStreams$.createOutputStream(CodecStreams.scala:81)
	at org.apache.spark.sql.execution.datasources.CodecStreams$.createOutputStreamWriter(CodecStreams.scala:92)
	at org.apache.spark.sql.execution.datasources.csv.CsvOutputWriter.<init>(CsvOutputWriter.scala:38)
	at org.apache.spark.sql.execution.datasources.csv.CSVFileFormat$$anon$1.newInstance(CSVFileFormat.scala:84)
	at org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter.newOutputWriter(FileFormatDataWriter.scala:161)
	at org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter.<init>(FileFormatDataWriter.scala:146)
	at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:389)
	at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
	at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
	at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
	at org.apache.spark.scheduler.Task.run(Task.scala:141)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
	at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)
	at java.base/java.lang.Thread.run(Unknown Source)

Driver stacktrace:
  at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2856)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2792)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2791)
  at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
  at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
  at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
  at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2791)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1247)
  at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1247)
  at scala.Option.foreach(Option.scala:407)
  at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1247)
  at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:3060)
  at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2994)
  at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2983)
  at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
  at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:989)
  at org.apache.spark.SparkContext.runJob(SparkContext.scala:2398)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.$anonfun$executeWrite$4(FileFormatWriter.scala:307)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.writeAndCommit(FileFormatWriter.scala:271)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeWrite(FileFormatWriter.scala:304)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.write(FileFormatWriter.scala:190)
  at org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand.run(InsertIntoHadoopFsRelationCommand.scala:190)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult$lzycompute(commands.scala:113)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.sideEffectResult(commands.scala:111)
  at org.apache.spark.sql.execution.command.DataWritingCommandExec.executeCollect(commands.scala:125)
  at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:107)
  at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:125)
  at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:201)
  at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:108)
  at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
  at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:66)
  at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:107)
  at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
  at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:461)
  at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(origin.scala:76)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:461)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:32)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
  at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
  at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:32)
  at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:437)
  at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:98)
  at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:85)
  at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:83)
  at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:142)
  at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:859)
  at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:388)
  at org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:361)
  at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:240)
  at org.apache.spark.sql.DataFrameWriter.csv(DataFrameWriter.scala:850)
  ... 49 elided
Caused by: java.io.IOException: Mkdirs failed to create file:/path/to/your/file.csv/_temporary/0/_temporary/attempt_202404141148395855329347615099816_0004_m_000000_4 (exists=false, cwd=file:/opt/spark/work-dir)
  at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:515)
  at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:500)
  at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1195)
  at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1175)
  at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:1064)
  at org.apache.spark.sql.execution.datasources.CodecStreams$.createOutputStream(CodecStreams.scala:81)
  at org.apache.spark.sql.execution.datasources.CodecStreams$.createOutputStreamWriter(CodecStreams.scala:92)
  at org.apache.spark.sql.execution.datasources.csv.CsvOutputWriter.<init>(CsvOutputWriter.scala:38)
  at org.apache.spark.sql.execution.datasources.csv.CSVFileFormat$$anon$1.newInstance(CSVFileFormat.scala:84)
  at org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter.newOutputWriter(FileFormatDataWriter.scala:161)
  at org.apache.spark.sql.execution.datasources.SingleDirectoryDataWriter.<init>(FileFormatDataWriter.scala:146)
  at org.apache.spark.sql.execution.datasources.FileFormatWriter$.executeTask(FileFormatWriter.scala:389)
  at org.apache.spark.sql.execution.datasources.WriteFilesExec.$anonfun$doExecuteWrite$1(WriteFiles.scala:100)
  at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:893)
  at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:893)
  at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
  at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:367)
  at org.apache.spark.rdd.RDD.iterator(RDD.scala:331)
  at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)
  at org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:166)
  at org.apache.spark.scheduler.Task.run(Task.scala:141)
  at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:620)
  at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally(SparkErrorUtils.scala:64)
  at org.apache.spark.util.SparkErrorUtils.tryWithSafeFinally$(SparkErrorUtils.scala:61)
  at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:94)
  at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:623)
  at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(Unknown Source)
  at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(Unknown Source)
  at java.base/java.lang.Thread.run(Unknown Source)

scala> final_df.coalesce(1)
        .write
        .option("header", "true")
        .csv("/data/file.csv")res33: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [short_name: string, versatility: string]

scala> res34: org.apache.spark.sql.DataFrameWriter[org.apache.spark.sql.Row] = org.apache.spark.sql.DataFrameWriter@194b1e32

scala> res35: org.apache.spark.sql.DataFrameWriter[org.apache.spark.sql.Row] = org.apache.spark.sql.DataFrameWriter@194b1e32

scala> 

