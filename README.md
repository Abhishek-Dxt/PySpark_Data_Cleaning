In this project I'll be doing data cleaning using Spark (PySpark) on Google Colab environment in a Jupyter Notebook. It can be seen that we don't necessarily need a cluster for Spark Programming.

Spark is faster than MapReduce because it uses RDDs, which can cache data in memory, are optimized for iterative processing, and use a DAG engine to optimize task execution.

The fundamental building blocks and the core data structure of Apache Spark are RDDs. RDDs (Resilient Distributed Datasets) are immutable distributed collections of objects that can be processed in parallel across a cluster of machines. They are fault-tolerant and can recover from node failures.

# Creating a Spark session & setting up the Spark environment - -

#### Installing JDK -

!apt-get install openjdk- 8 -jdk-headless -qq > /dev/null

#### Spark Installer (using the link from spark.apache.org) -

!wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz

```
--2023-04-08 23:58:38-- https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
Resolving dlcdn.apache.org (dlcdn.apache.org)... 151.101.2.132, 2a04:4e42::
Connecting to dlcdn.apache.org (dlcdn.apache.org)|151.101.2.132|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 299360284 (285M) [application/x-gzip]
Saving to: ‘spark-3.3.2-bin-hadoop3.tgz.1’
```
```
spark-3.3.2-bin-had 100%[===================>] 285.49M 195MB/s in 1.5s
```
```
2023-04-08 23:58:40 (195 MB/s) - ‘spark-3.3.2-bin-hadoop3.tgz.1’ saved [299360284/299360284]
```
!tar -xvf spark-3.3.2-bin-hadoop3.tgz


```
spark3.3.2binhadoop3/sbin/startslaves.sh
spark-3.3.2-bin-hadoop3/sbin/start-thriftserver.sh
spark-3.3.2-bin-hadoop3/sbin/start-worker.sh
spark-3.3.2-bin-hadoop3/sbin/start-workers.sh
spark-3.3.2-bin-hadoop3/sbin/stop-all.sh
spark-3.3.2-bin-hadoop3/sbin/stop-history-server.sh
spark-3.3.2-bin-hadoop3/sbin/stop-master.sh
spark-3.3.2-bin-hadoop3/sbin/stop-mesos-dispatcher.sh
spark-3.3.2-bin-hadoop3/sbin/stop-mesos-shuffle-service.sh
spark-3.3.2-bin-hadoop3/sbin/stop-slave.sh
spark-3.3.2-bin-hadoop3/sbin/stop-slaves.sh
spark-3.3.2-bin-hadoop3/sbin/stop-thriftserver.sh
spark-3.3.2-bin-hadoop3/sbin/stop-worker.sh
spark-3.3.2-bin-hadoop3/sbin/stop-workers.sh
spark-3.3.2-bin-hadoop3/sbin/workers.sh
spark-3.3.2-bin-hadoop3/yarn/
spark-3.3.2-bin-hadoop3/yarn/spark-3.3.2-yarn-shuffle.jar
```

#### Setting up Environment Variable (JAVA-HOME and SPARK-HOME) Paths -

!ls /usr/lib/jvm

```
java-1.11.0-openjdk-amd64 java-1.8.0-openjdk-amd
java-11-openjdk-amd64 java-8-openjdk-amd
```
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

os.environ["SPARK_HOME"] = "/content/spark-3.3.2-bin-hadoop3"

#### Installing findspark

!pip install -q findspark

"findspark " is a Python library that provides a simple way to add Apache Spark to a Python environment. It allows users to locate and use a Spark installation on their computer, and then easily integrate Spark into their Python code. This library is particularly useful for those who want to work with Spark in a Jupyter notebook or other Python environment, without having to install and congure Spark separately.

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

!wget https://raw.githubusercontent.com/Abhishek-Dxt/PySpark_Scala/main/Consumer_data.csv

```
--2023-04-09 00:27:31-- https://raw.githubusercontent.com/Abhishek-Dxt/PySpark_Scala/main/Consumer_data.csv
Resolving raw.githubusercontent.com (raw.githubusercontent.com)... 185.199.110.133, 185.199.108.133, 185.199.109.133, ...
Connecting to raw.githubusercontent.com (raw.githubusercontent.com)|185.199.110.133|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 17552 (17K) [text/plain]
Saving to: ‘Consumer_data.csv’
```
```
Consumer_data.csv 100%[===================>] 17.14K --.-KB/s in 0.001s
```
```
2023-04-09 00:27:31 (14.7 MB/s) - ‘Consumer_data.csv’ saved [17552/17552]
```
!ls

```
Consumer_data.csv sample_data spark-3.3.2-bin-hadoop3.tgz
dataset_nm.csv spark-3.3.2-bin-hadoop3 spark-3.3.2-bin-hadoop3.tgz.
```
consumerDF = spark.read.csv("Consumer_data.csv",header=True)

consumerDF.show()

##### +----------+-----------+------+------+-------+

```
|CustomerID| Age|Salary|Gender|Country|
+----------+-----------+------+------+-------+
| 1| 18| 20000| Male|Germany|
| 2| 19| 22000|Female|Unknown|
| 3| null| 24000|Female|England|
| 4| 21| 2600| Male|England|
| 5| 22| 50000| Male| France|
| 6| null| 35000|Female|Unknown|
| 7| 24| 4300| Male|Germany|
| 8| 25| null|Female| France|
| 9| 35| 35000| Male|Germany|
| 10| 27| 37000|Female| France|
| 11| 31| 25000| Male|Germany|
| 12|32.38181818| 27000|Female|Unknown|
| 13|33.76363636| 29000|Female|England|
| 14|35.14545455| 7600| Male|England|
| 15|36.52727273| null| Male| France|
| 16|37.90909091| 40000|Female|England|
| 17|39.29090909| 9300| Male|Germany|
| 18|40.67272727| 37000|Female| France|
| 19|42.05454545| 40000| Male|Unknown|
| 20|43.43636364| 42000|Female| France|
+----------+-----------+------+------+-------+
only showing top 20 rows
```

It can be seen that the data has some countries as 'Unknown' and we have missing values in the Age & Salary columns. Let's handle these problems.

## Replacing rows with 'Unknown' country

consumerDF1 = consumerDF.filter(consumerDF['Country'] != "Unknown")
consumerDF1.show()

```
+----------+-----------+------+------+-------+
|CustomerID| Age|Salary|Gender|Country|
+----------+-----------+------+------+-------+
| 1| 18| 20000| Male|Germany|
| 3| null| 24000|Female|England|
| 4| 21| 2600| Male|England|
| 5| 22| 50000| Male| France|
| 7| 24| 4300| Male|Germany|
| 8| 25| null|Female| France|
| 9| 35| 35000| Male|Germany|
| 10| 27| 37000|Female| France|
| 11| 31| 25000| Male|Germany|
| 13|33.76363636| 29000|Female|England|
| 14|35.14545455| 7600| Male|England|
| 15|36.52727273| null| Male| France|
| 16|37.90909091| 40000|Female|England|
| 17|39.29090909| 9300| Male|Germany|
| 18|40.67272727| 37000|Female| France|
| 20|43.43636364| 42000|Female| France|
| 21|44.81818182| 30000|Female| France|
| 22| 46.2| 32000|Female| France|
| 23|47.58181818| 34000|Female| France|
| 24|48.96363636| 12600|Female|Germany|
+----------+-----------+------+------+-------+
only showing top 20 rows
```
## Replacing rows having null values with mean of respective columns -

consumerDF1.printSchema()

```
root
|-- CustomerID: string (nullable = true)
|-- Age: string (nullable = true)
|-- Salary: string (nullable = true)
|-- Gender: string (nullable = true)
|-- Country: string (nullable = true)
```

#### First I'll convert Age & Salary to integer & float type respectively.

from pyspark.sql.types import IntegerType,FloatType

consumerDF2 = consumerDF1.withColumn("Age", consumerDF1["Age"].cast(IntegerType())).withColumn("Salary", consumerDF1["Salary"].cast(FloatType

consumerDF2.printSchema()

```
root
|-- CustomerID: string (nullable = true)
|-- Age: integer (nullable = true)
|-- Salary: float (nullable = true)
|-- Gender: string (nullable = true)
|-- Country: string (nullable = true)
```
from pyspark.sql.functions import mean

mean_age = consumerDF2.select(mean(consumerDF2['Age'])).collect()
mean_age = mean_age[ 0 ][ 0 ]

mean_age

```
52.
```
mean_salary = consumerDF2.select(mean(consumerDF2['Salary'])).collect()
mean_salary = mean_salary[ 0 ][ 0 ]

mean_salary

```
35094.
```
Original data (null age and salary values can be seen) -

consumerDF2.show()


```
+----------+----+-------+------+-------+
|CustomerID| Age| Salary|Gender|Country|
+----------+----+-------+------+-------+
| 1| 18|20000.0| Male|Germany|
| 3|null|24000.0|Female|England|
| 4| 21| 2600.0| Male|England|
| 5| 22|50000.0| Male| France|
| 7| 24| 4300.0| Male|Germany|
| 8| 25| null|Female| France|
| 9| 35|35000.0| Male|Germany|
| 10| 27|37000.0|Female| France|
| 11| 31|25000.0| Male|Germany|
| 13| 33|29000.0|Female|England|
| 14| 35| 7600.0| Male|England|
| 15| 36| null| Male| France|
| 16| 37|40000.0|Female|England|
| 17| 39| 9300.0| Male|Germany|
| 18| 40|37000.0|Female| France|
| 20| 43|42000.0|Female| France|
| 21| 44|30000.0|Female| France|
| 22| 46|32000.0|Female| France|
| 23| 47|34000.0|Female| France|
| 24| 48|12600.0|Female|Germany|
+----------+----+-------+------+-------+
only showing top 20 rows
```
consumerDF3 = consumerDF2.na.fill(mean_age,["Age"])
consumerDF3 = consumerDF3.na.fill(mean_salary,["Salary"])
consumerDF3.show()

```
+----------+---+---------+------+-------+
|CustomerID|Age| Salary|Gender|Country|
+----------+---+---------+------+-------+
| 1| 18| 20000.0| Male|Germany|
| 3| 52| 24000.0|Female|England|
| 4| 21| 2600.0| Male|England|
| 5| 22| 50000.0| Male| France|
| 7| 24| 4300.0| Male|Germany|
```

```
| 8| 25|35094.836|Female| France|
| 9| 35| 35000.0| Male|Germany|
| 10| 27| 37000.0|Female| France|
| 11| 31| 25000.0| Male|Germany|
| 13| 33| 29000.0|Female|England|
| 14| 35| 7600.0| Male|England|
| 15| 36|35094.836| Male| France|
| 16| 37| 40000.0|Female|England|
| 17| 39| 9300.0| Male|Germany|
| 18| 40| 37000.0|Female| France|
| 20| 43| 42000.0|Female| France|
| 21| 44| 30000.0|Female| France|
| 22| 46| 32000.0|Female| France|
| 23| 47| 34000.0|Female| France|
| 24| 48| 12600.0|Female|Germany|
+----------+---+---------+------+-------+
only showing top 20 rows
```
## Saving the cleaned data

consumerDF3.write.format("csv").save("consumer_data_cleaned")

New file has been created - 

!ls

```
consumer_data_cleaned spark-3.3.2-bin-hadoop
Consumer_data.csv spark-3.3.2-bin-hadoop3.tgz
dataset_nm.csv spark-3.3.2-bin-hadoop3.tgz.
sample_data
```
We can see the part file -

!ls consumer_data_cleaned/

```
part-00000-a7e94f85-cb32-4fc3-a15e-da09a396129b-c000.csv _SUCCESS
```
New file can be easily exported -

!cat consumer_data_cleaned/part- 00000 -a7e94f85-cb32- 4 fc3-a15e-da09a396129b-c000.csv

```
1,18,20000.0,Male,Germany
3,52,24000.0,Female,England
4,21,2600.0,Male,England
5,22,50000.0,Male,France
7,24,4300.0,Male,Germany
8,25,35094.836,Female,France
9,35,35000.0,Male,Germany
10,27,37000.0,Female,France
11,31,25000.0,Male,Germany
13,33,29000.0,Female,England
14,35,7600.0,Male,England
15,36,35094.836,Male,France
16,37,40000.0,Female,England
17,39,9300.0,Male,Germany
18,40,37000.0,Female,France
20,43,42000.0,Female,France
21,44,30000.0,Female,France
22,46,32000.0,Female,France
23,47,34000.0,Female,France
24,48,12600.0,Female,Germany
25,50,60000.0,Female,Germany
26,51,45000.0,Female,Germany
27,53,14300.0,Male,Germany
28,54,42000.0,Male,Germany
29,52,45000.0,Male,England
30,57,47000.0,Male,England
31,58,35000.0,Male,England
32,60,37000.0,Male,England
33,61,39000.0,Male,England
34,62,17600.0,Male,England
35,64,65000.0,Male,England
36,65,50000.0,Female,England
37,66,19300.0,Female,England
38,18,29000.0,Male,England
39,19,7600.0,Male,England
40,20,35094.836,Female,Germany
41,21,40000.0,Male,Germany
42,22,9300.0,Female,Germany
43,23,37000.0,Male,England
```

# On GCP using Spark on Dataproc - 

Now, let's try to do the above cleaning, this time not on local notebook, but on a Dataproc cluster on GCP - 

First, I'll create a cluster and import the data from this Git repository using the SSH. I'll keep the uncleaned data in a 'rawdata' folder. The file is now stored on the HDFS.

<img width="941" alt="1_load" src="https://user-images.githubusercontent.com/71979171/230753282-317cf5ce-c4aa-45e9-b84d-94b2861c518b.PNG">


Now, I'll do the cleaning "transformation" on the data stored in HDFS using Spark. Let's login to the Pyspark shell & get into the Spark environment.

<img width="944" alt="2_pyspark" src="https://user-images.githubusercontent.com/71979171/230753453-489eaa0f-044c-4256-ad55-8cc0b68e841f.PNG">


Then, I'll proceed with performing the same data cleaning steps as I did on the PySpark-GoogleColab Notebook local setup and I should get the similar output of cleaned data - 

<img width="938" alt="3_cleaned" src="https://user-images.githubusercontent.com/71979171/230753796-39acea3a-71a1-4e2c-9243-26d73401cbc6.PNG">

Next, after saving the cleaned data, I will create a database using hive to save the cleaned data so that others may access the it.

<img width="946" alt="4_hivedb" src="https://user-images.githubusercontent.com/71979171/230754195-cba95a7a-3b03-478c-9c89-12cb29923a05.PNG">





