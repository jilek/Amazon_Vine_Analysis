# Amazon_Vine_Analysis


```
import os
# Find the latest version of spark 2.0  from http://www-us.apache.org/dist/spark/ and enter as the spark version
# For example:
# spark_version = 'spark-3.0.0'
#spark_version = 'spark-3.0.1'
spark_version = 'spark-3.1.2'

os.environ['SPARK_VERSION']=spark_version

# Install Spark and Java
!apt-get update
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!wget -q http://www-us.apache.org/dist/spark/$SPARK_VERSION/$SPARK_VERSION-bin-hadoop2.7.tgz
!tar xf $SPARK_VERSION-bin-hadoop2.7.tgz
!pip install -q findspark

# Set Environment Variables
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = f"/content/{spark_version}-bin-hadoop2.7"

# Start a SparkSession
import findspark
findspark.init()
```


```
# Download the Postgres driver that will allow Spark to interact with Postgres.
!wget https://jdbc.postgresql.org/download/postgresql-42.2.16.jar
```


```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("BigData-Challenge")\
    .config("spark.driver.extraClassPath","/content/postgresql-42.2.16.jar")\
    .getOrCreate()
```


```
from pyspark import SparkFiles
url="https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Furniture_v1_00.tsv.gz"
spark.sparkContext.addFile(url)
df = spark.read.option("encoding", "UTF-8")\
      .csv(SparkFiles.get(""), sep="\t", header=True, inferSchema=True)
df.show(10)
```

Figure X - Furniture Dataframe read from S3 bucket URL (00:00:00)
![furniture_dataframe.png](Images/furniture_dataframe.png)

```
# Create the customers_table DataFrame
# customers_df = df.groupby("").agg({""}).withColumnRenamed("", "customer_count")
from pyspark.sql.functions import count
customers_df = df.groupby("customer_id")\
                .agg({"*": "count"})\
                .withColumnRenamed("count(1)", "customer_count")
customers_df.show(10)
```

Figure X - customers_df

![customers_df.png](Images/customers_df.png)

```
# Create the products_table DataFrame and drop duplicates.
products_df = df.select(['product_id', 'product_title']).drop_duplicates()
products_df.show(10)
```

![products_df.png](Images/products_df.png)

```
from pyspark.sql.functions import to_date

# Create the review_id_table DataFrame.
# Convert the 'review_date' column to a date datatype with to_date("review_date", 'yyyy-MM-dd').alias("review_date")
review_id_df = df.select(
                  [
                    'review_id',
                    'customer_id',
                    'product_id',
                    'product_parent',
                    to_date("review_date", 'yyyy-MM-dd').alias("review_date")
                  ]
                )
review_id_df.show(10)
```

![review_id_df.png](Images/review_id_df.png)

```
# Create the vine_table. DataFrame
vine_df = df.select(['review_id', 'star_rating', 'helpful_votes', 'total_votes', 'vine', 'verified_purchase'])
vine_df.show(10)
```

![vine_df.png](Images/vine_df.png)

```
CREATE TABLE review_id_table (
  review_id TEXT PRIMARY KEY NOT NULL,
  customer_id INTEGER,
  product_id TEXT,
  product_parent INTEGER,
  review_date DATE -- this should be in the formate yyyy-mm-dd
);

-- This table will contain only unique values
CREATE TABLE products_table (
  product_id TEXT PRIMARY KEY NOT NULL UNIQUE,
  product_title TEXT
);

-- Customer table for first data set
CREATE TABLE customers_table (
  customer_id INT PRIMARY KEY NOT NULL UNIQUE,
  customer_count INT
);

-- vine table
CREATE TABLE vine_table (
  review_id TEXT PRIMARY KEY,
  star_rating INTEGER,
  helpful_votes INTEGER,
  total_votes INTEGER,
  vine TEXT,
  verified_purchase TEXT
);
```

```
# Configure settings for RDS
from getpass import getpass
password = getpass('Enter database password')

mode = "append"

jdbc_url="jdbc:postgresql://dataviz2.cscg8rwqauuq.us-east-2.rds.amazonaws.com:5432/amazon_review3_db"

config = {"user":"postgres",
          "password": password,
          "driver":"org.postgresql.Driver"}
```

```
# Write review_id_df to table in RDS
review_id_df.write.jdbc(url=jdbc_url, table='review_id_table', mode=mode, properties=config)
```

```
select * from review_id_table limit(5);
```

![pgadmin_review_id_table.png](Images/pgadmin_review_id_table.png)

```
# Write products_df to table in RDS
products_df.write.jdbc(url=jdbc_url, table='products_table', mode=mode, properties=config)
```

```
select * from products_table limit(5);
```

![pgadmin_products_table.png](Images/pgadmin_products_table.png)

```
# Write customers_df to table in RDS
# Operation took 3m 19s
customers_df.write.jdbc(url=jdbc_url, table='customers_table', mode=mode, properties=config)
```

```
select * from customers_table limit(5);
```

Figure X - Database read of customers_table

![pgadmin_customers_table.png](Images/pgadmin_customers_table.png)

```
# Write vine_df to table in RDS
# Operation took 3m 58s
vine_df.write.jdbc(url=jdbc_url, table='vine_table', mode=mode, properties=config)
```

```
select * from vine_table limit(5);
```

![pgadmin_vine_table.png](Images/pgadmin_vine_table.png)

```
from pyspark.sql import SQLContext
sqlContext = SQLContext(spark.sparkContext)
```

```
# Read review_id_df from RDS
# Operation took 1m 9s
review_id_df = sqlContext.read.jdbc(url=jdbc_url, table='review_id_table', properties=config)
```

```
# Read products_df from RDS
# Operation took 15s
products_df = sqlContext.read.jdbc(url=jdbc_url, table='products_table', properties=config)
```

```
# Read customers_df from RDS
# Operation took 19s
customers_df = sqlContext.read.jdbc(url=jdbc_url, table='customers_table', properties=config)
```

```
# Read vine_df from RDS
# Operation took 47s
vine_df = sqlContext.read.jdbc(url=jdbc_url, table='vine_table', properties=config)
```

```
tot_votes_gt_20_df = vine_df.filter("total_votes > 20")
tot_votes_gt_20_df.show(10)
```

![tot_votes_gt_20_df.png](Images/tot_votes_gt_20_df.png)

```
helpful_votes_pct_df = tot_votes_gt_20_df.filter("(helpful_votes / total_votes) > 0.5")
helpful_votes_pct_df.show(10)
```

![helpful_votes_pct_df.png](Images/helpful_votes_pct_df.png)]

```
vine_votes_df = helpful_votes_pct_df.filter("vine == 'Y'")
vine_votes_df.show(10)
```

![vine_votes_df.png](Images/vine_votes_df.png)

```
not_vine_votes_df = vine_df.filter("vine == 'N'")
not_vine_votes_df.show(10)
```

![not_vine_votes_df.png](Images/not_vine_votes_df.png)












| Category | .tsv Rows (millions) | Vine Reviews | .gz file MBytes |
| :---     | ---:          | --:          | --:   |
| Apparel_v1_00 | 5.9 | 2,336 | 619 |
| Automotive_v1_00 | 9.4 | 5,925 | 556 |
| **Baby_v1_00** | 11.2 | 12,100 | 341 |
| Beauty_v1_00 | 16.3 | 33,309 | 872 |
| Books_v1_00 | 26.6 | 152,087 | 2,614 |
| Books_v1_01 | 32.7 | 121,395 | 2,568 |
| Camera_v1_00 | 34.5 | 7,883 | 423 |
| Digital_Ebook_Purchase_v1_00 | 47.0 | 32 | 2,566 |
| Digital_Ebook_Purchase_v1_01 | 52.1 | 27 | 1,235 |
| Digital_Music_Purchase_v1_00 | 53.8 | **0** | 242  |
| Digital_Software_v1_00 | 53.9 | **0** | 19 |
| Digital_Video_Download_v1_00 | 58.0 | **0** | 484 |
| Digital_Video_Games_v1_00 | 58.1 | **0** | 27 |
| Electronics_v1_00 | 61.2 | 18 | 667 |
| **Furniture_v1_00** | **62.0** | **2,775** | **14388** |
| Gift_Card_v1_00 | 62.2 | **0** | 12 |
| **Grocery_v1_00** | 64.6 | 16,612 | 383  |
| Health_Personal_Care_v1_00 | 69.9 | 32,026 | 965 |
| **Home_Entertainment_v1_00** | 70.6 | 2,106 | 185  |
| Home_Improvement_v1_00 | 73.2 | 10,779 | 481  |
| Home_v1_00 | 79.5 | 23,683 | 1,031 |
| **Jewelry_v1_00** | 81.2 | 3,815 | 236  |
| Kitchen_v1_00 | 86.1 | 24,434 | 888 |
| Lawn_and_Garden_v1_00 | 88.7 | 13,454 | 465  |
| Luggage_v1_00 | 89.0 | 904 | 58 |
| Major_Appliances_v1_00 | 89.1 | 248 | 24  |
| Mobile_Apps_v1_00 | 94.1 | **0** | 533  |
| Mobile_Electronics_v1_00 | 94.3 | 18 | 22  |
| Music_v1_00 | 99.0 | 1,933 | 1,452  |
| **Musical_Instruments_v1_00** | 99.9 | 2,287 | 185  |
| Office_Products_v1_00 | 102.6 | 29,188 | 489  |
| Outdoors_v1_00 | 104.9 | 3,137 | 429  |
| PC_v1_00 | 111.8 | 36,230 | 1,443  |
| Personal_Care_Appliances_v1_00 | 111.8 | 32 | 17  |
| Pet_Products_v1_00 | 114.5 | 10,215 | 492  |
| Shoes_v1_00 | 118.9 | 895 | 613  |
| **Software_v1_00** | 119.2 | 10,415 | 90 |
| Sports_v1_00 | 124.1 | 10,080 | 833  |
| **Tools_v1_00** | 125.8 | 7,761 | 319  |
| Toys_v1_00 | 130.7 | 41,835 | 800  |
| Video_DVD_v1_00 | 135.8 | 4,340 | 1,443  |
| Video_Games_v1_00 | 137.5 | 4,291 | 454  |
| Video_v1_00 | 137.9 | **0** | 133  |
| Watches_v1_00 | 138.9 | 1,747 | 456  |
| Wireless_v1_00 | 147.9 | 17,481 | 1,626  |
