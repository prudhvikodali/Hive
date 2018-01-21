Log Analysis:

Capture E-commerce logs. Cleanse it and provide the following Analytics...
PageViews per user
PageViews per country
PageViews per hour
PageViews per hour per country
Top products viewed
Top products viewed per user


start-dfs.sh  //Starts HDFS Service
start-yarn-sh // Starts Yarn Cluster Manager
hiveserver2 & // Starts Hive Server2

beeline –u jdbc:hive2://localhost:10000 –n kafka –p kafka123

<property>
  <name>hive.aux.jars.path</name>
  <value>file:///usr/lib/hive/lib/hive-contrib.jar</value>
</property>
in hive>>Configuration>>Service-Wide>>Advanced>>Hive Service Advanced Configuration Snippet (Safety Valve) for hive-site.xml



Create ecommerce directory
hadoop fs -mkdir -p /user/hduser/data/ecommerce
Copy ecommerce logs to hdfs
hadoop fs -put /home/hduser/Projects/data/ecommerce/log-2014 /user/hduser/data/ecommerce/2014/.
hadoop fs -put /home/hduser/Projects/data/ecommerce/log-2015 /user/hduser/data/ecommerce/2015/.
hadoop fs -put /home/hduser/Projects/data/ecommerce/log-2016 /user/hduser/data/ecommerce/2016/.

OR add JAR /opt/apache-hive-1.2.1-bin/lib/hive-contrib-1.2.1.jar;

CREATE EXTERNAL TABLE ecommercelogs(
host STRING,
identity STRING,
userid STRING,
time STRING,
request STRING,
status STRING,
size STRING,
referer STRING,
agent STRING)
PARTITIONED BY (dt      STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
"input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) (-|\\[[^\\]]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*) ([^ \"]*|\"[^\"]*\") ([^ \"]*|\"[^\"]*\")",
"output.format.string" = "%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s"
)
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hduser/data/ecommerce'; 


Alter table ecommercelogs Add IF NOT EXISTS partition(dt=2014) location '/user/hduser/data/ecommerce/2014';
Alter table ecommercelogs Add IF NOT EXISTS partition(dt=2015) location '/user/hduser/data/ecommerce/2015';
Alter table ecommercelogs Add IF NOT EXISTS partition(dt=2016) location '/user/hduser/data/ecommerce/2016';


add JAR /home/kafka/Downloads/Training/hadoop/hive-pig-examples/target/hadoop-examples-1.0-SNAPSHOT.jar;
add JAR /opt/apache-hive-2.1.1-bin/lib/hive-contrib-2.1.1.jar;

CREATE TEMPORARY FUNCTION convertDateFormat AS 'com.datamantra.hive.udf.UDFDateFormat';
select year(convertDateFormat(time, "[dd/MMM/yyyy:HH:mm:ss Z]", "yyyy-MM-dd HH:mm:ss")) as year from ecommercelogs limit 10;

CREATE TEMPORARY FUNCTION getCountryCode AS 'com.datamantra.hive.udf.CountryCodeUdf';
add FILE  /home/kafka/Downloads/Training/hadoop/hive-pig-examples/src/main/resources/all_classbs.txt;
select getCountryCode(host, "/home/kafka/Downloads/Training/hadoop/hive-pig-examples/src/main/resources/all_classbs.txt") as CountryCode from ecommercelogs limit 10;


CREATE TEMPORARY FUNCTION getProductCategory AS 'com.datamantra.hive.udf.UDFProductsCategory';
select distinct(getProductCategory(request)) from ecommercelogs;

CREATE TEMPORARY FUNCTION getProducts AS 'com.datamantra.hive.udf.UDFProducts';
select distinct(getProducts(request)) from ecommercelogs;


CREATE TEMPORARY FUNCTION collectCategory AS 'com.datamantra.udaf.ProductsInCategoryUDAF';
select collectCategory(request, "mobiles") from ecommercelogs;



CREATE TABLE clicks(
countrycode STRING,
userid STRING,
datetime STRING,
products STRING)
STORED AS ORC;

add JAR /home/kafka/Downloads/Training/hadoop/hive-pig-examples/target/hadoop-examples-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION convertDateFormat AS 'com.datamantra.hive.udf.UDFDateFormat';
CREATE TEMPORARY FUNCTION getProducts AS 'com.datamantra.hive.udf.UDFProducts';
CREATE TEMPORARY FUNCTION getCountryCode AS 'com.datamantra.hive.genericudf.CountryCodeUDF';
add FILE  /home/kafka/Downloads/Training/hadoop/hive-pig-examples/src/main/resources/all_classbs.txt;

INSERT OVERWRITE TABLE clicks
SELECT getCountryCode(host, "/home/kafka/Downloads/Training/hadoop/hive-pig-examples/src/main/resources/all_classbs.txt"), userid, convertDateFormat(time, "[dd/MMM/yyyy:HH:mm:ss Z]", "yyyy-MM-dd HH:mm:ss"), getProducts(request)
FROM ecommercelogs;

PageViews per user;
select userid, count(products) from clicks group by userid;

PageViews per country
select Country, count(products) from clicks group by Country;


select products, count(products) from clicks group by products;
PageViews per hour
create view viewsbyhour as select hour(time) as hr, products from clicks;
select hr, count(products) from viewsbyhour group by hr;

PageViews per hour per country
create view viewsbyhourcountry as select Country, hour(time) as hr, products from clicks;
select Country, hr, count(products) from viewsbyhourcountry group by Country, hr;

Top products viewed
select products, count(products) as cnt from clicks group by products sort by cnt desc;

Top products viewed per user;
select user, products, count(products) as cnt from clicks group by user, products sort by cnt


Querying all dates in a particular month of a year
SELECT * FROM clicks WHERE datetime LIKE '2015-02-% %:%:%'

Querying All Days that start/end with a 5
SELECT * FROM clicks WHERE datetime LIKE '%-%-%5 %:%:%';

SELECT cast(substring(from_unixtime(UNIX_TIMESTAMP(datetime, 'YYYY-MM-DD')), 1, 10) as date) AS dt from clicks limit 10;
SELECT substring(from_unixtime(UNIX_TIMESTAMP(datetime, 'YYYY-MM-DD')), 1, 10) AS dt from clicks;

CREATE VIEW clicksview AS SELECT countrycode, userid, products, substring(from_unixtime(UNIX_TIMESTAMP(datetime, 'YYYY-MM-DD')), 1, 10) AS dt from clicks;
SELECT * from clicksview WHERE dt IN ('2013-02-02');
SELECT * from clicks WHERE substring(from_unixtime(UNIX_TIMESTAMP(datetime, 'YYYY-MM-DD')), 1, 10) IN ('2013-010-01');
