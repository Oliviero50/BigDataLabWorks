# Setup

1. Azure Cluster Overview
2. Overview -> Dashboards -> Ambari views
3. Hive View 2.0
4. Create external table:

* Checkouts are in dir /data
* Inventory is in dir /inventory

```sql
DROP TABLE lab04data;
CREATE EXTERNAL TABLE lab04data(
    BibNumber string,
    ItemBarcode string,
    ItemType string,
    BibCollection string,
    CallNumber string,
    CheckoutDateTime string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://hn0-lab04c.gyefre0bcfgunl1cebsurirggg.frax.internal.cloudapp.net/data/'
tblproperties ("skip.header.line.count"="1");
```

```sql
DROP TABLE lab04inventory;
CREATE EXTERNAL TABLE lab04inventory(
    BibNumber string,
    Title string,
    Author string,
    ISBN string,
    PublicationYear string,
    Publisher string,
    Subjects string,
    ItemType string,
    ItemCollection string,
    FloatingItem string,
    ItemLocation string,
    ReportDate string,
    ItemCount int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
) 
LOCATION 'hdfs://hn0-lab04.k4pktlmweozepoyrr5xj4ov4jf.frax.internal.cloudapp.net/inventory/'
tblproperties ("skip.header.line.count"="1");
```

5. Location = headnode + directory in HDFS

# Queries

## Query date
```sql
select *
from lab04data
WHERE from_unixtime(unix_timestamp(CheckoutDateTime,"MM/dd/yyyy hh:mm:ss aaa"), "MM/dd/yyyy hh:mm:ss aaa") = "11/21/2006 05:44:00 PM";
```

## Query author
```sql
select * 
from lab04inventory
WHERE author = "O'Ryan, Ellie";
```

## Join
```sql
SELECT *
FROM lab04data
JOIN lab04inventory ON (lab04data.bibnumber = lab04inventory.bibnum);
```

## Ausleihen pro Jahr   
```sql
SELECT COUNT(*) AS TotalCount,
YEAR(from_unixtime(unix_timestamp(CheckoutDateTime,"MM/dd/yyyy hh:mm:ss aaa")))
AS year
FROM lab04data
GROUP BY YEAR(from_unixtime(unix_timestamp(CheckoutDateTime,"MM/dd/yyyy hh:mm:ss aaa")))
ORDER BY year;
```

## Top 10 books alltime
```sql
SELECT COUNT(*) AS TotalCount, lab04data.bibnumber, lab04inventory.title
FROM lab04data
JOIN lab04inventory ON (lab04data.bibnumber = lab04inventory.bibnumber)
GROUP BY lab04data.bibnumber, lab04inventory.title
ORDER BY totalcount DESC
LIMIT 10;
```
