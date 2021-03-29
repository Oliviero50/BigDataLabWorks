1. Azure Cluster Overview
2. Overview -> Dashboards -> Ambari views
3. Hive View 2.0
4. Create external table:

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
LOCATION 'hdfs://hn0-lab04.y4tcsfbxfosevo2xcke2rpws2e.parx.internal.cloudapp.net/data/'
tblproperties ("skip.header.line.count"="1");

5. Location = headnode + directory in HDFS

