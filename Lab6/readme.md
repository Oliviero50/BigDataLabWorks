# Setup

## Install Spark 3.1.1 with Hadoop 3.2
[Apache Spark Homepage](https://www.apache.org/dyn/closer.lua/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz)

Download and unzip
```bash
wget https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
tar xvf spark-*
```
Move to opt/
```bash
sudo mv spark-3.0.1-bin-hadoop2.7 /opt/spark
```
Add Spark to path in .profile
```bash
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
export PYSPARK_PYTHON=/usr/bin/python3
```

Set java and javac to Java 8!
```bash
sudo update-alternatives --config java
sudo update-alternatives --config javac
```

Install pyspark
```bash
pip3 install pyspark
```

Start Spark
```bash
start-master.sh
```

Get address from localhost:8080 and start worker
```bash
start-slave.sh spark://address:port
```

## Kafka
Start docker-compose script


# Run Kafka + Spark Streaming

Run **producer.py** and **consumer.py**
Send **spark.py** to Spark
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 spark.py
```



