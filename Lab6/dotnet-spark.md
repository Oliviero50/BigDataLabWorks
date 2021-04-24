# Dotnet Spark API

## Hello World
[Microsoft Tutorial Hello World](https://github.com/dotnet/spark/blob/main/docs/getting-started/ubuntu-instructions.md)

Run in bin/Debug
```bash
spark-submit \
--class org.apache.spark.deploy.dotnet.DotnetRunner \
--master local \
microsoft-spark-2-4_2.11-1.1.1.jar \
dotnet HelloSpark.dll
```

## Spark Streaming
[Microsoft Tutorial Spark Streaming](https://docs.microsoft.com/en-us/dotnet/spark/tutorials/streaming)

Set Java to Java 8
```bash
sudo update-alternatives --config java
sudo update-alternatives --config javac
```

Make Spark Worker executable in
```bash
cd $DOTNET_WORKER_DIR
chmod +x Microsoft.Spark.Worker
```

```bash
spark-submit \
--class org.apache.spark.deploy.dotnet.DotnetRunner \
--master local \
microsoft-spark-2-4_2.11-1.1.1.jar \
dotnet mySparkStreamingApp.dll
```