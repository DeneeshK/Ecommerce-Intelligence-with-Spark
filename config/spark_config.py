"""
Spark Configuration for Ecommerce Intelligence Platform

Optimized for local development on:
- 16GB RAM
- i5 9th Gen (6 cores, 12 threads)
"""

def get_spark_config():
    """
    Returns Spark configuration dictionary optimized for local development.
    
    Returns:
        dict: Spark configuration parameters
    """
    return {
        # Application
        "spark.app.name": "EcommerceIntelligence",
        
        # Memory Management (crucial for 16GB machine)
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",
        
        # Parallelism (based on CPU cores)
        "spark.sql.shuffle.partitions": "8",
        "spark.default.parallelism": "8",
        
        # Adaptive Query Execution (Spark 3.x - AUTO OPTIMIZATION)
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        
        # Performance Optimizations
        "spark.sql.autoBroadcastJoinThreshold": "10485760",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        
        # UI and Logging
        "spark.ui.port": "4040",
        "spark.ui.showConsoleProgress": "true",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        
        # Warehouse location
        "spark.sql.warehouse.dir": "file:///tmp/spark-warehouse",
    }


def get_spark_session():
    """
    Creates and returns a configured Spark session.
    
    Returns:
        SparkSession: Configured Spark session
    """
    from pyspark.sql import SparkSession
    
    config = get_spark_config()
    
    builder = SparkSession.builder
    
    # Apply all configurations
    for key, value in config.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark Session Created: {spark.version}")
    print(f"✅ Spark UI available at: http://localhost:4040")
    print(f"✅ Master: {spark.sparkContext.master}")
    print(f"✅ Cores: {spark.sparkContext.defaultParallelism}")
    
    return spark
