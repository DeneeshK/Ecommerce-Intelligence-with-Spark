"""Test script to verify Spark installation"""

from pyspark.sql import SparkSession
import sys

print("="*60)
print("TESTING SPARK INSTALLATION")
print("="*60)

print(f"\n✅ Python Version: {sys.version}")

# Create Spark Session
try:
    spark = SparkSession.builder \
        .appName("InstallationTest") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    print(f"✅ Spark Version: {spark.version}")
    print(f"✅ Spark UI: http://localhost:4040")
    
    # Test basic operation
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    print("\n✅ Test DataFrame:")
    df.show()
    
    print("\n" + "="*60)
    print("🎉 SUCCESS! Your Spark environment is ready!")
    print("="*60)
    
    spark.stop()
    
except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
    print("\nPlease check:")
    print("1. Java is installed (java -version)")
    print("2. JAVA_HOME is set correctly")
    print("3. PySpark is installed (pip list | grep pyspark)")
    sys.exit(1)
