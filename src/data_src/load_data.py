
#Data Loading Utilities for Olist E-Commerce Dataset

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when , isnan
import os
from typing import Dict, List, Tuple
from .schemas import SCHEMAS, FILE_PATHS, EXPECTED_ROW_COUNTS

def load_dataset(spark: SparkSession, dataset_name: str, validate: bool = True ) -> DataFrame:
    """
    Load a single CSV dataset with proper schema and validation.
    
    This is the CORE function that all other loading functions use.
    It handles the actual Spark read operation with error handling.
    
    Args:
        spark: Active SparkSession instance    
            
        dataset_name: Name of dataset to load (e.g., 'customers', 'orders')   
            
        validate: Whether to validate row count (default: True)
           
    Returns:
        DataFrame: Loaded Spark DataFrame with schema applied
        
    Raises:
        FileNotFoundError: If CSV file doesn't exist at expected path
        KeyError: If dataset_name is not recognized
        ValueError: If row count doesn't match expected (when validate=True)
        
    """
    #Validate dataset name
    if dataset_name not in SCHEMAS:
        available= list(SCHEMAS.keys())
        raise KeyError(
            f"Unknown dataset:'{dataset_name}'\n"
            f" Avaliable options: {available}"
        )
    
    # check if file exists
    filepath=FILE_PATHS[dataset_name]

    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f" File not found: {filepath} \n"
            f" Please ensure dataset is in data/raw/ folder"
        )
    schema= SCHEMAS[dataset_name]

    #get schema
    schema=SCHEMAS[dataset_name]

    #load csv with spark
    try:
        #df=spark.read.csv(
        #    filepath,
        #    header=True,
        #    schema=schema,
        #    encoding='utf-8',
        #    mode="FAILFAST"
        #)

        df = spark.read.csv(
            filepath,
            header=True,
            schema=schema,
            encoding='utf-8',
            mode='PERMISSIVE',        #  Handle malformed rows gracefully
            multiLine=True,           #  Handle newlines in text fields
            escape='"',               #  Handle quotes in strings
            quote='"'                 #  Proper quote handling
        )
    except Exception as e:
        raise ValueError(
            f"Error loading {dataset_name}: {str(e)}\n"
            f" check if csv format matches schema definition"
        )
    
    row_count=df.count()
    col_count=len(df.columns)

    if validate and dataset_name in EXPECTED_ROW_COUNTS:
        expected=EXPECTED_ROW_COUNTS[dataset_name]

        tolerance=0.1
        diff_pct=abs(row_count - expected) / expected

        if diff_pct > tolerance:
            raise ValueError(
                f"Row count mismatch for {dataset_name}: \n"
                f" Expected: {expected:,} rows \n"
                f" Got: {row_count:,} rows\n"
                f" File might be corrupted or incomplete!"
            )
    print(f" LOADED {dataset_name}: {row_count:,} rows, {col_count} columns")

    return df



#load all DATASETS

def load_all_datasets(spark: SparkSession, validate: bool =True)-> Dict[str, DataFrame]:
    
    
    """
    Load all 8 datasets into a dictionary.
    
    This is a CONVENIENCE function that loads all datasets at once.
    Returns a dictionary so we can access datasets by name.
    
    Args:
        spark: Active SparkSession instance
        validate: Whether to validate row counts (default: True)
    
    Returns:
        dict: Dictionary with dataset names as keys, DataFrames as values
              Example: {'customers': DataFrame, 'orders': DataFrame, ...}
    
    
    """
    datasets={}
    print("LOADING ALL DATASETS")

    for name in SCHEMAS.keys():
        try:
            datasets[name]=load_dataset(spark, name, validate=validate)
        except Exception as e:

            print(f"\n FATAL ERROR Loading {name}:")
            print(f" {str(e)}")
            print(f"\n Stopping load process.")
            raise
        
    print()
    print(f" Successfully loaded {len(datasets)} datasets!")
    
    return datasets




#display dataframe infos

def display_dataframe_info(df: DataFrame, name: str, sample_rows: int = 5 ) -> None:
    
    
    """
    Display comprehensive information about a DataFrame.
    
    This is a DIAGNOSTIC function - shows everything you need to know
    about a DataFrame's structure and content.
    
    Args:
        df: DataFrame to analyze
        name: Name of the dataset (for display)
        sample_rows: Number of sample rows to show (default: 5)
    
    Returns:
        None (prints to console)
    
    Displays:
        1. Row and column counts
        2. Schema (data types)
        3. Sample data
        4. Null counts per column
    
    
    """
    
    print(f"DATASET: {name.upper()}")
    row_count=df.count()
    col_count=len(df.columns)

    #basic information
    print(f"\n Basic Info:")
    print(f" Rows:{row_count:,}")
    print(f" Columns:{col_count}")

    #schema
    print(f"\n Schema:")
    df.printSchema()

    #sample data
    print(f"\n Sample Data (first {sample_rows} rows): ")
    df.show(sample_rows, truncate=True)

    #null count

    print(f"\n Null count per Column:")
    
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) 
        for c in df.columns
    ])
    null_counts.show(truncate=False)



def get_table_relationships() -> Dict[str, List[Tuple[str, str]]]:


      """
    Returns foreign key relationships between tables.
    
    This is DOCUMENTATION function - defines how tables relate.
    Critical for understanding joins in feature engineering.
    
    Returns:
        dict: Relationships in format:
              {table: [(fk_column, referenced_table), ...]}

    """
      return  {
        "orders": [
            ("customer_id", "customers"),
            #  Each order belongs to one customer
        ],
        
        "order_items": [
            ("order_id", "orders"),
            #  Each item belongs to one order
            
            ("product_id", "products"),
            # Each item is one product
            
            ("seller_id", "sellers"),
            #  Each item is sold by one seller
        ],
        
        "order_reviews": [
            ("order_id", "orders"),
            #  Each review is for one order
        ],
        
        "order_payments": [
            ("order_id", "orders"),
            #  Each payment is for one order
            # NOTE: Orders can have multiple payments (split payment)
        ],
    }
      
      
def validate_relationships(datasets: Dict[str, DataFrame]) -> None:
    """
    Validate that foreign key relationships are intact.
    
    This is a DATA QUALITY function - ensures referential integrity.
    In a perfect world, every foreign key value exists in the referenced table.
    
    Args:
        datasets: Dictionary of loaded DataFrames
    
    Returns:
        None (prints validation results)
    
    Checks:
        - Do all foreign key values exist in referenced table?
        - How many orphaned records (FKs with no match)?
    
    
    """
    
    print("\n" + "="*60)
    print("VALIDATING TABLE RELATIONSHIPS")
    print("="*60)
    
    relationships = get_table_relationships()
    
    for table_name, fk_list in relationships.items():
        print(f"\n {table_name.upper()}:")
        
        for fk_column, ref_table in fk_list:
            # Get the foreign key DataFrame
            fk_df = datasets[table_name]
            
            # Get the referenced DataFrame
            # Primary key is always the first column in our schemas
            ref_df = datasets[ref_table]
            pk_column = ref_df.columns[0]
            
            # Count distinct values
            fk_distinct = fk_df.select(fk_column).distinct().count()
            pk_distinct = ref_df.select(pk_column).distinct().count()
            
            # Check for orphaned records
            # HOW: Left anti join = rows in left with no match in right
            orphaned = fk_df.join(
                ref_df,
                fk_df[fk_column] == ref_df[pk_column],
                "left_anti"  # Keep only rows with no match
            ).count()
            
            # Display results
            status = "good" if orphaned == 0 else "bad"
            
            print(f"  {status} {fk_column} → {ref_table}.{pk_column}")
            print(f"     Unique FK values: {fk_distinct:,}")
            print(f"     Unique PK values: {pk_distinct:,}")
            
            if orphaned > 0:
                print(f"       Orphaned records: {orphaned:,}")
                print(f"        (FK values with no match in {ref_table})")
            else:
                print(f"     All FKs have matches (referential integrity OK)")
     
def get_data_summary(datasets: Dict[str, DataFrame]) -> None:
    """
    Display summary statistics for all datasets.
    
    This is an OVERVIEW function - bird's eye view of all data.
    
    Args:
        datasets: Dictionary of loaded DataFrames
    
    Returns:
        None (prints summary table)
    """
    
    print("\n" + "="*60)
    print("DATA SUMMARY")
    print("="*60)
    
    print(f"\n{'Dataset':<20} {'Rows':>15} {'Columns':>10} {'Size (MB)':>12}")
    print("-" * 60)
    
    for name, df in datasets.items():
        row_count = df.count()
        col_count = len(df.columns)
        
        # Estimate size (rough approximation)
        # WHY? Helps understand memory requirements
        size_mb = (row_count * col_count * 8) / (1024 * 1024)  # 8 bytes per value
        
        print(f"{name:<20} {row_count:>15,} {col_count:>10} {size_mb:>12.2f}")
    
    print("-" * 60)
    
    total_rows = sum(df.count() for df in datasets.values())
    print(f"{'TOTAL':<20} {total_rows:>15,}")


# ==============================================================================
# MODULE USAGE EXAMPLE
# ==============================================================================
"""
HOW TO USE THIS MODULE IN NOTEBOOKS:

from pyspark.sql import SparkSession
from config.spark_config import get_spark_session
from src.data.load_data import load_all_datasets, display_dataframe_info

# Start Spark
spark = get_spark_session()

# Load all datasets
datasets = load_all_datasets(spark)

# Access individual datasets
customers = datasets['customers']
orders = datasets['orders']

# Display info about a dataset
display_dataframe_info(customers, 'customers')

# Get summary of all data
get_data_summary(datasets)
"""
