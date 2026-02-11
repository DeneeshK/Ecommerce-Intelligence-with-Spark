
#schema Definitions for Olist E-Commerce Dataset


from pyspark.sql.types import(
    StructType, StructField, StringType , IntegerType, DoubleType, TimestampType
)

# CUSTOMER DATASET

"""
 PURPOSE : Customer demographic and location information
 PRIMARY KEY : order_id
 BUSINESS KEY : customeer_unique_id (tracks same customer across orders)
"""

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(),False),
    StructField("customer_zip_code_prefix", StringType(), True) ,
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

#ORDERS DATASET

"""
PURPOSE: order lifecycle tracking (purchase-> delivery)
PRIMARY  KEY : order_id,
FOREIGN KEY : customer_id-> customers.customer_id
CRITICAL FOR : Churn prediction (time between orders)
"""

orders_schema =StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
])

# ORDER ITEMS DATASET

"""
PURPOSE : Individual products within each order
FOREIGN KEYS: order_id, product_id, seller_id
"""

order_items_schema=StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_di", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(),True),
    StructField("shipping_limit_date", TimestampType(),True),
    StructField("price", DoubleType(),True),
    StructField("freight_value", DoubleType(), True),
])


#PRODUCTS DATASET

"""
PURPOSE: Product catalog with physical characteristics
PRIMARY KEY: product_id
CRITICAL FOR: Recommendations, shipping cost prediction
"""

products_schema=StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(),True),
    StructField("product_name_length", IntegerType(), True),
    StructField("product_description_length", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", IntegerType(), True),

    StructField("product_length_cm", IntegerType(), True),
    StructField("product_height_cm", IntegerType(), True),
    StructField("product_width_cm", IntegerType(), True),

])


# 5. ORDER REVIEWS DATASET

"""
 PURPOSE: Customer satisfaction scores and feedback
 FOREIGN KEY: order_id
 CRITICAL FOR: Customer satisfaction analysis, churn prediction
"""

order_reviews_schema = StructType([
    StructField("review_id", StringType(), False),
    
    StructField("order_id", StringType(), False),
   
    
    StructField("review_score", IntegerType(), True),
   
    
    StructField("review_comment_title", StringType(), True),

    
    StructField("review_comment_message", StringType(), True),
  
    
    StructField("review_creation_date", TimestampType(), True),

    StructField("review_answer_timestamp", TimestampType(), True),
   
])



# 6. ORDER PAYMENTS DATASET

"""
PURPOSE: Payment method and installment tracking
FOREIGN KEY: order_id
WHY SEPARATE? Orders can have multiple payment methods (split payment)

"""

order_payments_schema = StructType([
    StructField("order_id", StringType(), False),
    
    StructField("payment_sequential", IntegerType(), True),

    
    StructField("payment_type", StringType(), True),
 
    
    StructField("payment_installments", IntegerType(), True),

    
    StructField("payment_value", DoubleType(), True),

])



# 7. SELLERS DATASET

""" 
PURPOSE: Seller information (marketplace platform)
PRIMARY KEY: seller_id
"""

sellers_schema = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),

])



# 8. GEOLOCATION DATASET

""" PURPOSE: Zip code to lat/long mapping for distance calculations
 NOTE: This table is HUGE (1M+ rows) but small file size
 USE CASE: Calculate distance between customer and seller
"""

geolocation_schema = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat", DoubleType(), True),
    StructField("geolocation_lng", DoubleType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True),
])



# SCHEMA DICTIONARY - Easy Access



SCHEMAS = {
    "customers": customers_schema,
    "orders": orders_schema,
    "order_items": order_items_schema,
    "products": products_schema,
    "order_reviews": order_reviews_schema,
    "order_payments": order_payments_schema,
    "sellers": sellers_schema,
    "geolocation": geolocation_schema,
}


# ==============================================================================
# FILE PATHS MAPPING
# ==============================================================================

import os

# Get absolute path to project root
# __file__ = this schemas.py file location
# dirname twice = go up two levels (src/data/ -> src/ -> project_root/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FILE_PATHS = {
    "customers": os.path.join(PROJECT_ROOT, "data/raw/olist_customers_dataset.csv"),
    "orders": os.path.join(PROJECT_ROOT, "data/raw/olist_orders_dataset.csv"),
    "order_items": os.path.join(PROJECT_ROOT, "data/raw/olist_order_items_dataset.csv"),
    "products": os.path.join(PROJECT_ROOT, "data/raw/olist_products_dataset.csv"),
    "order_reviews": os.path.join(PROJECT_ROOT, "data/raw/olist_order_reviews_dataset.csv"),
    "order_payments": os.path.join(PROJECT_ROOT, "data/raw/olist_order_payments_dataset.csv"),
    "sellers": os.path.join(PROJECT_ROOT, "data/raw/olist_sellers_dataset.csv"),
    "geolocation": os.path.join(PROJECT_ROOT, "data/raw/olist_geolocation_dataset.csv"),
}

# ==============================================================================
# EXPECTED ROW COUNTS - Data Validation
# ==============================================================================


EXPECTED_ROW_COUNTS = {
    "customers": 99441,
    "orders": 99441,
    "order_items": 112650,
    "products": 32951,
    "order_reviews": 99224,
    "order_payments": 103886,
    "sellers": 3095,
    "geolocation": 1000163,
}
