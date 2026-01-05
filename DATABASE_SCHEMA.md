# Olist E-Commerce Database Schema Documentation

## Table of Contents

1. [Overview](#overview)
2. [Data Architecture Layers](#data-architecture-layers)
3. [Bronze Layer Tables](#bronze-layer-tables)
   - [customer_bz](#customer_bz)
   - [geolocation_bz](#geolocation_bz)
   - [product_bz](#product_bz)
   - [seller_bz](#seller_bz)
   - [product_category_bz](#product_category_bz)
   - [order_bz](#order_bz)
   - [order_item_bz](#order_item_bz)
   - [payment_bz](#payment_bz)
   - [review_bz](#review_bz)
   - [data_quality_quarantine](#data_quality_quarantine)
4. [Silver Layer Tables](#silver-layer-tables)
   - [order_sl](#order_sl)
   - [order_item_sl](#order_item_sl)
   - [payment_sl](#payment_sl)
   - [review_sl](#review_sl)
5. [Gold Layer Tables](#gold-layer-tables)
   - [order_gl](#order_gl)
   - [order_item_gl](#order_item_gl)
   - [payment_gl](#payment_gl)

---

## Overview

This document provides comprehensive schema documentation for all tables in the Olist E-Commerce ETL pipeline. The architecture follows a medallion pattern with three data quality layers:

- **Bronze Layer**: Raw data ingestion with minimal transformation, schema enforcement, and metadata tracking
- **Silver Layer**: Cleaned, deduplicated data with upsert capabilities and update tracking
- **Gold Layer**: Business-ready, enriched data with calculated metrics and analytical transformations

All tables are stored as Delta Lake tables in Microsoft Fabric Lakehouse, providing ACID transactions, time travel, and schema evolution capabilities.

---

## Data Architecture Layers

### Bronze Layer
The Bronze layer contains raw data with minimal transformation. All tables include:
- Original source data columns
- `load_time`: Timestamp when the record was loaded into the Bronze layer
- `source_file`: File path of the source data

**Storage Location**: `abfss://olist_project@onelake.dfs.fabric.microsoft.com/olist_LH.Lakehouse/Files/medallion/bronze`

### Silver Layer
The Silver layer contains cleaned and deduplicated data. Tables include:
- All columns from Bronze layer
- `update_time`: Timestamp when the record was last updated (for upsert tracking)

**Storage Location**: `abfss://olist_project@onelake.dfs.fabric.microsoft.com/olist_LH.Lakehouse/Files/medallion/silver`

### Gold Layer
The Gold layer contains business-ready, enriched data with:
- All columns from Silver layer
- Calculated/derived columns (e.g., `total_value`, `delivery_duration`)
- Date/time extracted fields for analytical purposes

**Storage Location**: `abfss://olist_project@onelake.dfs.fabric.microsoft.com/olist_LH.Lakehouse/Files/medallion/gold`

---

## Bronze Layer Tables

### customer_bz

**Description**: Customer dimension table containing customer information and demographics.

**Table Type**: Delta Lake  
**Primary Key**: `customer_id`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| customer_id | string | NOT NULL | Unique identifier for the customer (primary key) |
| customer_unique_id | string | NULL | Unique customer identifier (may differ from customer_id) |
| customer_zip_code_prefix | string | NULL | Customer zip code prefix (5 digits) |
| customer_city | string | NULL | City where the customer is located |
| customer_state | string | NULL | State where the customer is located (2-letter code) |
| first_name | string | NULL | Customer's first name |
| last_name | string | NULL | Customer's last name |
| full_name | string | NULL | Customer's full name (concatenated) |
| gender | string | NULL | Customer's gender |
| date_of_birth | date | NULL | Customer's date of birth |
| age | integer | NULL | Customer's age (calculated from date_of_birth) |
| load_time | timestamp | NOT NULL | Timestamp when the record was loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |

**Indexes/Constraints**:
- Primary key: `customer_id`
- Unique constraint: `customer_id`

---

### geolocation_bz

**Description**: Geographic location reference table containing zip code coordinates and location information.

**Table Type**: Delta Lake  
**Primary Key**: `geolocation_zip_code_prefix` (composite with city/state)

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| geolocation_zip_code_prefix | string | NOT NULL | Zip code prefix (5 digits) |
| geolocation_lat | integer | NULL | Latitude coordinate |
| geolocation_lng | integer | NULL | Longitude coordinate |
| geolocation_city | string | NULL | City name |
| geolocation_state | string | NULL | State abbreviation (2-letter code) |
| load_time | timestamp | NOT NULL | Timestamp when the record was loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |

**Indexes/Constraints**:
- Composite key: `geolocation_zip_code_prefix`, `geolocation_city`, `geolocation_state`

---

### product_bz

**Description**: Product dimension table containing product information, dimensions, and metadata.

**Table Type**: Delta Lake  
**Primary Key**: `product_id`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| product_id | string | NOT NULL | Unique identifier for the product (primary key) |
| product_category_name | string | NULL | Product category name (Portuguese) |
| product_name_lenght | integer | NULL | Length of the product name (number of characters) |
| product_description_lenght | integer | NULL | Length of the product description (number of characters) |
| product_photos_qty | integer | NULL | Number of product photos available |
| product_weight_g | integer | NULL | Product weight in grams |
| product_length_cm | integer | NULL | Product length in centimeters |
| product_height_cm | integer | NULL | Product height in centimeters |
| product_width_cm | integer | NULL | Product width in centimeters |
| load_time | timestamp | NOT NULL | Timestamp when the record was loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |

**Indexes/Constraints**:
- Primary key: `product_id`
- Foreign key: `product_category_name` references `product_category_bz.product_category_name`

---

### seller_bz

**Description**: Seller dimension table containing seller location and identification information.

**Table Type**: Delta Lake  
**Primary Key**: `seller_id`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| seller_id | string | NOT NULL | Unique identifier for the seller (primary key) |
| seller_zip_code_prefix | string | NULL | Seller zip code prefix (5 digits) |
| seller_city | string | NULL | City where the seller is located |
| seller_state | string | NULL | State where the seller is located (2-letter code) |
| load_time | timestamp | NOT NULL | Timestamp when the record was loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |

**Indexes/Constraints**:
- Primary key: `seller_id`

---

### product_category_bz

**Description**: Product category translation table mapping Portuguese category names to English.

**Table Type**: Delta Lake  
**Primary Key**: `product_category_name`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| product_category_name | string | NOT NULL | Product category name in Portuguese (primary key) |
| product_category_name_english | string | NULL | Product category name translated to English |
| load_time | timestamp | NOT NULL | Timestamp when the record was loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |

**Indexes/Constraints**:
- Primary key: `product_category_name`

---

### order_bz

**Description**: Order fact table containing order status and delivery timeline information.

**Table Type**: Delta Lake  
**Primary Key**: `order_id`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| order_id | string | NOT NULL | Unique identifier for the order (primary key) |
| customer_id | string | NULL | Foreign key to customer_bz table |
| order_status | string | NULL | Order status (e.g., 'delivered', 'shipped', 'canceled', etc.) |
| order_purchase_timestamp | timestamp | NULL | Timestamp when the order was placed |
| order_approved_at | timestamp | NULL | Timestamp when the order was approved |
| order_delivered_carrier_date | timestamp | NULL | Timestamp when the order was delivered to the carrier |
| order_delivered_customer_date | timestamp | NULL | Timestamp when the order was delivered to the customer |
| order_estimated_delivery_date | timestamp | NULL | Estimated delivery date for the order |
| load_time | timestamp | NOT NULL | Timestamp when the record was loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |

**Indexes/Constraints**:
- Primary key: `order_id`
- Foreign key: `customer_id` references `customer_bz.customer_id`

**Valid Values**:
- `order_status`: 'delivered', 'approved', 'invoiced', 'shipped', 'canceled', 'unavailable', 'processing', 'created'

---

### order_item_bz

**Description**: Order items fact table containing individual items within each order.

**Table Type**: Delta Lake  
**Primary Key**: `order_id`, `order_item_id` (composite)

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| order_id | string | NOT NULL | Foreign key to order_bz table |
| order_item_id | string | NOT NULL | Sequential number identifying the item in the order |
| product_id | string | NULL | Foreign key to product_bz table |
| seller_id | string | NULL | Foreign key to seller_bz table |
| shipping_limit_date | timestamp | NULL | Date by which the seller must ship the product |
| price | double | NULL | Price of the product |
| freight_value | double | NULL | Freight/shipping cost for the product |
| order_purchase_timestamp | timestamp | NULL | Timestamp when the order was placed (denormalized for performance) |
| load_time | timestamp | NOT NULL | Timestamp when the record was loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |

**Indexes/Constraints**:
- Primary key: `order_id`, `order_item_id`
- Foreign key: `order_id` references `order_bz.order_id`
- Foreign key: `product_id` references `product_bz.product_id`
- Foreign key: `seller_id` references `seller_bz.seller_id`

---

### payment_bz

**Description**: Payment transactions fact table containing payment information for orders.

**Table Type**: Delta Lake  
**Primary Key**: `order_id`, `payment_sequential` (composite)

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| order_id | string | NOT NULL | Foreign key to order_bz table |
| payment_sequential | integer | NOT NULL | Sequential number for multiple payments on the same order |
| payment_type | string | NULL | Payment method type (e.g., 'credit_card', 'boleto', 'voucher', etc.) |
| payment_installments | integer | NULL | Number of installments for the payment |
| payment_value | double | NULL | Payment amount |
| order_purchase_timestamp | timestamp | NULL | Timestamp when the order was placed (denormalized for performance) |
| load_time | timestamp | NOT NULL | Timestamp when the record was loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |

**Indexes/Constraints**:
- Primary key: `order_id`, `payment_sequential`
- Foreign key: `order_id` references `order_bz.order_id`

**Valid Values**:
- `payment_type`: 'credit_card', 'boleto', 'voucher', 'debit_card', 'not_defined'

---

### review_bz

**Description**: Customer review fact table containing product reviews and ratings.

**Table Type**: Delta Lake  
**Primary Key**: `review_id`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| review_id | string | NOT NULL | Unique identifier for the review (primary key) |
| order_id | string | NULL | Foreign key to order_bz table |
| review_score | integer | NULL | Review rating score (1-5 scale) |
| review_comment_title | string | NULL | Title of the review comment |
| review_comment_message | string | NULL | Full text of the review comment |
| review_creation_date | timestamp | NULL | Timestamp when the review was created |
| review_answer_timestamp | timestamp | NULL | Timestamp when the review was answered by the seller |
| order_purchase_timestamp | timestamp | NULL | Timestamp when the order was placed (denormalized for performance) |
| load_time | timestamp | NOT NULL | Timestamp when the record was loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |

**Indexes/Constraints**:
- Primary key: `review_id`
- Foreign key: `order_id` references `order_bz.order_id`

**Valid Values**:
- `review_score`: Integer between 1 and 5

---

### data_quality_quarantine

**Description**: Data quality quarantine table storing records that failed validation rules.

**Table Type**: Delta Lake  
**Primary Key**: None (log table)

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| table_name | string | NOT NULL | Name of the source table where the violation occurred |
| batch_id | long | NOT NULL | Batch ID from the streaming process |
| violated_rules | string | NULL | Comma-separated list of validation rules that were violated |
| raw_data | string | NULL | JSON representation of the entire row that failed validation |
| ingestion_time | timestamp | NOT NULL | Timestamp when the record was quarantined |

**Indexes/Constraints**:
- Index on: `table_name`, `ingestion_time` (for querying recent violations)

**Usage**: This table is used by the Great Expectations data quality framework to store records that fail validation checks before being inserted into Bronze layer tables.

---

## Silver Layer Tables

### order_sl

**Description**: Cleaned and deduplicated order data from Bronze layer with upsert tracking.

**Table Type**: Delta Lake  
**Primary Key**: `order_id`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| order_id | string | NOT NULL | Unique identifier for the order (primary key) |
| customer_id | string | NULL | Foreign key to customer_bz table |
| order_status | string | NULL | Order status |
| order_purchase_timestamp | timestamp | NULL | Timestamp when the order was placed |
| order_approved_at | timestamp | NULL | Timestamp when the order was approved |
| order_delivered_carrier_date | timestamp | NULL | Timestamp when the order was delivered to the carrier |
| order_delivered_customer_date | timestamp | NULL | Timestamp when the order was delivered to the customer |
| order_estimated_delivery_date | timestamp | NULL | Estimated delivery date for the order |
| load_time | timestamp | NOT NULL | Timestamp when the record was first loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |
| update_time | timestamp | NOT NULL | Timestamp when the record was last updated in Silver layer |

**Indexes/Constraints**:
- Primary key: `order_id`
- Foreign key: `customer_id` references `customer_bz.customer_id`

**Upsert Logic**: Uses MERGE operation on `order_id` to handle updates and inserts.

---

### order_item_sl

**Description**: Cleaned and deduplicated order items data from Bronze layer with upsert tracking.

**Table Type**: Delta Lake  
**Primary Key**: `order_id`, `order_item_id` (composite)

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| order_id | string | NOT NULL | Foreign key to order_sl table |
| order_item_id | string | NOT NULL | Sequential number identifying the item in the order |
| product_id | string | NULL | Foreign key to product_bz table |
| seller_id | string | NULL | Foreign key to seller_bz table |
| shipping_limit_date | timestamp | NULL | Date by which the seller must ship the product |
| price | double | NULL | Price of the product |
| freight_value | double | NULL | Freight/shipping cost for the product |
| order_purchase_timestamp | timestamp | NULL | Timestamp when the order was placed |
| load_time | timestamp | NOT NULL | Timestamp when the record was first loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |
| update_time | timestamp | NOT NULL | Timestamp when the record was last updated in Silver layer |

**Indexes/Constraints**:
- Primary key: `order_id`, `order_item_id`
- Foreign key: `order_id` references `order_sl.order_id`
- Foreign key: `product_id` references `product_bz.product_id`
- Foreign key: `seller_id` references `seller_bz.seller_id`

**Upsert Logic**: Uses MERGE operation on `order_id` and `order_item_id` to handle updates and inserts.

---

### payment_sl

**Description**: Cleaned and deduplicated payment transactions data from Bronze layer with upsert tracking.

**Table Type**: Delta Lake  
**Primary Key**: `order_id`, `payment_sequential` (composite)

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| order_id | string | NOT NULL | Foreign key to order_sl table |
| payment_sequential | integer | NOT NULL | Sequential number for multiple payments on the same order |
| payment_type | string | NULL | Payment method type |
| payment_installments | integer | NULL | Number of installments for the payment |
| payment_value | double | NULL | Payment amount |
| order_purchase_timestamp | timestamp | NULL | Timestamp when the order was placed |
| load_time | timestamp | NOT NULL | Timestamp when the record was first loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |
| update_time | timestamp | NOT NULL | Timestamp when the record was last updated in Silver layer |

**Indexes/Constraints**:
- Primary key: `order_id`, `payment_sequential`
- Foreign key: `order_id` references `order_sl.order_id`

**Upsert Logic**: Uses MERGE operation on `order_id` and `payment_sequential` to handle updates and inserts.

---

### review_sl

**Description**: Cleaned and deduplicated customer review data from Bronze layer with upsert tracking.

**Table Type**: Delta Lake  
**Primary Key**: `order_id`, `review_id` (composite)

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| review_id | string | NOT NULL | Unique identifier for the review |
| order_id | string | NULL | Foreign key to order_sl table |
| review_score | integer | NULL | Review rating score (1-5 scale) |
| review_comment_title | string | NULL | Title of the review comment |
| review_comment_message | string | NULL | Full text of the review comment |
| review_creation_date | timestamp | NULL | Timestamp when the review was created |
| review_answer_timestamp | timestamp | NULL | Timestamp when the review was answered by the seller |
| order_purchase_timestamp | timestamp | NULL | Timestamp when the order was placed |
| load_time | timestamp | NOT NULL | Timestamp when the record was first loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |
| update_time | timestamp | NOT NULL | Timestamp when the record was last updated in Silver layer |

**Indexes/Constraints**:
- Primary key: `order_id`, `review_id`
- Foreign key: `order_id` references `order_sl.order_id`

**Upsert Logic**: Uses MERGE operation on `order_id` and `review_id` to handle updates and inserts.

---

## Gold Layer Tables

### order_gl

**Description**: Business-ready order data with enriched calculated fields for analytics.

**Table Type**: Delta Lake  
**Primary Key**: `order_id`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| order_id | string | NOT NULL | Unique identifier for the order (primary key) |
| customer_id | string | NULL | Foreign key to customer_bz table |
| order_status | string | NULL | Order status |
| order_purchase_timestamp | timestamp | NULL | Timestamp when the order was placed |
| order_approved_at | timestamp | NULL | Timestamp when the order was approved |
| order_delivered_carrier_date | timestamp | NULL | Timestamp when the order was delivered to the carrier |
| order_delivered_customer_date | timestamp | NULL | Timestamp when the order was delivered to the customer |
| order_estimated_delivery_date | timestamp | NULL | Estimated delivery date for the order |
| load_time | timestamp | NOT NULL | Timestamp when the record was first loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |
| update_time | timestamp | NOT NULL | Timestamp when the record was last updated in Silver layer |
| order_purchase_date | date | NULL | **Calculated**: Date extracted from order_purchase_timestamp |
| order_purchase_time | string | NULL | **Calculated**: Time extracted from order_purchase_timestamp (HH:mm:ss format) |
| delivery_duration | integer | NULL | **Calculated**: Number of days between order_purchase_timestamp and order_delivered_customer_date |

**Indexes/Constraints**:
- Primary key: `order_id`
- Foreign key: `customer_id` references `customer_bz.customer_id`
- Index on: `order_purchase_date` (for time-based analytics)

**Calculated Fields**:
- `order_purchase_date`: Extracted using `TO_DATE(order_purchase_timestamp)`
- `order_purchase_time`: Extracted using `DATE_FORMAT(order_purchase_timestamp, 'HH:mm:ss')`
- `delivery_duration`: Calculated using `DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)`

---

### order_item_gl

**Description**: Business-ready order items data with enriched calculated fields for analytics.

**Table Type**: Delta Lake  
**Primary Key**: `order_id`, `order_item_id` (composite)

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| order_id | string | NOT NULL | Foreign key to order_gl table |
| order_item_id | string | NOT NULL | Sequential number identifying the item in the order |
| product_id | string | NULL | Foreign key to product_bz table |
| seller_id | string | NULL | Foreign key to seller_bz table |
| shipping_limit_date | timestamp | NULL | Date by which the seller must ship the product |
| price | double | NULL | Price of the product |
| freight_value | double | NULL | Freight/shipping cost for the product |
| order_purchase_timestamp | timestamp | NULL | Timestamp when the order was placed |
| load_time | timestamp | NOT NULL | Timestamp when the record was first loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |
| update_time | timestamp | NOT NULL | Timestamp when the record was last updated in Silver layer |
| order_purchase_date | date | NULL | **Calculated**: Date extracted from order_purchase_timestamp |
| order_purchase_time | string | NULL | **Calculated**: Time extracted from order_purchase_timestamp (HH:mm:ss format) |
| total_value | double | NULL | **Calculated**: Total value including price and freight (price + freight_value) |

**Indexes/Constraints**:
- Primary key: `order_id`, `order_item_id`
- Foreign key: `order_id` references `order_gl.order_id`
- Foreign key: `product_id` references `product_bz.product_id`
- Foreign key: `seller_id` references `seller_bz.seller_id`
- Index on: `order_purchase_date` (for time-based analytics)

**Calculated Fields**:
- `order_purchase_date`: Extracted using `TO_DATE(order_purchase_timestamp)`
- `order_purchase_time`: Extracted using `DATE_FORMAT(order_purchase_timestamp, 'HH:mm:ss')`
- `total_value`: Calculated as `price + freight_value`

---

### payment_gl

**Description**: Business-ready payment transactions data with enriched date/time fields for analytics.

**Table Type**: Delta Lake  
**Primary Key**: `order_id`, `payment_sequential` (composite)

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| order_id | string | NOT NULL | Foreign key to order_gl table |
| payment_sequential | integer | NOT NULL | Sequential number for multiple payments on the same order |
| payment_type | string | NULL | Payment method type |
| payment_installments | integer | NULL | Number of installments for the payment |
| payment_value | double | NULL | Payment amount |
| order_purchase_timestamp | timestamp | NULL | Timestamp when the order was placed |
| load_time | timestamp | NOT NULL | Timestamp when the record was first loaded into Bronze layer |
| source_file | string | NOT NULL | Source file path from which the data was ingested |
| update_time | timestamp | NOT NULL | Timestamp when the record was last updated in Silver layer |
| order_purchase_date | date | NULL | **Calculated**: Date extracted from order_purchase_timestamp |
| order_purchase_time | string | NULL | **Calculated**: Time extracted from order_purchase_timestamp (HH:mm:ss format) |

**Indexes/Constraints**:
- Primary key: `order_id`, `payment_sequential`
- Foreign key: `order_id` references `order_gl.order_id`
- Index on: `order_purchase_date` (for time-based analytics)

**Calculated Fields**:
- `order_purchase_date`: Extracted using `TO_DATE(order_purchase_timestamp)`
- `order_purchase_time`: Extracted using `DATE_FORMAT(order_purchase_timestamp, 'HH:mm:ss')`

---

## Data Type Reference

### Spark SQL Data Types Used

| Spark Type | Description | Range/Format |
|-----------|-------------|--------------|
| `string` | Variable-length character string | UTF-8 encoded text |
| `integer` | 32-bit signed integer | -2,147,483,648 to 2,147,483,647 |
| `long` | 64-bit signed integer | -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 |
| `double` | 64-bit double precision floating point | IEEE 754 standard |
| `date` | Date value | YYYY-MM-DD format |
| `timestamp` | Date and time value | YYYY-MM-DD HH:MM:SS.fffffffff format |

---

## Relationships Overview

### Entity Relationship Diagram (Text Representation)

```
customer_bz (1) ──< (N) order_bz (1) ──< (N) order_item_bz
                                            │
                                            ├── (N) payment_bz
                                            │
                                            └── (N) review_bz

product_bz (1) ──< (N) order_item_bz
product_category_bz (1) ──< (N) product_bz
seller_bz (1) ──< (N) order_item_bz
```

### Key Relationships

1. **Orders to Customers**: One customer can have many orders (`customer_bz.customer_id` → `order_bz.customer_id`)

2. **Orders to Order Items**: One order can have many order items (`order_bz.order_id` → `order_item_bz.order_id`)

3. **Orders to Payments**: One order can have many payment transactions (`order_bz.order_id` → `payment_bz.order_id`)

4. **Orders to Reviews**: One order can have one review (`order_bz.order_id` → `review_bz.order_id`)

5. **Products to Order Items**: One product can appear in many order items (`product_bz.product_id` → `order_item_bz.product_id`)

6. **Sellers to Order Items**: One seller can have many order items (`seller_bz.seller_id` → `order_item_bz.seller_id`)

7. **Product Categories to Products**: One category can have many products (`product_category_bz.product_category_name` → `product_bz.product_category_name`)

---

## Notes

### Data Quality

- All Bronze layer tables are subject to Great Expectations validation rules
- Records failing validation are stored in `data_quality_quarantine` table
- Validation occurs before data insertion into Bronze layer tables

### Streaming Processing

- Bronze layer: Uses Spark Structured Streaming to ingest data from landing zone
- Silver layer: Uses Delta Streaming to process changes from Bronze layer with upsert logic
- Gold layer: Uses Delta Streaming to process changes from Silver layer with enrichment transformations

### Metadata Columns

All tables include metadata columns for data lineage and audit purposes:
- `load_time`: When the record was first ingested into Bronze layer
- `source_file`: Source file path (Bronze and Silver layers)
- `update_time`: When the record was last updated (Silver and Gold layers)

### Table Naming Conventions

- **Bronze tables**: Suffix `_bz` (e.g., `customer_bz`)
- **Silver tables**: Suffix `_sl` (e.g., `order_sl`)
- **Gold tables**: Suffix `_gl` (e.g., `order_gl`)

---

## Document Version

**Version**: 1.0  

