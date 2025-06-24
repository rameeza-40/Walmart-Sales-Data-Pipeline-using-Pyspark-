!pip install pyspark

from IPython import get_ipython
from IPython.display import display

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder.appName("Walmart Sales Data Pipeline using Pyspark ").getOrCreate()

customer_schema = StructType([
    StructField("Customer Id", IntegerType(), nullable=False),
    StructField("Name", StringType(),nullable=False),
    StructField("City", StringType(), nullable=False),
    StructField("State", StringType(), nullable=False),
    StructField("Zip Code", IntegerType(), nullable=False)
])

sales_schema = StructType([
    StructField("Sales", IntegerType(), nullable=False),
    StructField("Txn Id", IntegerType(), nullable=False),
    StructField("Category Name", StringType(), nullable=False),
    StructField("Product Id", IntegerType(), nullable=False),
    StructField("Product Name", StringType(), nullable=False),
    StructField("Price", DoubleType(), nullable=False),
    StructField("Quantity", IntegerType(), nullable=False),
    StructField("Customer Id", IntegerType(), nullable=False)
])

customers_df = spark.read.csv("/content/customers.csv", header=True,  encoding="utf-8", schema=customer_schema)

salestxns_df = spark.read.csv("/content/salestxns.csv", header=True,  encoding="utf-8", schema=sales_schema)

customers_df = customers_df.withColumnRenamed("Customer Id", "Customer_id")
salestxns_df = salestxns_df.withColumnRenamed("Customer Id", "Customer_id")


print("Customers Data:")
customers_df.show(truncate=False)
print("Sales Transactions Data:")
salestxns_df.show(truncate=False)
print("Customers Data:")


**Question No:1  Total Number of Customers:**


            How many unique customers are there in the dataset?


total_customers = spark.sql("Select count(distinct Customer_id) as total_customers from customers_df")
total_customers.show()

**Question No:2 Total Sales by State**

            What is the total sales amount for each state?
        
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

joined_df = salestxns_df.join(customers_df, on="Customer_id", how="inner")

total_sales_by_state = joined_df.groupBy("State").agg(sum("Sales").alias("Total Sales"))

print("Total Sales by State:")
total_sales_by_state.show()

**Question No:3 Top 10 Most Purchased Products:**

            Which are the top 10 most purchased products based on the quantity sold?

from pyspark.sql.functions import sum, desc

product_sales = salestxns_df.groupBy("Product Name").agg(sum("Quantity").alias("Total Quantity Sold"))

top_10_products = product_sales.orderBy(desc("Total Quantity Sold")).limit(10)

print("Top 10 Most Purchased Products:")
top_10_products.show(truncate=False)

**Question No:4 Average Transaction Value:**

            What is the average price of transactions across all sales?

from pyspark.sql.functions import avg

average_transaction_value = salestxns_df.agg(avg("Price").alias("Average Transaction Value"))

print("Average Transaction Value:")

average_transaction_value.show()

**Question No:5 Top 5 Customers by Expenditure:**

           Who are the top 5 customers by total amount spent?

from pyspark.sql.functions import sum, desc

top_5_customers = salestxns_df.groupBy("Customer_id").agg(sum("Sales").alias("Total Amount Spent"))

top_5_customers = top_5_customers.orderBy(desc("Total Amount Spent")).limit(5)

print("Top 5 Customers by Expenditure:")
top_5_customers.show()

**Question No:6 Product Purchases by a Specific Customer:**

List all products purchased by a specific customer (e.g., customer with ID 256),
including the product name, quantity, and total amount spent.

customer_id_to_analyze = 256

customer_purchases = salestxns_df.filter(salestxns_df["Customer_id"] == customer_id_to_analyze)

customer_purchases_report = customer_purchases

print(f"Products purchased by customer with ID {customer_id_to_analyze}:")
customer_purchases_report.show(truncate=False)

**Question No:7 Monthly Sales Trends:**
Assuming there is a date field, analyze the sales trends over the months. Which month had the highest sales?

from pyspark.sql.functions import month, sum, asc, desc

date_column_name = "Date"

if date_column_name in salestxns_df.columns:
    monthly_sales = salestxns_df.withColumn("Month", month(date_column_name))
    monthly_sales_trend = monthly_sales.groupBy("Month").agg(sum("Sales").alias("Total Monthly Sales"))
    monthly_sales_trend = monthly_sales_trend.orderBy(asc("Month"))

    print("Monthly Sales Trends:")
    monthly_sales_trend.show()

    month_with_highest_sales = monthly_sales.groupBy("Month").agg(sum("Sales").alias("Total Monthly Sales"))
    month_with_highest_sales = month_with_highest_sales.orderBy(desc("Total Monthly Sales")).limit(1)

    print("Month with the Highest Sales:")
    month_with_highest_sales.show()

else:
    print(f"Date column '{date_column_name}' not found in salestxns_df. Cannot analyze monthly sales trends.")
    print("Please ensure your salestxns_df has a date column with the correct name.")

**Question No:8 Category with Highest Sales:**

            Which product category generated the highest total sales revenue?

from pyspark.sql.functions import sum, desc

category_sales = salestxns_df.groupBy("Category Name").agg(sum("Sales").alias("Total Category Sales"))

category_with_highest_sales = category_sales.orderBy(desc("Total Category Sales")).limit(1)

print("Category with Highest Sales:")

category_with_highest_sales.show(truncate=False)

**Question No:9 State-wise Sales Comparison:**

Compare the total sales between two specific states (e.g., Texas vs. Ohio). Which state had higher sales?

from pyspark.sql.functions import col, sum as spark_sum

joined_df = salestxns_df.join(customers_df, on="Customer_id", how="inner")

joined_df = joined_df.withColumn("total_amount", col("Price") * col("Quantity"))

filtered_states = joined_df.filter(col("State").isin("TX", "OH"))

sales_comparison = filtered_states.groupBy("State") \
                                  .agg(spark_sum("total_amount").alias("Total_Sales"))

sales_comparison.orderBy(col("Total_Sales").desc()).show()

**Question No:10 Detailed Customer Purchase Report:**

Generate a detailed report showing each customer along with their total purchases, the total number of transactions they have made, and the average transaction value.

from pyspark.sql.functions import col, sum as spark_sum, count as spark_count, round

joined_df = salestxns_df.join(customers_df, on="Customer_id", how="inner") 

joined_df = joined_df.withColumn("total_amount", col("Price") * col("Quantity"))

customer_report = joined_df.groupBy("Customer_id", "Name") \
    .agg(
        spark_sum("total_amount").alias("Total_Purchases"),
        spark_count("Txn Id").alias("Total_Transactions"),
        round((spark_sum("total_amount") / spark_count("Txn Id")), 2).alias("Avg_Transaction_Value")
    )

customer_report.orderBy(col("Total_Purchases").desc()).show(truncate=False)
