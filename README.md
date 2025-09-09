# 🛒 Walmart Sales Data Pipeline using PySpark  

This project demonstrates an end-to-end **data analysis pipeline** for Walmart sales data using **PySpark**.  
The pipeline covers **data ingestion, cleaning, transformation, and analysis** to extract key business insights.  

---

## 📌 Project Overview  

Retail businesses like Walmart generate massive amounts of transaction and customer data daily.  
This project leverages **PySpark** to process and analyze Walmart sales data efficiently.  

The pipeline answers business-critical questions such as:  
- How many unique customers are there?  
- What are the sales trends across states and months?  
- Which products and categories are most popular?  
- Who are the top customers by expenditure?  
- How can we generate a detailed customer purchase report?  

---

## ⚙️ Tech Stack  

- **Programming Language:** Python  
- **Framework:** PySpark  
- **Environment:** Jupyter / Colab Notebook  
- **Data Source:** CSV files (`customers.csv`, `salestxns.csv`)  

---

## 📂 Dataset Description  

### **1. Customers Dataset (`customers.csv`)**  
| Column       | Type    | Description                  |  
|--------------|---------|------------------------------|  
| Customer Id  | Integer | Unique ID for each customer |  
| Name         | String  | Customer full name          |  
| City         | String  | City of customer            |  
| State        | String  | State of customer           |  
| Zip Code     | Integer | ZIP code                    |  

### **2. Sales Transactions Dataset (`salestxns.csv`)**  
| Column        | Type    | Description                                |  
|---------------|---------|--------------------------------------------|  
| Sales         | Integer | Total sales amount of transaction         |  
| Txn Id        | Integer | Transaction ID                            |  
| Category Name | String  | Product category                          |  
| Product Id    | Integer | Unique product ID                         |  
| Product Name  | String  | Product name                              |  
| Price         | Double  | Price of the product                      |  
| Quantity      | Integer | Number of units purchased                 |  
| Customer Id   | Integer | Foreign key linking to `customers.csv`    |  

---

## 🚀 Project Workflow  

### **Step 1: Import Dependencies and Create Spark Session**  
- Install PySpark  
- Create SparkSession  
- Define schemas for structured data ingestion  

### **Step 2: Load Datasets into PySpark DataFrames**  
- Load `customers.csv` and `salestxns.csv` using defined schemas  
- Clean and rename columns for consistency  

### **Step 3: Data Exploration & Cleaning**  
- Preview customers and sales data  
- Ensure proper data types  
- Standardize column names  

### **Step 4: Business Questions & Analysis**  

#### **Q1. Total Number of Customers**  
Count unique customers in the dataset.  

#### **Q2. Total Sales by State**  
Join datasets and calculate total sales grouped by state.  

#### **Q3. Top 10 Most Purchased Products**  
Find the top 10 products based on total quantity sold.  

#### **Q4. Average Transaction Value**  
Calculate the average transaction price.  

#### **Q5. Top 5 Customers by Expenditure**  
Find customers who spent the most.  

#### **Q6. Product Purchases by a Specific Customer**  
List all products purchased by a customer (e.g., Customer ID = 256).  

#### **Q7. Monthly Sales Trends**  
Analyze monthly sales trends (requires a `Date` column).  

#### **Q8. Category with Highest Sales**  
Find the category generating the highest total sales revenue.  

#### **Q9. State-wise Sales Comparison**  
Compare sales between two states (e.g., **Texas vs. Ohio**).  

#### **Q10. Detailed Customer Purchase Report**  
Generate a report showing each customer’s:  
- Total purchases  
- Number of transactions  
- Average transaction value




