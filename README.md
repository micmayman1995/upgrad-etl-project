# upgrad-etl-project
- The following tasks will be carried out for this project:

    1. Extracting the transactional data from a given MySQL RDS server to HDFS(EC2) instance using Sqoop.

    2. Transforming the transactional data according to the given target schema using PySpark. 

    3. This transformed data is to be loaded to an S3 bucket.

    4. Creating the Redshift tables according to the given schema.

    5. Loading the data from Amazon S3 to Redshift tables.

    6. Performing the analysis queries.

# Sqoop
Data ingestion from RDS to HDFS using Sqoop

- Sqoop import command

- Command to see the list of imported data in HDFS

For this step, the RDS connection string and credentials are as follows:

- RDS Connection String - jdbc:mysql://upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com/testdatabase
- Username - student
- Password - STUDENT123
- Table Name - SRC_ATM_TRANS
 

# PySpark
Reading the data from the files in HDFS by a specific schema using PySpark

- Command to create an input schema using StructType (We recommend you to create a custom schema using the StructType class of PySpark, to avoid any data type mismatch.)

- Commands to read the data using the input schema created and verifying the data using the count function

Creation of dimension tables using PySpark

- Command to create a data frame for the dimension according to the target schema(dimension model) provided

- Commands to clean and transform the data:

    - Making sure that duplicate records are cleaned (Avoid duplicate info especially in the dimension tables).

    - Making sure that appropriate primary keys are present for the dimensions( You need to generate a primary key for each dimension table. For example for the 'Date' dimension one way to generate the primary key can be by adding "date" as the prefix  to the row number i.e. 'date1', 'date2' and so on.) 

    - Rearranging the fields if necessary (According to the target schema)

 

Note: Here, the tasks given above have to be done for all four dimension tables. 

- Creation of transaction fact table using PySpark

    - Commands to set proper alias for the various PySpark DataFrames before proceeding with creating the fact table (optional)

    - Commands for various stages where the original data frame is appropriately joined with the dimension tables created above

    - Commands to clean and transform the data:

        - Making sure that the appropriate primary key is present for the fact table

        - Rearranging the fields if necessary

- Loading the dimension and fact tables into Amazon S3 bucket

    - Write the DataFrames containing the dimensions and fact table directly to an S3 bucket folder. [You should create different folders on your S3 bucket for different dimensions and fact table.]

 

# Redshift
- Creation of a Redshift Cluster

    - You need to create a Redshift cluster in the same way as it was done in the Amazon Redshift module. 

- Setting up a database in the Redshift cluster and running queries to create the dimension and fact tables

    - Queries to create the various dimension and fact tables with appropriate primary and foreign keys

- Loading data into a Redshift cluster from Amazon S3 bucket

    - Queries to copy the data from S3 buckets to the Redshift cluster in the appropriate tables

- Using queries on a Redshift cluster to find the solution to the following analytical queries.

    1. Top 10 ATMs where most transactions are in the ’inactive’ state

    2. Number of ATM failures corresponding to the different weather conditions recorded at the time of the transactions

    3. Top 10 ATMs with the most number of transactions throughout the year

    4. Number of overall ATM transactions going inactive per month for each month

    5. Top 10 ATMs with the highest total amount withdrawn throughout the year 

    6. Number of failed ATM transactions across various card types

    7. Top 10 records with the number of transactions ordered by the ATM_number, ATM_manufacturer, location, weekend_flag and then total_transaction_count, on weekdays and on weekends throughout the year

    8. Most active day in each ATMs from location "Vejgaard"