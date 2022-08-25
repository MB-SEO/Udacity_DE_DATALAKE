## Udacity DE Nano Degree: Data Lake Project
This project's main purpose is to practice ETL process within AWS environment using Apache Spark Framework. <>
Following ETL process extracts data from S3 bucket -transform and load back to differnt S3 bucket with parquet file. <>
This will allow analytic teams to easily navigate datasets for their use anywhere anytime.<>

## Schema
Star schema is used and schema is ideal for OLAP (Online Analytical Processing).
- Fact Table = Songplays
- Dim 1 Table = users
- Dim 2 Table = artists
- Dim 3 Table = songs
- Dim 4 Table = time

## ETL
- Extract: Song & log data extracted from S3 location. 
- Transform: converted unix format to timestamp, datetime format for efficient use of data. Duplicates are also taken care in the process
- Load: Loaded back to public S3 bucket with write.parquet(). Datas are partitioned for faster processing capability.


## Files being used
- dl.cfg = Credentials for AWS Access
- etl.py = ETL process






