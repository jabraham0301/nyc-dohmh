## Simple Data Pipeline
### Diagram
![NYC Inspections Data-Página-2 drawio](https://user-images.githubusercontent.com/43384034/230814570-f25497b9-d30b-49d1-a8fa-7d4569f71b41.png)

This pipeline can help us to extract data from [NYC Open Data](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j) throught [Socrata Open Data API](https://dev.socrata.com/)
### Workflow Apache Spark
1. With scheduled job configured in Databricks, every day at 23:40 UTC-06:00, the script request updated data from socrata
![image](https://user-images.githubusercontent.com/43384034/230815943-86e055ce-e606-4fa3-812b-8065f701ae4c.png)


2. After data is fetched, the script create dataframe and rename some columns as we can see in this ER Diagram

![NYC Inspections Data-Página-3 drawio](https://user-images.githubusercontent.com/43384034/230815607-d48b3fb9-08f5-4dff-93ad-526f812c553c.png)

3. With our dataframe is cleaned and selected only rows we needed, the script read a table from Hive metastore, stored in Delta Lake from our Azure Databricks and merge with fetched dataframe

![image](https://user-images.githubusercontent.com/43384034/230816546-0b2ebf4e-e6de-4492-bed2-ebd1d1f85d91.png)


4. After dataframe doesn't have duplicates, save into restaurant table, overwriting all data to prevent duplicate data

### Observations
* With Pandas (nyc-extractor-pandas) notebook, we store the same data into SQL Server Table, but, it takes loooooooong time to finish this task :upside_down_face:
* The comparision of time between Spark and Pandas was:

| Spark    | Pandas      |
|----------|-------------|
| 51.72sec | 31min 23sec |

### Future changes
1. Implement Azure Blob Storage to save each day dataframe into CSV file, good backup
2. Use Apache superset to visualize data generated here (Docker image deployed in some cloud compute instance)
