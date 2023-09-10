# Formula_One_Data_Integration_Transformation_and_Visualisation

Our datawarehousing model of the solution :

![modele_DW](https://github.com/mohamedkanfoudi/Formula_One_Data_Integration_Transformation_and_Visualisation/assets/76444482/ed7b6915-316a-4855-b948-0d50ab9711f7)

We execute our pipelines in Azure Data Factory to load all json files from the website to our container "raw" inside Azure Data Lake Gen 2 :

![DataCopy](https://github.com/mohamedkanfoudi/Formula_One_Data_Integration_Transformation_and_Visualisation/assets/76444482/fd4992dc-2f6b-4371-b0ae-5cc3183300c9)

We schedule the execution of our transformations when a new data arrived:

![DataTransform](https://github.com/mohamedkanfoudi/Formula_One_Data_Integration_Transformation_and_Visualisation/assets/76444482/45c5aee5-3c0e-456a-b708-d079a7d345e9)

![raw_container](https://github.com/mohamedkanfoudi/Formula_One_Data_Integration_Transformation_and_Visualisation/assets/76444482/ed0d9b7d-da44-4428-8f8a-27932111d1c8)

We realize a dashboard with the "Charts" option in Azure Databricks :

![Dashboard_nbr_points_by_driver_name](https://github.com/mohamedkanfoudi/Formula_One_Data_Integration_Transformation_and_Visualisation/assets/76444482/d7eea86f-843b-4b6d-a2f7-b5b5fa0c4161)

Our Database called : fromula1_db 
We import two tables for reporting and visualisation in Power BI
- Table 1 :driver_standings
- Table 2 : race_results

![power_bi](https://github.com/mohamedkanfoudi/Formula_One_Data_Integration_Transformation_and_Visualisation/assets/76444482/354d2c34-6ef3-4632-8362-d5f76e26e8ae)

Report_power_bi : visualisation of 
- nombre of fastest_lab per driver_nationamlity
- sum of total points per driver_nationality
![report_power_bi](https://github.com/mohamedkanfoudi/Formula_One_Data_Integration_Transformation_and_Visualisation/assets/76444482/01a298f3-a064-4bc2-a9b7-046ed5c9b658)
