## Module 3: Data Warehouse (BigQuery)
### Learning Summary:
- Combined multiple csv/parquet files data from a cloud bucket into one big external table in BigQuery
- Created partitioned and clustered tables in BQ
- Learnt about the cost-benefit of partitioning and clustering large tables in BQ
- BigQuery ML: Built a simple linear regression model in BQ to predict how much tip a passenger is likely to pay the driver at the end of a journey
    - Hyper-parameter tuned the model and evaluated its performance
    - Exported and deployed the model using a Docker image
    - The biggest merit of ML in BQ is that it allows the us to build ML models right where the data lives in the dwh
<br>

### Resources
- [Zoomcamp BigQuery Videos & Notes](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse)
- [Machine Learning in BigQuery](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse#movie_camera-machine-learning-in-big-query)
- [Export and Deploy a BigQuery ML Model](/bq_model_deployment/deploy_model.md)