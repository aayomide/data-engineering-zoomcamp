 ## Module 1: Tools & Environment Setup
 ### Learning Summary
- Set up and configured needed data engineering tools & environments, including Python/Anaconda, WSL/Ubuntu 20.04, Google Cloud SDK, Git/Gitbash, Docker with docker-compose, Terraform, Postgres, GCP (with key services such as Google Cloud Storage, BigQuery, and Service Accounts)
- Wrote Terraform scripts to set up and configure essential services (GCS and BQ) on the GCP account
- Created data ingestion scripts in Python to download and load the NY Taxi Trips and Zone datasets into a PostgreSQL database
- Containerized the data ingestion scripts using Docker with docker-compose
- Developed SQL queries to explore the contents of the datasets loaded into the PostgreSQL database
<br>

### Resources
- WSL Installation Guide: [Running Linux on Windows](https://learn.microsoft.com/en-us/windows/wsl/install)
- [Installing `wget` on windows](https://www.jcchouinard.com/wget-install-windows/)
- [Creating and Using a Virtual Environment in Python](https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/#create-and-use-virtual-environments)
- [Data Zoomcamp Docker-Terraform Notes](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform)
    - [Docker Course Prep Note](https://docs.google.com/document/d/e/2PACX-1vRJUuGfzgIdbkalPgg2nQ884CnZkCg314T_OBq-_hfcowPxNIA0-z5OtMTDzuzute9VBHMjNYZFTCc1/pub)
- [MichaelShoemaker's GCP VM Setup Steps](https://github.com/MichaelShoemaker/shoemaker-de-zoomcamp-final-project/blob/main/GitLikeMe.md)
- [NY Taxi Data (CSV)](https://github.com/DataTalksClub/nyc-tlc-data)