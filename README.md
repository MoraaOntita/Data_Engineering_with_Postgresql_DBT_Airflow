# Data_Engineering_with_Postgresql_DBT_Airflow

A techstack is a combination of programming languages, frameworks, libraries, tools and other technologies used to develop a software application or system. It is the building blocks that developers use to create and maintain the functionalities of their application

In data engineering this typically involves a combination of tools and technologies tailored for handling large volumes of data, processing, storing and analyzing it. The tech stack used in this data engineering project are:


- Data Ingestion: data is collected from pNEUMA  which is downloaded as a csv file in your local machine.
- Data Storage: Data is stored in PostgreSQL database (or data warehouse) 
- Data Orchestration: Apache-airflow has been used to orchestrate data workflows, scheduling data pipelines and managing dependencies between different processing tasks.
- Data Preprocessing: DBT is used for transforming data. It can perform complex data transformations, aggregations and analytics at a scale.
- Data Visualization and Analysis: After data is processed visualization can be done using tools like Redash that helps in creating dashboards, reports and visualizations to derive insights from the data
- Infrastructure: Cloud platforms like AWS, Google Cloud Platform, Azure provide scalable infrastructure services such as compute, storage and networking which are essential for deploying and running the data engineering pipelines. Also docker and docker orchestration platforms like Kubernetes maybe used for managing and scaling data processing workloads.
