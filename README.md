# FDA Drug Adverse Events: End-to-End ETL and Analytics Pipeline

This project involves extracting data on drug adverse events from the [FDA's open-source data platform](https://open.fda.gov/), transforming it using Python Pandas, and loading it into Snowflake for further analysis and visualization. The entire ETL process is orchestrated using Apache Airflow and Docker, with data visualization performed using Tableau Desktop.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Tools and Services Used](#tools-and-services-used)
4. [API Details](#api-details)
5. [Data Extraction](#data-extraction)
6. [Data Storage](#data-storage)
7. [Data Modeling](#data-modeling)
8. [ETL Workflow](#etl-workflow)
9. [Setup Instructions](#setup-instructions)
10. [Running the Project](#running-the-project)
11. [Data Visualization with Tableau Desktop](#data-visualization-with-tableau-desktop)
12. [Contribution](#contribution)

## Project Overview

This project involves:

- Designing an API to retrieve data from the FDA's open-source data platform.
- Building a Python script to fetch and process the data.
- Storing the data in AWS S3.
- Modeling and storing the transformed data in Snowflake.
- Orchestrating the entire ETL process using Apache Airflow.
- Visualizing the data using Tableau Desktop.

## Architecture Diagram

![Architecture_diagram](https://github.com/ravishankar324/FDA-Drug-Adverse-events-ETL-Project/assets/131810013/cdad42f3-676b-43db-a2ed-2e240e5f93c9)


## Tools and Services Used

- **Python**: For scripting and data processing.
- **Pandas**: For data manipulation and analysis.
- **AWS S3**: As a data lake for storing raw and transformed data.
- **Snowflake**: For data modeling and storage.
- **Apache Airflow**: For orchestrating the ETL workflow.
- **Docker**: For containerizing the Airflow environment.
- **Visual Studio Code**: As the development environment.
- **Tableau Desktop**: For data visualization.
- **ODBC Snowflake Driver**: For connecting Tableau to Snowflake.

## API Details

- **Base URL**: [https://api.fda.gov/drug/event.json](https://api.fda.gov/drug/event.json)
- **Search Query**: `receivedate:[20200101 TO 20201231] AND occurcountry:"US" AND _missing_:companynumb`

The API retrieves data reported directly from consumers in the United States in the year 2020 who had adverse reactions to suspected drugs.

## Data Extraction

- Build a Python script to fetch 80,000 rows using the API. Implement paging as the maximum rows the API can fetch is 1000.
- Use Pandas to extract only the required columns and handle null values.
- Handle missing values and update fields as per the data shown on the [FDA page](https://open.fda.gov/apis/drug/event/).

## Data Storage

- Use AWS S3 as a data lake.
- Create an S3 bucket with different folders for raw and cleaned data.

## Data Modeling

- Create SQL worksheets in Snowflake for designing database schema.
- Create new schemas, databases, and tables to handle the transformed data from S3.

## ETL Workflow

Create an Airflow DAG script to orchestrate the ETL process with the following tasks:

1. **Check API Availability**: Ensure the API is available.
2. **Extract Data**: Fetch FDA drug adverse events data from the API, load it into S3, and push the S3 path to Xcom for reference.
3. **Transform Data**: Pull the S3 path from Xcom, extract the raw data from S3, transform and normalize it, and load the transformed data into a different S3 folder.
4. **Create Snowflake Stage**: Set up a Snowflake stage for the transformed data.
5. **Load Data into Snowflake**: Copy the transformed data into Snowflake tables using the stage from Task 4.

## Setup Instructions

1. **Install Astro CLI**: A tool provided by Astronomer to help manage and interact with Airflow environments, allowing you to run Airflow locally in Docker containers.
2. **Install Docker Desktop**: Update the `.wslconfig` file to use the maximum CPU and RAM possible (minimum 6 processors and 6GB RAM).
3. **Update Airflow Configurations**: Modify the `.env` and `requirements.txt` files as shown in [how_to_run.docx](https://github.com/ravishankar324/FDA-Drug-Adverse-events-ETL-Project/blob/master/How_To_Run.docx) file.
4. **Start Docker Desktop**: Initialize the Astro environment using Astro commands in Visual Studio Code as detailed in [how_to_run.docx](https://github.com/ravishankar324/FDA-Drug-Adverse-events-ETL-Project/blob/master/How_To_Run.docx) file.

## Running the Project

1. **Run Airflow in Docker**: Use Astro commands as detailed in [how_to_run.docx](https://github.com/ravishankar324/FDA-Drug-Adverse-events-ETL-Project/blob/master/How_To_Run.docx) file.
2. **Access Airflow**: Navigate to [http://localhost:8080](http://localhost:8080).
3. **Navigate to Airflow Connections**: Create HTTP, AWS, and Snowflake connections.
4. **Trigger the Airflow DAG**.

## Data Visualization with Tableau Desktop

Once the data is loaded into Snowflake:

1. **Install the ODBC Snowflake Driver**: Required for Tableau Desktop to connect to Snowflake.
2. **Create an Extract Connection**: Connect Tableau Desktop to Snowflake.
3. **Data Modeling in Tableau**: Create a star schema and define relationships between the cleaned data tables.
4. **Create Visualizations**: Use Tableau Desktop to process and visualize the data.

## Dashboard
[![2020_Animation](https://github.com/user-attachments/assets/4604de80-adfe-42b6-8a63-a673257cdb1d)
](https://public.tableau.com/app/profile/ravi.shankar.p.r/viz/2020_Drug_adverse_events/Dashboard3)

> ### Checkout Tableau data visualization at [2020_Drug_adverse_events \| Tableau Public](https://public.tableau.com/app/profile/shreyas.anil.hingmire/viz/FDADrugAdverseEventsEnd-to-EndETLandAnalyticsPipeline/Dashboard3)

> ### Checkout [how_to_run.docx](https://github.com/shreyasah99/FDA-Drug-Adverse-Events-End-to-End-ETL-and-Analytics-Pipeline/blob/main/How_To_Run.docx) file for detailed steps to run this project.

> ### Checkout [S3 URI.txt](https://github.com/shreyasah99/FDA-Drug-Adverse-Events-End-to-End-ETL-and-Analytics-Pipeline/blob/main/S3%20URI.txt) to get URI's for raw and transformed data.

## Contribution
Contributions are welcome! Please open an issue or submit a pull request with any improvements.
