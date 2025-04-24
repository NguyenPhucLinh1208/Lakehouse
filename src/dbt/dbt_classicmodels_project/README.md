# DBT Classicmodels Project

## Overview

This DBT project transforms the classicmodels sample data (loaded into PostgreSQL)
into staging models and core data marts (dimensions and facts) ready for analytics.

## Prerequisites

* Python 3.8+
* pip
* Git
* Access to a PostgreSQL database with the classicmodels data loaded (e.g., in the `public` schema).
* DBT Core and dbt-postgres adapter installed (`pip install dbt-core dbt-postgres`).
* Environment variables set for database credentials (see `profiles.yml` section).

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd dbt_classicmodels_project
    ```
2.  **Configure Profiles:**
    Ensure you have a `profiles.yml` file in your `~/.dbt/` directory with a profile named `dbt_classicmodels_project` pointing to your PostgreSQL instance. Set the required environment variables (e.g., `DBT_USER_DEV`, `DBT_PASSWORD_DEV`).
    *See `profiles.yml` example in documentation or ask project maintainer.*
3.  **Install Dependencies:**
    ```bash
    dbt deps
    ```

## Running the Project

* **Check connection:** `dbt debug`
* **Run all models:** `dbt run`
* **Run staging models only:** `dbt run --select staging`
* **Run mart models only:** `dbt run --select marts`
* **Run tests:** `dbt test`
* **Generate documentation:** `dbt docs generate`
* **View documentation:** `dbt docs serve`

## Project Structure

* `models/staging`: Raw data cleaning and renaming.
* `models/marts`: Business-focused dimension and fact tables.
* `macros/`: Reusable code snippets.
* ... (briefly explain other folders) ...

## Key Models

* `dim_customers`: Customer dimension.
* `fct_orders`: Order items fact table.
* ...
