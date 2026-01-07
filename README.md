# Crypto ELT & Analytics Pipeline

A high-performance **ELT (Extract, Load, Transform)** pipeline designed to ingest, store, and analyze cryptocurrency market data. Unlike traditional ETL, this project leverages the power of the database engine to perform complex transformations using SQL Window Functions, followed by automated data visualization.

## 🛠 Tech Stack
* **Language:** Python 3.11
* **Data Ingestion:** Asyncio, Aiohttp (Asynchronous API fetching)
* **Storage:** MySQL 8.0 (Relational Database)
* **Data Processing:** Pandas (Lightweight cleaning) & SQL (Core transformations)
* **Visualization:** Matplotlib
* **Testing:** Pytest (Unit & Mock testing)
* **Infrastructure:** Docker, Docker Compose



## Architecture: The ELT Approach
The project follows a modern ELT pattern, prioritizing raw data integrity and in-database processing:

1.  **Extract:** Asynchronous fetching from crypto APIs. Uses **Semaphores** to handle Rate Limits and ensure reliable data stream.
2.  **Load:** Ingests data directly into MySQL with minimal pre-processing. This preserves the original data lineage.
3.  **Transform (In-Database):** The core analytical logic is executed via the **Analyzer** module using advanced **SQL Window Functions** (`DENSE_RANK`, `LAG`, `OVER`). This offloads heavy computations to the database engine.
4.  **Visualize:** An automated reporting layer using **Matplotlib** to generate trend charts and volatility plots from the transformed data.

## Key Engineering Features
* **Asynchronous Concurrency:** Managed high-frequency API calls without IP blocking using Python's `asyncio` and `aiohttp`.
* **Database-Centric Analytics:** Implemented complex market metrics (volatility, price rankings) directly in SQL for maximum efficiency.
* **Automated Visualization:** Automatically generates `.png` reports, providing visual insights into price movements.
* **Robust Testing Suite:** Comprehensive coverage using `pytest` and `unittest.mock` to ensure pipeline stability.
* **Full Containerization:** Easy deployment with a single `docker-compose up --build` command.

## Getting Started

### Prerequisites
* [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) installed.

### Installation & Run
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/dartjomd/crypto_analysis_project.git](https://github.com/dartjomd/crypto_analysis_project.git)
    cd crypto_analysis_project
    ```

2.  **Launch the pipeline:**
    ```bash
    docker-compose up --build
    ```

3.  **Run Tests:**
    ```bash
    docker-compose run crypto_etl pytest
    ```

## Analytical Insights
The ELT approach allows for flexible and powerful analysis:
* **Market Spikes:** Real-time ranking of assets by price performance.
* **Volatility Tracking:** Period-over-period change analysis using windowing.
* **Visual Reports:** Generated plots located in the output directory (e.g., monthly price trends).
