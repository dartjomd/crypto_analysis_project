# Crypto ETL & Analytics Pipeline

A professional-grade ETL (Extract, Transform, Load) pipeline designed to ingest, process, and analyze cryptocurrency market data. This project demonstrates a full-cycle data engineering workflow: from handling asynchronous API rate limits to generating visual market insights.

## Tech Stack
* **Language:** Python 3.11
* **Data Processing:** Pandas
* **Asynchronous I/O:** Asyncio, Aiohttp
* **Database:** MySQL 8.0
* **Visualization:** Matplotlib
* **Testing:** Pytest (with Unit & Mock testing)
* **Infrastructure:** Docker, Docker Compose



## Architecture
The system is built with a modular architecture to ensure scalability:
1.  **Extractor (Async):** High-performance asynchronous data fetching. Implements **Semaphores** to strictly respect API Rate Limits and prevent IP blocking.
2.  **Transformer:** A data cleaning layer using Pandas for schema normalization, type conversion, and handling missing values.
3.  **Loader:** Handles reliable ingestion into MySQL, ensuring data consistency for downstream analytics.
4.  **Analyzer:** A specialized module that executes complex analytical queries using **SQL Window Functions** (`DENSE_RANK`, `LAG`, `OVER`) to extract market trends.
5.  **Visualizer:** A reporting layer that generates automated price charts and volatility plots using Matplotlib.

## Key Engineering Features
* **Concurrency Control:** Solved API throttling challenges using `asyncio` semaphores, allowing efficient data collection without being banned.
* **Advanced SQL Analytics:** Implemented business logic (asset ranking and volatility calculation) directly in SQL for performance optimization.
* **Automated Reporting:** The pipeline automatically generates visual trend reports (`.png` plots) from the analyzed data.
* **Robust Testing:** Critical logic is covered by a test suite utilizing `unittest.mock` to simulate API responses and database interactions.
* **Containerized Deployment:** The entire stack is fully orchestrated via Docker, ensuring seamless deployment and "it works on my machine" reliability.

## Getting Started

### Prerequisites
* [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/) installed.

### Installation & Run
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/dartjomd/crypto_analysis_project.git](https://github.com/dartjomd/crypto_analysis_project.git)
    cd crypto-etl-pipeline
    ```

2.  **Launch the pipeline:**
    This command builds the images and starts both the MySQL database and the ETL application:
    ```bash
    docker-compose up --build
    ```

3.  **Run Tests:**
    Execute the test suite inside the container environment:
    ```bash
    docker-compose run crypto_etl pytest
    ```



## Analytical Insights
The pipeline provides deep market analytics:
* **Market Spikes:** Detects significant price/volume jumps using ranking logic.
* **Volatility Analysis:** Tracks period-over-period price fluctuations via Window Functions.
* **Visual Trends:** Generates automated plots for monthly price dynamics.

## Roadmap
- [ ] Integration with Apache Airflow for DAG orchestration.
- [ ] Real-time dashboard using Grafana or Streamlit.