# TasteHub Data Pipeline

Welcome to the TasteHub Data Pipeline project repository! This project simulates a real-time sales data processing pipeline for TasteHub, a fictional beverage and snack company. It leverages modern data engineering tools and techniques to achieve seamless data processing, visualization, and deployment.

## Project Overview

TasteHub Data Pipeline focuses on processing sales data streaming from various points of sale across Belgium, providing both real-time insights and historical analysis. We have incorporated a comprehensive suite of tools and technologies to maximize efficiency and scalability.

## Key Features

- **Real-Time Data Processing:** Uses Kafka for data streaming and PySpark for distributed processing to handle large datasets efficiently.
- **Workflow Orchestration:** Utilizes Apache Airflow to manage and automate complex workflows.
- **Data Storage:** Employs a SQL database to store and query historical sales data for in-depth analysis.
- **Web Interface:** A Streamlit app provides interactive visualizations and dashboards for data exploration and insights.
- **Cloud Deployment:** The entire pipeline is deployed on AWS, taking advantage of cloud scalability and reliability.
- **Containerization:** Docker ensures environment consistency and simplifies deployment processes.

## Technology Stack

- **PySpark**
- **Kafka**
- **Airflow**
- **SQL Database**
- **Streamlit**
- **Docker**
- **AWS**

## Skills Highlighted

- Data Engineering
- Real-Time Data Processing
- Cloud Deployment
- Data Visualization
- DevOps Practices

## Getting Started

### Prerequisites

- **Docker and Docker Compose:** Ensure these are installed on your machine. [Download here](https://www.docker.com/get-started).
- **Basic Command-Line Knowledge:** Familiarity with running commands in a terminal.

### Setup Instructions

Follow these steps to set up and run the TasteHub Data Pipeline locally:

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/ChristianValery/tastehub-data-pipeline.git
   cd tastehub-data-pipeline
   ```

2. **Configure Environment Variables:**
   - Create a `.env` file in the project root directory.
   - Add the following content, replacing placeholders with your preferred credentials:
     ```
     POSTGRES_USER=youruser
     POSTGRES_PASSWORD=yourpassword
     POSTGRES_DB=yourdb
     PGADMIN_DEFAULT_EMAIL=admin@example.com
     PGADMIN_DEFAULT_PASSWORD=adminpassword
     KAFKA_BOOTSTRAP_SERVERS=kafka:9092
     ```

3. **Build Docker Images:**
   - Run the following command to build all necessary Docker images:
     ```bash
     docker-compose build
     ```

4. **Run Initialization Tasks:**
   - **Set Up the Database:**
     ```bash
     docker-compose run --rm db_setup
     ```
   - **Generate Historical Transactions:**
     ```bash
     docker-compose run --rm historical_data
     ```
   - *Note:* Ensure these tasks complete successfully before proceeding.

5. **Start Continuous Services:**
   - Launch all services in the background:
     ```bash
     docker-compose up -d
     ```

6. **Access the Dashboard:**
   - Open your browser and navigate to `http://localhost:8501` to explore the Streamlit app with interactive visualizations.

7. **Explore the Database with pgAdmin:**
   - Go to `http://localhost:5050` in your browser.
   - Log in using the email and password specified in your `.env` file (e.g., `admin@example.com` and `adminpassword`).
   - Add a new server with these details:
     - **Host:** `postgres`
     - **Port:** `5432`
     - **Username:** `youruser`
     - **Password:** `yourpassword`
     - **Database:** `yourdb`

8. **Stopping the Pipeline:**
   - To stop all services gracefully:
     ```bash
     docker-compose down
     ```
   - To stop services and remove all data (use with caution):
     ```bash
     docker-compose down -v
     ```

### Notes

- Complete the initialization tasks (step 4) before starting continuous services (step 5) to ensure proper setup.
- If you encounter issues, view service logs with:
  ```bash
  docker-compose logs <service_name>
  ```
- For additional setup details, refer to `docs/installation.md`.

## Cloud Deployment

This project supports deployment on AWS for scalability. For instructions, see `docs/deployment.md`.

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.

## Contributions

We welcome contributions and feedback to enhance this project. Please feel free to open issues or submit pull requests.

## Contact

For queries or support, please contact [Christian Valéry](mailto:c.nguembou@gmail.com).
