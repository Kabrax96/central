# central
Automatizar información de consejo Nuevo León



# 📊 Presupuesto Data ETL Pipeline

## 🏆 **Project Overview**
This ETL (Extract, Transform, Load) pipeline automates the process of extracting presupuesto data from multiple **CSV** and **Excel** files, transforming it into a structured format, and loading it into a **PostgreSQL** database. The pipeline handles both individual and bulk file processing while maintaining detailed logs for auditing and troubleshooting.

---

## 📁 **Project Structure**

```
app/
│
├── etl_project/                      # Main ETL project directory
│   │
│   ├── assets/                        # Core ETL utilities and helper scripts
│   │   ├── __init__.py
│   │   ├── metadata_logging.py        # Metadata logging functions
│   │   ├── pipeline_logging.py        # Logging utility (Loguru-based)
│   │   ├── presupuesto_etl.py         # Extract, Transform, Load (ETL) functions
│   │
│   ├── connectors/                    # Database connectors
│   │   ├── __init__.py
│   │   ├── postgresql.py               # PostgreSQL client connector
│   │
│   ├── data/                          # Data storage
│   │   ├── presupuestos/              # Financial data files (CSV/Excel)
│   │   │   ├── Nuevo_Leon_Financials_2024_Q1_daily.csv
│   │   │   ├── Nuevo_Leon_Financials_2024_Q2_daily.csv
│   │   │   ├── Nuevo_Leon_Financials_2024_Q3_daily.csv
│   │   │   ├── Nuevo_Leon_Financials_2024_Q4_daily.csv
│   │
│   ├── logs/                          # Log storage
│   │   ├── .gitkeep                   # Placeholder to ensure directory exists in repo
│   │
│   ├── pipelines/                     # ETL orchestrators
│   │   ├── __init__.py
│   │   ├── bulk_presupuesto_pipeline.py  # ETL pipeline for multiple files
│   │   ├── bulk_presupuesto_pipeline.yaml  # Config for bulk pipeline
│   │   ├── presupuesto_pipeline.py     # ETL pipeline for a single file
│   │   ├── presupuesto_pipeline.yaml   # Config for single pipeline
│   │
│   ├── etl_project_tests/             # Unit tests for ETL project
│   │   ├── assets/
│   │   │   ├── __init__.py
│   │   │   ├── test_presupuesto_etl.py  # Tests for ETL functions
│   │   ├── connectors/
│   │   │   ├── test_postgresql.py      # Tests for PostgreSQL connection
│   │   ├── data/
│   │   │   ├── __init__.py
│
├── data/                              # Additional raw data storage
│
├── .env                               # Environment variables (DB credentials)
├── .gitignore                         # Files and folders to ignore in Git
├── Dockerfile                         # Docker setup for containerization
├── README.md                          # Project documentation
├── requirements.txt                    # Python dependencies


## ⚙️ **Setup & Installation**

### 1️⃣ **Clone the Repository:**
```bash
git clone <repo_url>
cd central
```

### 2️⃣ **Create & Activate a Virtual Environment:**
```bash
python -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate
```

### 3️⃣ **Install Dependencies:**
```bash
pip install -r requirements.txt
```

### 4️⃣ **Configure Environment Variables (.env):**
Create a `.env` file in the project root with the following content:

```env
DB_USERNAME=your_db_username
DB_PASSWORD=your_db_password
SERVER_NAME=localhost
DATABASE_NAME=your_database_name
PORT=5432
```

### 5️⃣ **Verify PostgreSQL is Running:**
Ensure that PostgreSQL is running locally or remotely and is accessible with the credentials provided in the `.env` file.

---

## 📂 **Processing Presupuesto Data**

### ✅ **Run ETL for a Single File:**
To run the ETL process for a single file:

```bash
python central/pipelines/presupuesto_pipeline.py
```

### ✅ **Run ETL for Multiple Files:**
To run the ETL process for all files in the `data/presupuesto_files/` directory:

```bash
python central/pipelines/bulk_presupuesto_pipeline.py
```

The pipeline will process all `.csv` and `.xlsx` files in the directory.

---

## 📊 **Logging System**

The pipeline uses **Loguru** for advanced logging. Logs include timestamps, log levels, status codes, and descriptive messages.

### ✅ **Log Storage:**
- Logs are stored in the `logs/` directory.
- Each pipeline run creates a **new log file** with a timestamp in its name.

### ✅ **Log Codes:**
| **Code** | **Meaning**                    |
|----------|--------------------------------|
| **100**  | Process started or in progress |
| **200**  | Success/Completion             |
| **400**  | Warning/Recoverable issue      |
| **500**  | Critical Error/Failure         |

---

## 🧮 **ETL Process Flow**

1. **Extract** — Reads data from CSV/Excel files.
2. **Transform** —
    - Cleans and formats the data.
    - Extracts metadata (e.g., year, quarter).
    - Prepares data for insertion into PostgreSQL.
3. **Load** —
    - Inserts or upserts data into the PostgreSQL database.
    - Handles conflicts using PostgreSQL's `ON CONFLICT` resolution.

---

## 💾 **PostgreSQL Table Structure:**

The financial data is loaded into the `nuevo_leon_financials` table with the following schema:

| **Column**      | **Type** |
|-----------------|----------|
| concept         | String   |
| sublabel        | String   |
| year_quarter    | String   |
| type            | String   |
| amount          | Float    |

---

## 💡 **Additional Notes:**

- **Error Handling:**
  - The pipeline logs all errors and skips problematic files during bulk operations.
  - Errors are logged with a **500** status code.

- **Performance:**
  - Log rotation set to **500 MB**.
  - Logs retained for **10 days**.

- **Scalability:**
  - The pipeline is designed to handle large datasets and multiple files.

---

## 🤝 **Contributing:**

1. Fork the repository.
2. Create a new branch (`feature/your-feature`).
3. Commit your changes.
4. Push to the branch.
5. Create a Pull Request.

---

## 📬 **Questions & Support:**

If you have any questions or run into issues, please contact the project maintainer or open an issue in the repository.
amoralesb196@gmail.com

Happy ETLing! 🚀
