# central
Automatizar información de consejo Nuevo León



# 📊 Presupuesto Data ETL Pipeline

## 🏆 **Project Overview**
This ETL (Extract, Transform, Load) pipeline automates the process of extracting presupuesto data from multiple **CSV** and **Excel** files, transforming it into a structured format, and loading it into a **PostgreSQL** database. The pipeline handles both individual and bulk file processing while maintaining detailed logs for auditing and troubleshooting.

---

## 📁 **Project Structure**

```
etl_project/
│
├── assets/                           # Core ETL functions and utilities
│   ├── presupuesto_etl.py            # Extract, Transform, Load functions
│   └── pipeline_logging.py           # Logging utility (Loguru-based)
│
├── connectors/                       # Database connectors
│   └── postgresql.py                 # PostgreSQL client connector
│
├── pipelines/                        # ETL orchestrators
│   ├── presupuesto_pipeline.py       # ETL for single file
│   └── bulk_presupuesto_pipeline.py  # ETL for processing multiple files
│
├── data/                             # Raw data files
│   └── financial_files/              # Contains all CSV/Excel files
│
├── logs/                             # Generated logs from ETL process
│   └── pipeline_logging<timestamp>.log
│
├── .env                              # Environment variables (DB credentials)
│
└── requirements.txt                  # Project dependencies
```

---

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
