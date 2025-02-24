import os
from dotenv import load_dotenv
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from etl_project.assets.financial_etl import extract_financial_data, transform_financial_data
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.connectors.postgresql import PostgreSqlClient

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # Initialize logging with proper folder and pipeline name
    pipeline_logging = PipelineLogging(pipeline_name="FinancialDataPipeline", log_folder_path="./logs")
    pipeline_logging.logger.info("100 | Starting ETL pipeline for Financial Data")

    # Load environment variables
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    SERVER_NAME = os.getenv("SERVER_NAME")
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    PORT = os.getenv("PORT", 5432)

    try:
        # PostgreSQL Connection
        pipeline_logging.logger.info("100 | Initializing PostgreSQL client")
        postgresql_client = PostgreSqlClient(
            server_name=SERVER_NAME,
            database_name=DATABASE_NAME,
            username=DB_USERNAME,
            password=DB_PASSWORD,
            port=PORT,
        )
        pipeline_logging.logger.success("200 | PostgreSQL client initialized successfully")

        # Extract
        file_path = './data/F4_Balance_Presupuestario_LDF_4T2024.xlsx'
        pipeline_logging.logger.info(f"100 | Extracting data from {file_path}")
        extracted_df = extract_financial_data(file_path)
        pipeline_logging.logger.success("200 | Data extraction completed")

        # Transform
        pipeline_logging.logger.info("100 | Transforming data")
        transformed_df = transform_financial_data(extracted_df, year=2024, quarter='Q4')
        pipeline_logging.logger.success("200 | Data transformation completed")

        # Define PostgreSQL table
        metadata = MetaData()
        table = Table(
            "nuevo_leon_financials",
            metadata,
            Column("concept", String),
            Column("sublabel", String),
            Column("year_quarter", String),
            Column("type", String),
            Column("amount", Float),
        )

        # Load
        pipeline_logging.logger.info("100 | Loading data into PostgreSQL")
        postgresql_client.upsert(data=transformed_df.to_dict(orient='records'), table=table, metadata=metadata)
        pipeline_logging.logger.success("200 | Data successfully loaded into PostgreSQL")

        pipeline_logging.logger.info("200 | ETL pipeline completed successfully")

    except Exception as e:
        pipeline_logging.logger.error(f"500 | ETL pipeline failed: {e}")
        raise
