import os
from dotenv import load_dotenv
from etl_project.assets.financial_etl import (
    extract_financial_data,
    transform_financial_data,
)
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, Column, String, MetaData, Float


def run_etl_for_all_files(directory_path, connection_string, table_name):
    """
    Runs the ETL process for all CSV/Excel files in a directory.

    Args:
        directory_path (str): Path to the directory containing files.
        connection_string (str): PostgreSQL connection string.
        table_name (str): Target PostgreSQL table name.
    """

    # Initialize logging
    pipeline_logging = PipelineLogging(
        pipeline_name="BulkFinancialDataPipeline", log_folder_path="./logs"
    )
    pipeline_logging.logger.info(
        f"100 | Starting bulk ETL process for files in {directory_path}"
    )

    # PostgreSQL Connection
    postgresql_client = PostgreSqlClient(
        server_name=os.getenv("SERVER_NAME"),
        database_name=os.getenv("DATABASE_NAME"),
        username=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("PORT", 5432),
    )

    # Iterate over all CSV/Excel files in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".csv") or filename.endswith(".xlsx"):
            file_path = os.path.join(directory_path, filename)
            pipeline_logging.logger.info(f"100 | Processing file: {filename}")

            try:
                # Extract
                extracted_df = extract_financial_data(file_path)
                pipeline_logging.logger.success(
                    f"200 | Data extraction completed for {filename}"
                )

                # Transform
                transformed_df = transform_financial_data(
                    extracted_df, year=2024, quarter="Q4"
                )
                pipeline_logging.logger.success(
                    f"200 | Data transformation completed for {filename}"
                )

                # Define PostgreSQL table
                metadata = MetaData()
                table = Table(
                    table_name,
                    metadata,
                    Column("concept", String),
                    Column("sublabel", String),
                    Column("year_quarter", String),
                    Column("type", String),
                    Column("amount", Float),
                )

                # Load
                postgresql_client.upsert(
                    data=transformed_df.to_dict(orient="records"),
                    table=table,
                    metadata=metadata,
                )
                pipeline_logging.logger.success(
                    f"200 | Data loaded into PostgreSQL for {filename}"
                )

            except Exception as e:
                pipeline_logging.logger.error(
                    f"500 | ETL process failed for {filename}: {e}"
                )
                continue

    pipeline_logging.logger.info("200 | Bulk ETL process completed for all files.")


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()

    # PostgreSQL connection string
    connection_string = f"postgresql+pg8000://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('SERVER_NAME')}:{os.getenv('PORT')}/{os.getenv('DATABASE_NAME')}"

    # Directory containing multiple CSV/Excel files
    directory_path = "./data/financial_files"

    # PostgreSQL table where data will be loaded
    table_name = "nuevo_leon_financials"

    # Run ETL for all files
    run_etl_for_all_files(directory_path, connection_string, table_name)
