import pandas as pd
from sqlalchemy import MetaData, Table
import os
from etl_project.connectors.postgresql import PostgreSqlClient


def extract_financial_data(year: int, quarter: str) -> pd.DataFrame:
    """
    Extracts financial data for a given year and quarter from a CSV file.

    Usage example:
        extract_financial_data(year=2024, quarter="Q1")

    Args:
        year: The year of the financial data (e.g., 2024).
        quarter: The quarter ('Q1', 'Q2', 'Q3', 'Q4').

    Returns:
        A DataFrame containing the extracted data, or None if the file does not exist.

    Raises:
        FileNotFoundError: If the specified file does not exist.
    """

    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(
        base_dir, "..", "data", "presupuestos"
    )  # Go up to etl_project/data/presupuestos
    file_path = os.path.join(
        data_dir, f"Nuevo_Leon_Financials_{year}_{quarter}_daily.csv"
    )

    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        return df
    else:
        print(f"File {file_path} not found.")
        return None


def transform_financial_data(df):
    """
    Transforms financial data by standardizing columns and cleaning values.

    Usage example:
        transform_financial_data(df)

    Args:
        df: The input DataFrame containing financial data.

    Returns:
        A DataFrame with standardized and cleaned data.

    Raises:
        ValueError: If the DataFrame contains invalid data that cannot be processed.
    """
    # Datetime standardization
    # df["Date"] = pd.to_datetime(df["Date"], errors="raise").dt.strftime("%Y-%m-%d")

    # df["Date"] = pd.to_datetime(
    #    df["Date"], format="%d-%m-%Y", errors="coerce"
    # ).dt.strftime("%Y-%m-%d")

    def parse_date(date_str):
        for fmt in ("%Y/%m/%d", "%d-%m-%Y"):
            try:
                return pd.to_datetime(date_str, format=fmt)
            except ValueError:
                continue
        return pd.NaT  # Return floating-point NaT for invalid dates

    # df["Date"] = df["Date"].apply(parse_date).dt.strftime("%Y-%m-%d")
    df["Date"] = df["Date"].apply(parse_date)
    df["Quarter"] = df["Date"].dt.to_period("Q").astype(str)

    # VarChar standardization
    df["Currency"] = (
        df["Currency"]
        .str.lower()
        .replace(
            {
                "pesos": "MXN",
                "mex$": "MXN",
                "mex": "MXN",
                "pesoss": "MXN",
                "mxn": "MXN",
            }
        )
    )

    # Numeric column standardization
    def clean_currency_values(value):
        try:
            value = (
                str(value)
                .replace(",", "")
                .replace("$", "")
                .replace("MXN", "")
                .replace("mex$", "")
                .replace("pesos", "")
                .replace("MEX$", "")
                .replace("MEX", "")
            )
            return float(value) if value else None
        except Exception as e:
            print(f"Error cleaning value: {value} - {e}")
            return None

    df["Revenue"] = df["Revenue"].apply(clean_currency_values)
    df["Expenses"] = df["Expenses"].apply(clean_currency_values)
    df["Tax Income"] = df["Tax Income"].apply(clean_currency_values)
    df["Debt"] = df["Debt"].apply(clean_currency_values)

    df["Revenue"] = df["Revenue"].astype("float64", errors="ignore")
    df["Expenses"] = df["Expenses"].astype("float64", errors="ignore")
    df["Tax Income"] = df["Tax Income"].astype("float64", errors="ignore")
    df["Debt"] = df["Debt"].astype("float64", errors="ignore")

    def clean_gdp_contribution(value):
        try:
            return float(value.replace("%", "")) if value else None
        except:
            return None

    df["GDP Contribution"] = df["GDP Contribution"].apply(clean_gdp_contribution)
    df = df.rename(columns={"GDP Contribution": "GDP Contribution Percentage"})
    df.columns = df.columns.str.upper().str.replace(" ", "_")

    return df


def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "upsert",
) -> None:
    """
    Load dataframe to a database.

    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )
