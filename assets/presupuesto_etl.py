import pandas as pd
from sqlalchemy import create_engine, MetaData

# Extraction Function
def extract_financial_data(file_path):
    """Extracts data from the provided Excel file."""
    excel_data = pd.ExcelFile(file_path)
    df = excel_data.parse(excel_data.sheet_names[0])
    return df

# Transformation Function
def transform_financial_data(df, year, quarter):
    """Cleans and structures the extracted financial data."""
    df = df.drop(index=range(0, 5)).reset_index(drop=True)
    df.dropna(subset=['Unnamed: 1'], inplace=True)
    df.columns = ['index', 'concept', 'estimated_approved', 'accrued', 'collected_paid']
    df.drop(columns=['index'], inplace=True)
    df['year_quarter'] = pd.to_datetime(f"{year}-01-01") + pd.offsets.QuarterEnd(int(quarter[-1]))
    df['sublabel'] = df['concept'].str.extract(r'^(\\b[A-Z]\\d+)\\.')
    df['concept'] = df['concept'].str.replace(r'^[A-Z]\\d*\\.\\s*', '', regex=True).str.strip()
    df = df[df['concept'].str.lower() != 'concepto']
    df = df[df['sublabel'].notnull()]
    df_melted = df.melt(id_vars=['concept', 'sublabel', 'year_quarter'],
                        var_name='type',
                        value_name='amount')
    return df_melted

# Load Function
def load_to_postgresql(df, connection_string, table_name, load_method="upsert"):
    """Loads the transformed data into a PostgreSQL table."""
    engine = create_engine(connection_string)
    metadata = MetaData(bind=engine)
    connection = engine.connect()

    if load_method == "insert":
        df.to_sql(table_name, con=connection, if_exists='append', index=False)
    elif load_method == "upsert":
        df.to_sql(table_name, con=connection, if_exists='replace', index=False)
    elif load_method == "overwrite":
        df.to_sql(table_name, con=connection, if_exists='replace', index=False)
    else:
        raise ValueError("Invalid load method. Choose from 'insert', 'upsert', or 'overwrite'.")

    connection.close()
