import pytest
import os
import csv
import re

# Define the folder where the generated financial data is stored
DATA_FOLDER = os.path.join(os.path.dirname(__file__), "presupuestos")

EXPECTED_COLUMNS = [
    "Date",
    "Revenue",
    "Expenses",
    "Tax Income",
    "Debt",
    "GDP Contribution",
    "Currency",
]


@pytest.fixture
def get_csv_files():
    """Retrieve all CSV files in the financial_data folder."""
    assert os.path.exists(DATA_FOLDER), "The financial_data folder does not exist!"
    assert os.path.isdir(DATA_FOLDER), "The financial_data path is not a directory!"
    files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]
    assert len(files) > 0, "No CSV files found in the financial_data folder!"
    return files


def test_financial_data_folder_not_empty(get_csv_files):
    """Ensure there is at least one file in the financial_data folder."""
    assert get_csv_files, "No CSV files found!"


def test_financial_data_csv_headers(get_csv_files):
    """Ensure the CSV file has the expected column headers."""
    file_path = os.path.join(DATA_FOLDER, get_csv_files[0])

    with open(file_path, newline="") as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)  # Read the first row
        assert headers == EXPECTED_COLUMNS, f"Unexpected CSV headers: {headers}"


def test_financial_data_csv_not_empty(get_csv_files):
    """Ensure the CSV file contains data beyond the headers."""
    file_path = os.path.join(DATA_FOLDER, get_csv_files[0])

    with open(file_path, newline="") as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)
        assert len(rows) > 1, "CSV file is empty (no data rows found)!"


def test_financial_data_csv_valid_rows(get_csv_files):
    """Ensure all rows in the CSV file contain valid data."""
    file_path = os.path.join(DATA_FOLDER, get_csv_files[0])

    with open(file_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            assert row["Date"], f"Missing Date in row: {row}"
            assert row["Revenue"], f"Missing Revenue in row: {row}"
            assert row["Expenses"], f"Missing Expenses in row: {row}"
            assert row["Tax Income"], f"Missing Tax Income in row: {row}"
            assert row["Debt"], f"Missing Debt in row: {row}"
            assert row["GDP Contribution"], f"Missing GDP Contribution in row: {row}"
            assert row["Currency"], f"Missing Currency in row: {row}"


def test_financial_data_numeric_values(get_csv_files):
    """Ensure that financial fields contain valid numeric values."""
    file_path = os.path.join(DATA_FOLDER, get_csv_files[0])

    def clean_numeric(value):
        """Helper function to clean currency symbols, non-numeric characters, and convert to float."""
        cleaned_value = re.sub(
            r"[^\d.]", "", value
        )  # Remove all non-numeric & non-period characters

        try:
            return float(cleaned_value)
        except ValueError:
            raise ValueError(
                f"Could not convert '{value}' to float (cleaned: '{cleaned_value}')"
            )

    with open(file_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                revenue = clean_numeric(row["Revenue"])
                expenses = clean_numeric(row["Expenses"])
                tax_income = clean_numeric(row["Tax Income"])
                debt = clean_numeric(row["Debt"])
                gdp_contribution = float(
                    row["GDP Contribution"].replace("%", "").strip()
                )

                assert revenue >= 0, f"Negative Revenue found: {row['Revenue']}"
                assert expenses >= 0, f"Negative Expenses found: {row['Expenses']}"
                assert (
                    tax_income >= 0
                ), f"Negative Tax Income found: {row['Tax Income']}"
                assert debt >= 0, f"Negative Debt found: {row['Debt']}"
                assert (
                    0 <= gdp_contribution <= 100
                ), f"Invalid GDP Contribution: {row['GDP Contribution']}"
            except ValueError as e:
                pytest.fail(f"Invalid numeric value found in row: {row} | Error: {e}")
