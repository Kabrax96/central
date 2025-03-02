import pytest
import pandas as pd
from datetime import datetime
from etl_project.assets.presupuesto_etl import transform_financial_data


@pytest.fixture
def setup_input_df_financial_data():
    return pd.DataFrame(
        [
            {
                "Date": "2024/01/01",
                "Revenue": "$130.27",
                "Expenses": "81.47 MXN",
                "Tax Income": "56.11 pesos",
                "Debt": "619.40 MEX$",
                "GDP Contribution": "3.76%",
                "Currency": "mxn",
            },
            {
                "Date": "2024/01/02",
                "Revenue": "$150.50",
                "Expenses": "100.00 MXN",
                "Tax Income": "70.20 pesos",
                "Debt": "500.50 MEX$",
                "GDP Contribution": "4.50%",
                "Currency": "Pesos",
            },
        ]
    )


@pytest.fixture
def setup_transformed_df():
    return pd.DataFrame(
        [
            {
                "DATE": pd.to_datetime("2024-01-01"),
                "REVENUE": 130.27,
                "EXPENSES": 81.47,
                "TAX_INCOME": 56.11,
                "DEBT": 619.40,
                "GDP_CONTRIBUTION_PERCENTAGE": 3.76,
                "CURRENCY": "MXN",
                "QUARTER": "2024Q1",
            },
            {
                "DATE": pd.to_datetime("2024-01-02"),
                "REVENUE": 150.50,
                "EXPENSES": 100.00,
                "TAX_INCOME": 70.20,
                "DEBT": 500.50,
                "GDP_CONTRIBUTION_PERCENTAGE": 4.50,
                "CURRENCY": "MXN",
                "QUARTER": "2024Q1",
            },
        ]
    )


def test_transform_financial_data(setup_input_df_financial_data, setup_transformed_df):
    # Expected
    df_input = setup_input_df_financial_data
    expected_df = setup_transformed_df

    # Actual
    df_transformed = transform_financial_data(df_input)

    # Compare
    pd.testing.assert_frame_equal(df_transformed, expected_df, check_exact=True)
