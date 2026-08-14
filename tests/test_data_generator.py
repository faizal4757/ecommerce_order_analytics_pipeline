import pandas as pd
from src.data_generator import generate_products, generate_customers

def test_generate_products():
    # Test that products generation returns a dataframe with expected columns and count
    df = generate_products(n_products=10)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 10
    assert "product_id" in df.columns
    assert "list_price" in df.columns

def test_generate_customers():
    # Test that customer generation returns a dataframe with expected structure
    df = generate_customers(n_customers=5)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
    assert "customer_id" in df.columns
    assert "tier" in df.columns