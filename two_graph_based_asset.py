from dagster import graph_asset, op, define_asset_job, Definitions
import pandas as pd

# Goal: Learn how to define and execute multiple graph-backed assets in a single workflow.
# - Graph-Backed Asset: Defined with @graph_asset, it is a multi-step asset, with each step as an op in a graph,
#   ideal for complex computations.
# Also, learn how to define and execute jobs for each asset in a workflow. Each job can target a specific asset.

# Workflow 1: Customer Data Processing

@op
def fetch_customer_data() -> pd.DataFrame:
    """Fetch customer data."""
    data = {"customer_id": [1, 2, 3], "customer_name": ["Alice", "Bob", "Charlie"]}
    return pd.DataFrame(data)

@op
def process_customer_data(data: pd.DataFrame) -> pd.DataFrame:
    """Process customer data by adding a greeting."""
    data["greeting"] = "Hello, " + data["customer_name"]
    return data

@graph_asset
def customer_data_asset() -> pd.DataFrame:
    """Graph-backed asset for processed customer data."""
    data = fetch_customer_data()
    processed_data = process_customer_data(data)
    return processed_data

# Workflow 2: Sales Data Processing

@op
def fetch_sales_data() -> pd.DataFrame:
    """Fetch sales data."""
    data = {"sale_id": [101, 102, 103], "amount": [250, 300, 400]}
    return pd.DataFrame(data)

@op
def process_sales_data(data: pd.DataFrame) -> pd.DataFrame:
    """Process sales data by adding a discount column."""
    data["discounted_amount"] = data["amount"] * 0.9
    return data

@graph_asset
def sales_data_asset() -> pd.DataFrame:
    """Graph-backed asset for processed sales data."""
    data = fetch_sales_data()
    processed_data = process_sales_data(data)
    return processed_data

# Define jobs for each asset

different_data_job = define_asset_job(name="different_data_job", selection="customer_data_asset")  # Job for customer data asset - We'll target the customer_data_asset
another_data_job = define_asset_job(name="another_data_job", selection="sales_data_asset")  # Job for sales data asset

# Register assets and jobs in Definitions

defs = Definitions(
    assets=[customer_data_asset, sales_data_asset],
    jobs=[different_data_job, another_data_job]
)

# Execute jobs separately

if __name__ == "__main__":
    # Execute the customer data processing job
    result_customer = defs.get_job_def("customer_data_job").execute_in_process()
    assert result_customer.success

    # Execute the sales data processing job
    result_sales = defs.get_job_def("sales_data_job").execute_in_process()
    assert result_sales.success
