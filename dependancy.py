from dagster import AssetOut, AssetIn, graph_asset, graph_multi_asset, op, Definitions, define_asset_job
import pandas as pd 

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

# Define an op to combine customer and sales data

@op
def combine_data(customer_data: pd.DataFrame, sales_data: pd.DataFrame) -> pd.DataFrame:
    """Combine customer and sales data into a single DataFrame."""
    combined_data = pd.concat([customer_data, sales_data], axis=1)
    return combined_data

# Multi-Asset Graph: Combines customer and sales data

@graph_multi_asset(
    ins={
        "customer_data": AssetIn("customer_data_asset"),
        "sales_data": AssetIn("sales_data_asset")
    },
    outs={
        "combined_customer_sales_data": AssetOut()
    }
)
def data_processing_graph(customer_data: pd.DataFrame, sales_data: pd.DataFrame) -> pd.DataFrame:
    """Graph-backed multi-asset that processes customer and sales data together."""
    return combine_data(customer_data, sales_data)

# Define a job to materialize all assets in the graph

asset_job = define_asset_job(name="data_processing_job", selection=["data_processing_graph"])

# Register assets and jobs in Definitions

defs = Definitions(
    assets=[data_processing_graph, sales_data_asset, customer_data_asset],
    jobs=[asset_job]
)

# Run the job

if __name__ == "__main__":
    result = defs.get_job_def("data_processing_job").execute_in_process()
    assert result.success
