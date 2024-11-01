import pandas as pd
from dagster import graph_asset, op, Definitions, define_asset_job

# - Basic Asset: Defined with @asset, it represents a single function without multiple steps.
# - Graph-Backed Asset: Defined with @graph_asset, it is a multi-step asset, with each step as an op in a graph, 
#   ideal for complex computations.


@op
def fetch_data() -> pd.DataFrame:
    """Generate some dummy data. Replace with actual data fetching logic."""
    data = {"column1": [1, 2, 3], "column2": [4, 5, 6]}
    return pd.DataFrame(data)

@op
def process_data(data: pd.DataFrame) -> pd.DataFrame:
    """Process the data. Replace with actual data processing logic."""
    data["column3"] = data["column1"] + data["column2"]
    return data

@graph_asset
def processed_data_asset() -> pd.DataFrame:
    """Create a graph asset that represents the processed data."""
    return process_data(fetch_data())

# Define a job that materializes the asset
asset_job = define_asset_job(name="process_data_job")

# Create Definitions with the asset and job
defs = Definitions(
    assets=[processed_data_asset],
    jobs=[asset_job]
)

# Execute the job
if __name__ == "__main__":
    result = defs.get_job_def("process_data_job").execute_in_process()
    assert result.success