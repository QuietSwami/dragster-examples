from dagster import AssetOut, graph_multi_asset, op, Definitions, define_asset_job
import random

@op
def generate_data() -> int:
    """Generate some dummy data. Replace with actual data fetching logic."""
    return random.randint(1, 100)

@op
def compute_square(value: int) -> int:
    """Return the square of the input value."""
    return value * value

@op
def compute_cube(value: int) -> int:
    """Return the cube of the input value."""
    return value * value * value

# Define a graph asset that represents the processed data
# In this case, the asset has two outputs: square_asset and cube_asset
@graph_multi_asset(
    outs={
        "square_asset": AssetOut(),
        "cube_asset": AssetOut()
    }
)

def compute_assets():
    """Asset job that computes the square and cube of a random number."""
    value = generate_data()
    square = compute_square(value)
    cube = compute_cube(value)
    return {"square_asset": square, "cube_asset": cube}

asset_job = define_asset_job(name="compute_assets_job")

# Create Definitions with the assets and job
defs = Definitions(
    assets=[compute_assets],
    jobs=[asset_job]
)

# Execute the job
if __name__ == "__main__":
    result = defs.get_job_def("compute_assets_job").execute_in_process()
    assert result.success