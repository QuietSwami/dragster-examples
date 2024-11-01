from dagster import ConfigurableResource, asset, job, Definitions

class WriterResource(ConfigurableResource):
    prefix: str

    def output(self, text: str) -> None:
        print(f"{self.prefix}{text}")

@asset
def asset_that_uses_writer(writer: WriterResource):
    """An asset that uses a configurable resource."""
    writer.output("Hello, World!")

@job
def asset_job():
    """A job that uses an asset that uses a configurable resource."""
    asset_that_uses_writer()
    
# Another alternative to defining the job is to use the define_asset_job helper function

# asset_job = define_asset_job(name="asset_job")

defs = Definitions(
    assets=[asset_that_uses_writer],
    jobs=[asset_job],
    resources={"writer": WriterResource(prefix=">> ")}
)

if __name__ == "__main__":
    defs.get_job_def("asset_job").execute_in_process()

