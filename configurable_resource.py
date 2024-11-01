from dagster import ConfigurableResource, asset, job, Definitions

class WriterResource(ConfigurableResource):
    prefix: str

    def output(self, text: str) -> None:
        print(f"{self.prefix}{text}")

@asset
def asset_that_uses_writer(writer: WriterResource):
    writer.output("Hello, World!")

@job
def asset_job():
    asset_that_uses_writer()

defs = Definitions(
    assets=[asset_that_uses_writer],
    jobs=[asset_job],
    resources={"writer": WriterResource(prefix=">> ")}
)

if __name__ == "__main__":
    defs.get_job_def("asset_job").execute_in_process()

