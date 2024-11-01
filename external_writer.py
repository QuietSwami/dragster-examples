from dagster import ConfigurableResource, asset, job, ResourceParam, InitResourceContext, Definitions
# If you need to integrate an existing class or a third-party object as a resource,
# you can override the create_resource method in your ConfigurableResource subclass to return the desired object. 
# This approach is beneficial when you want to separate configuration management from the runtime logic.


class ExternalWriter:
    def __init__(self, prefix: str):
        self.prefix = prefix

    def output(self, text: str) -> None:
        print(f"{self.prefix}{text}")

class WriterResource(ConfigurableResource):
    prefix: str

    def create_resource(self, context: InitResourceContext) -> ExternalWriter:
        return ExternalWriter(self.prefix)

@asset
def use_external_writer_as_resource(writer: ResourceParam[ExternalWriter]):
    writer.output("Hello, World!")

@job
def asset_job():
    use_external_writer_as_resource()

defs = Definitions(
    assets=[use_external_writer_as_resource],
    jobs=[asset_job],
    resources={"writer": WriterResource(prefix=">> ")}
)

if __name__ == "__main__":
    # Execute the job in-process - this is a simple way to run a job in a script
    defs.get_job_def("asset_job").execute_in_process()