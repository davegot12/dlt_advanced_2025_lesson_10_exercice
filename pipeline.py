import dlt
import time
import os

from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator


os.environ["EXTRACT__WORKERS"] = "3" # Adjust number of workers for extraction
os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = "10000" # About 60K orders total, trying to break normalization step into 6 workers.
os.environ['NORMALIZE__WORKERS'] = "6" # Take advantage of the 60K orders divided into 10K intermediary files at the extract stage.
os.environ['NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS'] = "10000" # Keeping extract to normalize intermediary file ratio 1:1.
os.environ["LOAD__WORKERS"] = "6"


client = RESTClient(
    base_url="https://jaffle-shop.scalevector.ai/api/v1",
    paginator=HeaderLinkPaginator(),
)

@dlt.resource(
    write_disposition="replace",
    name="customers",
    parallelized=True, # Activate parralel processing
)
def get_customers():
    paginate = client.paginate("customers")
    for page in paginate:
        yield page

@dlt.resource(
    write_disposition="replace",
    name="orders",
    parallelized=True, # Activate parralel processing
)
def get_orders():
    paginate = client.paginate(
        "orders",
        params={"page_size": 500}, # Will yield pages with more records, bigger chunks.
    )
    for page in paginate:
        yield page

@dlt.resource(
    write_disposition="replace",
    name="products",
    parallelized=True, # Activate parralel processing
)
def get_products():
    paginate = client.paginate("products")
    for page in paginate:
        yield page

@dlt.source # Group resources into source
def jaffle_source():
    return [get_customers(), get_orders(), get_products()]


pipeline = dlt.pipeline(
    pipeline_name="jaffle_shop_optimized",
    destination="duckdb",
    dataset_name="jaffle_shop_optimized",
    dev_mode=True,
)

start_extract = time.time()
pipeline.extract(jaffle_source())
end_extract = time.time()

start_normalize = time.time()
pipeline.normalize()
end_normalize = time.time()

start_load = time.time()
pipeline.load()
end_load = time.time()

print(pipeline.last_trace)
print(f"Extract time: {end_extract - start_extract}")
print(f"Normalize time: {end_normalize - start_normalize}")
print(f"Load time: {end_load - start_load}")
