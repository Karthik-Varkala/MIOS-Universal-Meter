from fastapi import FastAPI

from .api.routes import cdf_router, es_upload_router, sql_upload_router


app = FastAPI(
    title="CDF Data API",
    description="Extracts meter data, processes single files, handles S3 downloads, and publishes to Elasticsearch.",
)

app.include_router(cdf_router)
app.include_router(es_upload_router)
app.include_router(sql_upload_router)
