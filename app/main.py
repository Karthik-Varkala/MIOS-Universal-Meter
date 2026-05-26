from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .api.routes import cdf_router, es_upload_router, sql_upload_router
from .logging_utils import configure_logging, get_logger, log_exception, log_with_context
from .validation import validate_runtime_configuration


configure_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    validate_runtime_configuration()
    yield


app = FastAPI(
    title="CDF Data API",
    description="Extracts meter data, processes single files, handles S3 downloads, and publishes to Elasticsearch.",
    lifespan=lifespan,
)

app.include_router(cdf_router)
app.include_router(es_upload_router)
app.include_router(sql_upload_router)


def _build_request_context(request: Request) -> dict:
    return {
        "method": request.method,
        "path": request.url.path,
        "query": str(request.url.query or ""),
        "client": request.client.host if request.client else "",
    }


@app.exception_handler(RequestValidationError)
async def request_validation_exception_handler(request: Request, exc: RequestValidationError):
    context = _build_request_context(request)
    context["errors"] = exc.errors()
    log_with_context(logger, 40, "Request validation failed", **context)
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors(), "message": "Request validation failed."},
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    context = _build_request_context(request)
    context["status_code"] = exc.status_code
    context["detail"] = exc.detail
    log_with_context(logger, 40 if exc.status_code >= 400 else 20, "HTTP exception raised", **context)
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log_exception(logger, "Unhandled application exception", exc, **_build_request_context(request))
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error. Check logs/error_log.txt for details."},
    )
