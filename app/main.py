import os
import logging

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from app.data_processing import data_profiling
from app.database import retrieve_data_from_mongodb
from app.report_generation import generate_report
from app.utils import validate_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REPORT_FOLDER = "reports"
app_config = {"REPORT_FOLDER": REPORT_FOLDER}


@app.on_event("startup")
async def startup_event():
    try:
        os.makedirs(REPORT_FOLDER, exist_ok=True)
        logger.info(f"Directory '{REPORT_FOLDER}' created successfully.")
    except Exception as e:
        logger.error(f"Error creating directory '{REPORT_FOLDER}': {e}")


@app.post("/data-profile")
def data_profile(data: dict):
    data_source_table_id, table_name, error = validate_data(data)
    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "status_code": status.HTTP_400_BAD_REQUEST,
                "data": None,
                "message": error,
            },
        )

    df, error = retrieve_data_from_mongodb(data_source_table_id, table_name)
    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "status_code": status.HTTP_400_BAD_REQUEST,
                "data": None,
                "message": error,
            },
        )

    profile_result = data_profiling(df)
    return {
        "status_code": status.HTTP_200_OK,
        "data": profile_result,
        "message": "Data profiled successfully",
    }


@app.post("/generate-report")
def generate_report_endpoint(data: dict):
    data_source_table_id, table_name, error = validate_data(data)
    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "status_code": status.HTTP_400_BAD_REQUEST,
                "data": None,
                "message": error,
            },
        )

    df, error = retrieve_data_from_mongodb(data_source_table_id, table_name)
    if error:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "status_code": status.HTTP_400_BAD_REQUEST,
                "data": None,
                "message": error,
            },
        )

    report_path = generate_report(
        df, data_source_table_id, table_name, app_config["REPORT_FOLDER"]
    )
    if not report_path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "status_code": status.HTTP_400_BAD_REQUEST,
                "data": None,
                "message": "Error generating the report",
            },
        )

    return (
        {
            "status_code": status.HTTP_200_OK,
            "data": {"profiling_report_path": report_path},
            "message": "Report generated successfully",
        },
    )


@app.get("/download-report/{report_path:path}")
def download_report(report_path: str):
    try:
        return FileResponse(report_path, filename=os.path.basename(report_path))
    except Exception as e:
        logger.error(f"Error in /download-report endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "status_code": status.HTTP_404_NOT_FOUND,
                "data": None,
                "message": "File not found",
            },
        )
