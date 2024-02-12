import logging
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from fastapi import HTTPException, status
from ydata_profiling import ProfileReport

logger = logging.getLogger(__name__)


def generate_report_filename(data_source_table_id, table_name):
    try:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        random_string = str(uuid.uuid4().hex)[:8]
        project_name = table_name.split("_")[-1]
        filename = (
            f"{project_name}_{data_source_table_id}_{timestamp}_{random_string}_report.html"
        )
        return filename
    except Exception as e:
        logger.error(f"Error generating report filename: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Bad Request",
                "message": f"Error generating report filename: {e}",
            },
        )


def generate_ydata_profiling_report(df, data_source_table_id, table_name, report_folder):
    try:
        profile = ProfileReport(
            df,
            title=f"Data Profiling Report for {data_source_table_id}.{table_name}",
            explorative=True,
        )
        report_filename = generate_report_filename(data_source_table_id, table_name)
        report_html_path = os.path.join(report_folder, report_filename)
        profile.to_file(report_html_path)
        return report_html_path
    except Exception as e:
        logger.error(f"Error generating ydata_profiling report: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Bad Request",
                "message": f"Error generating ydata_profiling report: {e}",
            },
        )


def get_existing_report(data_source_table_id, table_name, report_folder):
    try:
        current_time = datetime.now()
        time_24_hours_ago = current_time - timedelta(hours=24)

        project_name = table_name.split("_")[-1]

        matching_reports = [
            filename
            for filename in os.listdir(report_folder)
            if filename.endswith(".html")
            and filename.startswith(f"{project_name}_{data_source_table_id}_")
        ]

        for report in matching_reports:
            parts = report.split("_")
            if len(parts) == 5 and len(parts[2]) == 14 and parts[2].isdigit():
                file_timestamp = parts[2]
                file_timestamp_dt = datetime.strptime(file_timestamp, "%Y%m%d%H%M%S")

                if (
                    file_timestamp_dt >= time_24_hours_ago
                    and file_timestamp_dt <= current_time
                ):
                    return os.path.join(report_folder, report)

        return None
    except Exception as e:
        logger.error(f"Error getting existing report: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Bad Request",
                "message": f"Error getting existing report: {e}",
            },
        )


def generate_report_async(df, data_source_table_id, table_name, report_folder):
    try:
        existing_report = get_existing_report(data_source_table_id, table_name, report_folder)
        if existing_report:
            return existing_report

        report_path = generate_ydata_profiling_report(
            df, data_source_table_id, table_name, report_folder
        )
        return report_path
    except Exception as e:
        logger.error(f"Error generating report asynchronously: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Bad Request",
                "message": f"Error generating report asynchronously: {e}",
            },
        )


def generate_report(df, data_source_table_id, table_name, report_folder):
    try:
        with ThreadPoolExecutor() as executor:
            future = executor.submit(
                generate_report_async, df, data_source_table_id, table_name, report_folder
            )
            return future.result(timeout=60)
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad Request", "message": f"Error generating report: {e}"},
        )
