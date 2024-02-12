import logging
from datetime import time

import pandas as pd
from bson import ObjectId
from fastapi import HTTPException, status

logger = logging.getLogger(__name__)


def convert_to_json_serializable(value):
    try:
        if value is None or pd.isna(value):
            return "null"
        elif isinstance(value, pd.Timestamp):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(value, time):
            return value.strftime("%H:%M:%S")
        elif isinstance(value, ObjectId):
            return str(value)
        return value
    except Exception as e:
        logger.error(f"Error converting value to JSON serializable: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Bad Request",
                "message": f"Error converting value to JSON serializable: {e}",
            },
        )


def data_profiling(data):
    try:
        if isinstance(data, str):
            if data.endswith(".csv"):
                df = pd.read_csv(data)
            elif data.endswith((".xlsx", ".XLSX")):
                df = pd.read_excel(data)
            else:
                raise ValueError("Unsupported file format")
        else:
            df = data

        df = df.map(convert_to_json_serializable)

        profile = {
            "Number of Rows": len(df),
            "Number of Columns": len(df.columns),
            "Columns": df.columns.tolist(),
        }

        data_types = df.dtypes.value_counts().to_dict()
        data_type_info = {}
        for dtype, count in data_types.items():
            data_type_info[str(dtype)] = count
        profile["Data Types"] = data_type_info

        missing_values = df.isnull().sum().to_dict()
        missing_values = {
            col: ("null" if count == 0 else count)
            for col, count in missing_values.items()
        }
        profile["Missing Values"] = missing_values

        statistics = df.describe().to_dict()
        stats_info = {}
        for col, values in statistics.items():
            stats_info[col] = {}
            for key, value in values.items():
                stats_info[col][key] = value
        profile["Basic Statistics"] = stats_info

        return profile
    except Exception as e:
        logger.error(f"Error in data profiling: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad Request", "message": f"Error in data profiling: {e}"},
        )
