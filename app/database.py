import logging
import os
from urllib.parse import urlparse
from bson import ObjectId

import pandas as pd
from dotenv import load_dotenv
from fastapi import HTTPException, status
from pymongo import MongoClient

load_dotenv()

logger = logging.getLogger(__name__)


def get_mongo_connection_details():
    mongo_url = os.getenv("MONGO_URL")
    if not mongo_url:
        logger.error("Missing MONGO_URL environment variable")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal Server Error",
                "message": "Missing MONGO_URL environment variable",
            },
        )

    return mongo_url


def extract_db_name_and_collection(table_name, data_source_table_id):
    if table_name:
        return table_name
    elif data_source_table_id:
        # Your logic to determine collection_name from data_source_table_id
        # Replace the following line with your specific logic
        return f"default_collection_for_{data_source_table_id}"
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Bad Request",
                "message": "Either table_name or data_source_table_id must be provided",
            },
        )


def connect_to_mongodb(collection_name):
    try:
        mongo_url = get_mongo_connection_details()
        parsed_url = urlparse(mongo_url)
        db_name = parsed_url.path[1:]
        client = MongoClient(mongo_url)
        db = client[db_name]
        return db[collection_name]
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal Server Error",
                "message": f"Error connecting to MongoDB: {e}",
            },
        )


def stream_data_from_mongodb(collection, data_source_table_id, batch_size=None):
    try:
        query = {"dsTableId": ObjectId(data_source_table_id)} 
        cursor = collection.find(query)
        if batch_size:
            cursor.batch_size(batch_size)
        for document in cursor:
            yield document
    except Exception as e:
        logger.error(f"Error streaming data from MongoDB: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Bad Request",
                "message": f"Error streaming data from MongoDB: {e}",
            },
        )


def retrieve_data_from_mongodb(data_source_table_id, table_name=None):
    try:
        collection_name = extract_db_name_and_collection(
            table_name, data_source_table_id
        )
        collection = connect_to_mongodb(collection_name)
        df_generator = stream_data_from_mongodb(collection, data_source_table_id, batch_size=1000)
        df = pd.DataFrame(df_generator)

        return df, None

    except Exception as e:
        logger.error(f"Error retrieving data from MongoDB: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Bad Request",
                "message": f"Error retrieving data from MongoDB: {e}",
            },
        )
