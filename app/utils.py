def validate_data(data):
    if not data:
        return None, None, "Missing data in request body"

    data_source_table_id = data.get("dataSourceTableId")
    if not data_source_table_id:
        return None, None, "Missing dataSourceTableId in request body"

    table_name = data.get("tableName")

    return data_source_table_id, table_name, None
