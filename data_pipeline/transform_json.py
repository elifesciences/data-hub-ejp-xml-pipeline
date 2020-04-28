def remove_key_with_null_value(record):
    if isinstance(record, dict):
        for key in list(record):
            val = record.get(key)
            if not val:
                record.pop(key, None)
            elif isinstance(val, (dict, list)):
                remove_key_with_null_value(val)

    elif isinstance(record, list):
        for index, val in enumerate(record):
            if isinstance(val, (dict, list)):
                remove_key_with_null_value(val)
            else:
                if val:
                    record[index] = val
    return record
