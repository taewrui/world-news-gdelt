import json

def parse_field_value(value: str):
    if not value: return None
    if '.' in value and value.replace('.','').isdigit():
        return float(value)
    if value.isdigit():
        return int(value)
    return value

def parse_field_values(field: str, delimiter: str):
    if not field: return []
    return [parse_field_value(x) for x in field.split(delimiter)]

def create_dict_from_fields(fields: list, keys: list):
    return {k: v for k, v in zip(keys, fields) if v is not None}

def convert_datetime_format(datetime_str):
    """yyyymmddhhmmss 형식을 yyyy-mm-dd hh:mm:ss 형식으로 변환"""
    if not datetime_str or len(str(datetime_str)) != 14:
        return datetime_str
    
    
    year = datetime_str[:4]
    month = datetime_str[4:6]
    day = datetime_str[6:8]
    hour = datetime_str[8:10]
    minute = datetime_str[10:12]
    second = datetime_str[12:14]
    
    return f"{year}-{month}-{day} {hour}:{minute}:{second}"

def convert_to_json_array(field: str, delimiter: str = ';'):
    if not field: return None
    values = field.split(delimiter)
    if not values[-1]: values.pop()
    return json.dumps(values, ensure_ascii=False)

def convert_to_json_dict_array(field: str, keys: list, d1: str = ';', d2: str = '#'):
    if not field: return None
    values_dict = []
    fields = field.split(d1)
    
    for f in fields:
        values = parse_field_values(f, d2)
        if len(values) <= 1: continue
        d = create_dict_from_fields(values, keys)
        if d: values_dict.append(d)
    
    return json.dumps(values_dict, ensure_ascii=False)

def edit_v2_counts(field: str):
    keys = [
        "count_type", "count_value", "object_type", "location_type",
        "location_fullname", "location_countrycode", "location_adm1code",
        "location_latitude", "location_longitude", "location_featureid", "offset"
    ]
    return convert_to_json_dict_array(field, keys)

def edit_v2_locations(field: str):
    keys = [
        "location_type", "location_fullname", "location_countrycode",
        "location_adm1code", "location_latitude", "location_longitude",
        "location_featureid", "offset"
    ]
    return convert_to_json_dict_array(field, keys)

def edit_v1_tone(field: str):
    if not field: return [None] * 7
    values = parse_field_values(field, ',')
    if len(values) < 7: return [None] * 7
    
    # JSON 대신 개별 값들을 리스트로 반환
    return values[:7]

def edit_v2_enhanceddates(field: str):
    keys = ["date_resolution", "month", "day", "year", "offset"]
    return convert_to_json_dict_array(field, keys)

def edit_v2_quotations(field: str):
    if not field: return None
    values_dict = []
    fields = field.split('#')
    for f in fields:
        values = f.split('|')
        if len(values) < 4: continue
        d = {
            "offset": values[0],
            "length": values[1],
            "verb": values[2],
            "quote": values[3],
        }
        values_dict.append(d)
    return json.dumps(values_dict, ensure_ascii=False)

def edit_v2_amounts(field: str):
    keys = ["amount", "object", "offset"]
    return convert_to_json_dict_array(field, keys, ';', ',')

def edit_to_dict(field: str, d1: str, d2: str):
    if not field: return None
    values_dict = {}
    values = field.split(d1)
    
    for v in values:
        if not v: continue
        try:
            key, value = v.split(d2, 1)
            values_dict[key.strip()] = parse_field_value(value.strip())
        except ValueError:
            continue
            
    return values_dict


def edit_actor_column(row, index):
    type = row[index]
    if type == 0:
        d = dict()
        del row[index+1:index+7]
        row[index+1] = json.dumps(d, ensure_ascii=False)
    elif type == 1:
        offset = 0
        if len(row[index + 2]) == 2:
            fullname = row[index + 1]
        else:
            fullname = [row[index + 1], row[index + 2]]
            offset = 1
        d = {
            "fullname": fullname,
            "country_code": row[index + 2 + offset],
            "adm1_code": row[index + 3 + offset],
            "latitude": float(row[index + 5 + offset]),
            "longitude": float(row[index + 6 + offset]),
            "feature_id": row[index + 7 + offset],
        }
        del row[index+1:index+7+offset]
        row[index+1] = json.dumps(d, ensure_ascii=False)
    elif type == 2 :
        d = {
            "fullname": [row[index + 1], row[index + 2]],
            "country_code": row[index + 3],
            "adm1_code": row[index + 4],
            "latitude": float(row[index + 6]),
            "longitude": float(row[index + 7]),
            "feature_id": row[index + 8],
        }
        del row[index+1:index+8]
        row[index+1] = json.dumps(d, ensure_ascii=False)
    elif type == 3:
        d = {
            "fullname": [row[index + 1], row[index + 2], row[index + 3]],
            "country_code": row[index + 4],
            "adm1_code": row[index + 5],
            "adm2_code": row[index + 6],
            "latitude": float(row[index + 7]),
            "longitude": float(row[index + 8]),
            "feature_id": row[index + 9],
        }
        del row[index+1:index+9]
        row[index+1] = json.dumps(d, ensure_ascii=False)
    elif type == 4:
        offset = 0
        if len(row[index + 4]) == 2:
            fullname = [row[index + i] for i in range(1, 4)]
        else:
            fullname = [row[index + i] for i in range(1, 5)]
            offset = 1
        d = {
            "fullname": fullname,
            "country_code": row[index + 4 + offset],
            "adm1_code": row[index + 5 + offset],
            "adm2_code": row[index + 6 + offset],
            "latitude": float(row[index + 7 + offset]),
            "longitude": float(row[index + 8 + offset]),
            "feature_id": row[index + 9 + offset],
        }
        del row[index+1:index+9+offset]
        row[index+1] = json.dumps(d, ensure_ascii=False)
    return row 