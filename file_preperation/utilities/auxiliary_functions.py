from re import sub
from typing import Any, Optional, Dict, List


def camel_case(s):
    # remove all _, -, (, ), :, . and replace by a space
    s = sub(r"[_\-():.]+", " ", s)
    # Add Space Before Capital Letter If and Only If Previous Letter is Not Also Capital
    # So 'HelloCHARLIE this isBob.' should become 'Hello CHARLIE this is Bob.'
    s = sub(r"(?<![A-Z])(?<!^)([A-Z])", r" \1", s)
    # replace # with num, capitalize each word and remove all spaces
    s = s.replace("#", "num")
    s = s.title().replace(" ", "")
    # make first word start with lower case
    return ''.join([s[0].lower(), s[1:]])


def convert_columns_into_camel_case(columns):
    return [camel_case(column) for column in columns]


def replace_undefined_value(item, value):
    return item if item is not None else value


def create_list(class_type: Any, obj: Optional[Dict[str, Any]], *args) -> List[Any]:
    if obj is None:
        return []
    else:
        new_list = [class_type.from_dict(y, *args) for y in obj]
        new_list = [item for item in new_list if item is not None]
        return new_list


