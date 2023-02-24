from typing import Any, Optional, Dict, List


def replace_undefined_value(item, value):
    return item if item is not None else value


def create_list(class_type: Any, obj: Optional[Dict[str, Any]], *args) -> List[Any]:
    if obj is None:
        return []
    else:
        new_list = [class_type.from_dict(y, *args) for y in obj]
        new_list = [item for item in new_list if item is not None]
        return new_list
