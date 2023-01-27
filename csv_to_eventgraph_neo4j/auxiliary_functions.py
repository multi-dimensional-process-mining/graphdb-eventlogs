from re import sub


def camel_case(s):
    # remove all _ or - and replace by a space
    s = sub(r"(_|-)+", " ", s)
    # remove all (, ), :, . with empty space
    s = sub(r"(\(|\)|:|\.)+", " ", s)
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
