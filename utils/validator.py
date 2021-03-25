import re


def validate_regex(content: str, regexp: str):
    if regexp is None:
        return None
    return bool(re.search(regexp, content))
