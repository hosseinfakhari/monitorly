import re


def validate_regex(content: str, regexp: str):
    """
    Validating text with regexp
    """
    if regexp is None:
        return None
    return bool(re.search(regexp, content))
