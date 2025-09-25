import re
import logging

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)

logger = logging.getLogger(__name__)


def extract_file_format(file_string):
    """
    Searchs for file format in json key 'file_to_obfuscator'

    Args:
        file_string: full s3 url location for the file
        that ends with the format

    Return:
        String representing the file format (e.g. csv, json, parquet)
    """

    # RegEx pattern to find any file format
    pattern = re.compile(r"\.([a-zA-Z0-9]+)$")
    match_ = pattern.search(file_string)

    # ruling out AttributeError if no match found
    if not match_:
        logger.error("no file format was found")
        return None

    # fetching the file format from capture group
    file_format = match_.group(1)

    return file_format
