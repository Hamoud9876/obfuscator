import json
import re
import logging
from io import BytesIO
from src.obfuscator_json import obfuscator_json
from src.obfuscator_csv import obfuscator_csv
from src.obfuscator_parquet import obfuscator_parquet
from utils.get_file_content import get_file_content
from utils.extract_file_format import extract_file_format


logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True
)

logger = logging.getLogger(__name__)


def obfuscator_main(json_str: str) -> BytesIO:
    """
    redact Personaly Identifying Inforamtion (pii)
    from the provided file and fields in the 
    inputed json string and return a Bytes stream.

    Args
        json_str: a string in the structure of a json
        that contains the distination and fields to be
        redactid 

        expected structure:
        {
            "file_to_be_obfuscater": "s3://key/in/bucket.(fileformat)",
            "pii_fields: ["list","of", "fields","to","obfuscator]
        }

    Return
        ByteStream contaning the new obfuscatored file content
        or {"status": 400} if it fails
    """

    #dictionary of functions, if updated in the future
    #make sure the key match the format
    router = {
        "csv": obfuscator_csv,
        "json": obfuscator_json,
        "parquet": obfuscator_parquet
    }


    #loading the string into a dict to be processed by python
    json_payload = json.loads(json_str)


    #checking to see if the string in contains required fields
    if 'file_to_obfuscator' not in json_payload:
        raise ValueError("Missing 'file_to_obfuscator' in input JSON")
    
    if 'pii_fields' not in json_payload:
        raise ValueError("Missing 'pii_fields' in input JSON")
    
    file_format = extract_file_format(json_payload)

    if file_format == None:
        logger.error("Failed to retreive file format")
        return {"status": 400}


    #retreiving file content
    file_content = get_file_content(
        json_payload["file_to_be_obfuscater"]
        )
    
    if file_content == None:
        logger.error("Failed to retreive file content")
        return {"status": 400}

    #dynamically invoke a function from router dict
    if file_format in router:
        router[file_format]()

    




    pass