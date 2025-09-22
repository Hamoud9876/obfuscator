import json
import re
import logging
from io import BytesIO
from utils.obfuscator_json import obfuscator_json
from utils.obfuscator_csv import obfuscator_csv
from utils.obfuscator_parquet import obfuscator_parquet 


logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
            "file_to_be_obfuscater": "s3://file/key/in/the/bucket.(fileformat)",
            "pii_fields: ["list","of", "fields","to","obfuscator]
        }

    Return
        Byte stream contaning the new obfuscatored file content
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
    
    
    #RegEx pattern to find any file format
    pattern = re.compile(r"\.([a-zA-Z0-9]+)$")
    match = pattern.search(json_payload['file_to_obfuscator'])
    file_format = match.group(1)


    #dynamically invoke a function from router dict
    if file_format in router:
        router[file_format]()

    




    pass