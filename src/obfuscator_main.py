import json
import logging
from io import BytesIO
from utils.get_file_content import get_file_content
from utils.extract_file_format import extract_file_format
from utils.file_to_df import file_to_df
from utils.redact_pii import redact_pii
from utils.to_byte_stream import to_byte_stream


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

    #loading the string into a dict to be processed by python
    json_payload = json.loads(json_str)


    #checking to see if the string in contains required fields
    if 'file_to_obfuscator' not in json_payload:
        raise ValueError("Missing 'file_to_obfuscator' in input JSON")
    
    if 'pii_fields' not in json_payload:
        raise ValueError("Missing 'pii_fields' in input JSON")
    
    #extracting the file format from the s3 url
    file_format = extract_file_format(json_payload)

    #checking if we got None
    if file_format == None:
        logger.error("Failed to retreive file format")
        return {"status": 400}


    #retreiving file content
    file_content = get_file_content(
        json_payload["file_to_be_obfuscater"]
        )
    

    #checking if we not get None
    if file_content == None:
        logger.error("Failed to retreive file content")
        return {"status": 400}
    

    #turning the file content into df
    df = file_to_df(file_content)


    #checking if we not get None
    if df == None:
        logger.error("Failed to convert file content to df")
        return {"status": 400}
    
    df_redacted = redact_pii(df,json_payload["pii_fields"])

    final_output = to_byte_stream(df_redacted, file_format)

    if final_output == None:
        logger.error("Failed to convert file to byte stream")
        return {"status": 400}
    
    return final_output