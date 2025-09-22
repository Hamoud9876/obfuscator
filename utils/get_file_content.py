import re
import logging
import boto3

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True
)

logger = logging.getLogger(__name__)

def get_file_content(flocation: str) -> dict:
    """
    retrieve file content from the bucket

    Args:
        flocation: url of the file in s3 bucket

    Return:
        dataframe contaning the file content if file found
        returns -1 if failed to retrieve any content
    """

    #checking if the input is of type string
    if not isinstance(flocation,str):
        logger.info("wrong input type for 'flocation'")
        return -1


    
    #regex pattern, returns bucket name in group 1
    #and key in group 2
    pattern = re.compile(r"^s3://([^/]+)/(.+)$")
    match_ = pattern.match(flocation)

    
    #ruling out AttributeError if no match found
    if not match_:
        logger.error(f"Invalid S3 URL: {flocation}")
        return -1


    #getting bucket name and key from the capture groups
    bucket = match_.group(1)
    key = match_.group(2)


    try:
        #creating boto3 client
        s3_client = boto3.client("s3")


        #retrieving the file content from the bucket
        s3_response = s3_client.get_object(
            Bucket= bucket,
            Key=key
        )


    except Exception as e:
        logger.error(f"Something went wrong,{type(e).__name__}: {e}")
        return -1


    return s3_response