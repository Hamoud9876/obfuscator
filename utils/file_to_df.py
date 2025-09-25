import pandas as pd
import logging
import io


logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)

logger = logging.getLogger(__name__)


def file_to_df(file_body, format: str) -> pd.DataFrame:
    """
    Reads a boto3 Streaming Body and returns
    dataframe of  content

    Args:
        file_body (StreamingBody): A boto3 StreamingBody object
        format (str): The format of the file
                ('csv', 'json', or 'parquet')

    Return:
        pd.DataFrame or None: The contents of the
        file as a DataFrame if successful;
        None if the file cannot be read.
    """

    try:
        match format:
            case "csv":
                df = pd.read_csv(file_body)
            case "json":
                df = pd.read_json(file_body, orient="records")
            case "parquet":
                buffer = io.BytesIO(file_body.read())
                df = pd.read_parquet(buffer, engine="pyarrow")
            case _:
                logger.warning("The provided format doesn't match")
                return None
    except Exception:
        logger.error("Something went wrong ", exc_info=True)
        return None

    return df
