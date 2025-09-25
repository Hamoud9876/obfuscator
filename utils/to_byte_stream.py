import io
import pandas as pd
import logging

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)

logger = logging.getLogger(__name__)


def to_byte_stream(df: pd.DataFrame, format: str) -> io.BytesIO:
    """
    convert df into byte stream of the provided format

    Args:
        df: dataframe contaning file content
        format: the format the byte esteam
        should be in

    Return:
        Ready to read byte stream of the same
        content as the df
    """
    # taking a deep copy not not mutate the original
    df_copy = df.copy(deep=True)

    # creating a buffer to store df content on
    buffer = io.BytesIO()
    try:
        match format:
            case "csv":
                df_copy.to_csv(buffer, index=False)
            case "json":
                df_copy.to_json(buffer, orient="records")
            case "parquet":
                df.to_parquet(buffer, index=False, engine="pyarrow")
            case _:
                logger.error(f"No Byte stream format matches {format}")
                return None

    except Exception:
        logger.error("Something went wrong ,", exc_info=True)
        return None

    buffer.seek(0)

    return buffer
