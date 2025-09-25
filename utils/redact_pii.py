import pandas as pd
import logging

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)

logger = logging.getLogger(__name__)


def redact_pii(df_file: pd.DataFrame, pii_fields: list) -> pd.DataFrame:
    """
    redact the pii field in the df based on the
    pii_field input

    Args:
        df_file: DataFrame contaning file content
        pii_fields: list contaning fields to be redaced

    Return:
        Dataframe with pii fields being redacted
    """

    # taking a deep copy to avoid mutating original df
    copy_df = df_file.copy(deep=True)

    # looping though pii list and redacing each column
    for field in pii_fields:
        if field in copy_df.columns:
            copy_df[field] = "***"
        else:
            logger.warning(f"pii field: {field} was not found")

    return copy_df
