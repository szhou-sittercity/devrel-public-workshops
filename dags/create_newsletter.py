import os

from airflow.sdk import asset, Asset, Metadata


# set these enviroment variables in order to store the newsletter 
# in cloud object storage instead of the local filesystem
OBJECT_STORAGE_SYSTEM = os.getenv("OBJECT_STORAGE_SYSTEM", default="file")
OBJECT_STORAGE_CONN_ID = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)
OBJECT_STORAGE_PATH_NEWSLETTER = os.getenv(
    "OBJECT_STORAGE_PATH_NEWSLETTER",
    default="include/newsletter",
)



@asset(schedule="@daily")
def raw_zen_quotes(context: dict):
    """
    Extracts a random set of quotes.
    """
    import requests

    r = requests.get("https://zenquotes.io/api/quotes/random")
    quotes = r.json()

    run_date = context["dag_run"].logical_date.strftime("%Y-%m-%d")

    # attach the run date to the asset event
    yield Metadata(Asset("raw_zen_quotes"), {"run_date": run_date})

    return quotes


@asset(schedule=[raw_zen_quotes])
def selected_quotes(context: dict):
    """
    Transforms the extracted raw_zen_quotes.
    """
    import numpy as np

    raw_zen_quotes = context["ti"].xcom_pull(
        dag_id="raw_zen_quotes",
        task_ids="raw_zen_quotes",  # FIXED: use string, not list
        key="return_value",
        include_prior_dates=True,
    )

    quotes_character_counts = [int(quote["c"]) for quote in raw_zen_quotes]
    median = np.median(quotes_character_counts)

    median_quote = min(
        raw_zen_quotes,
        key=lambda quote: abs(int(quote["c"]) - median),
    )
    raw_zen_quotes.pop(raw_zen_quotes.index(median_quote))
    short_quote = [quote for quote in raw_zen_quotes if int(quote["c"]) < median][0]
    long_quote = [quote for quote in raw_zen_quotes if int(quote["c"]) > median][0]

    # fetch the run date of the pipeline
    run_date = context["triggering_asset_events"][Asset("raw_zen_quotes")][0].extra[
        "run_date"
    ]

    # attach the run date to the asset event
    yield Metadata(Asset("selected_quotes"), {"run_date": run_date})

    return {
        "median_q": median_quote,
        "short_q": short_quote,
        "long_q": long_quote,
    }
    
@asset(schedule="@daily")
def formatted_newsletter(context: dict):
"""
Formats the newsletter.
"""
from airflow.io.path import ObjectStoragePath

object_storage_path = ObjectStoragePath(
   f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH_NEWSLETTER}",
   conn_id=OBJECT_STORAGE_CONN_ID,
)

selected_quotes = context["ti"].xcom_pull(
   dag_id="selected_quotes",
   task_ids="selected_quotes",
   key="return_value",
   include_prior_dates=True,
)

# fetch the run date of the pipeline
run_date = context["triggering_asset_events"][Asset("selected_quotes")][0].extra[
   "run_date"
]

newsletter_template_path = object_storage_path / "newsletter_template.txt"

newsletter_template = newsletter_template_path.read_text()

newsletter = newsletter_template.format(
   quote_text_1=selected_quotes["short_q"]["q"],
   quote_author_1=selected_quotes["short_q"]["a"],
   quote_text_2=selected_quotes["median_q"]["q"],
   quote_author_2=selected_quotes["median_q"]["a"],
   quote_text_3=selected_quotes["long_q"]["q"],
   quote_author_3=selected_quotes["long_q"]["a"],
   date=run_date,
)

date_newsletter_path = object_storage_path / f"{run_date}_newsletter.txt"

date_newsletter_path.write_text(newsletter)

# attach the run date to the asset event
yield Metadata(Asset("formatted_newsletter"), {"run_date": run_date})
