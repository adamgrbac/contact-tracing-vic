import pandas as pd
import sqlite3
import requests
from datetime import datetime

def get_data() -> list:

    ckanAPIKey = "f6325461-8ec7-4b06-99b3-cd64c014cca8"
    ckanResourceID = "afb52611-6061-4a2b-9110-74c920bede77"
    ckanBaseURL = "https://discover.data.vic.gov.au/api/3/action/datastore_search"
    api_base_url = "https://discover.data.vic.gov.au"

    headers = {"authorization": ckanAPIKey}

    res = requests.get(ckanBaseURL+"?resource_id="+ckanResourceID, headers=headers)

    res_dicts = []

    while res.status_code == 200 and len(res.json()["result"]["records"]) > 0:
        res_dicts += res.json()["result"]["records"]
        res = requests.get(api_base_url+res.json()["result"]["_links"]["next"], headers=headers)

    return res_dicts


def prep_database(con: sqlite3.Connection) -> None:

    cur = con.cursor()

    # Create history table if missing
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contact_tracing (
           severity varchar(256),
           data_date timestamp,
           data_location varchar(256),
           data_address varchar(256),
           data_suburb varchar(256),
           data_datetext varchar(256),
           data_timetext varchar(256),
           data_added  timestamp
        );""")

    cur.close()


def htmlify(df: pd.DataFrame) -> str:
    """
    Description:
        htmlify takes in a Pandas DataFrame and returns a prettified
        html version for insertion into an email.
    Arguments:
        df: pd.DataFrame - Pandas Dataframe to transform
    Returns:
        output: str - html string
    """

    output = "<ul>"
    for row in df.to_dict(orient="records"):
        output += f"<li>({row['severity']}) {row['data_location']}, {row['data_suburb']} on {row['data_datetext']} between {row['data_timetext']}</li>"
    output += "</ul>"
    return output


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Description:
        clean_dataframe cleans a pandas DataFrame by correcting formatting
        issues and creating some desired columns.
    Arguments:
        df: pd.DataFrame - Pandas Dataframe to clean
        table_name: str - Name of contact tracing table (used as column value)
    Returns:
        df: pd.DataFrame - cleaned pandas Dataframe
    """

    col_names = list(df.columns)

    df["severity"] = df["Advice_title"].apply(lambda x:  x[:6])
    df["data_date"] = pd.to_datetime(df["Exposure_date"], format="%d/%m/%Y")
    df["data_location"] = df["Site_title"]
    df["data_address"] = df["Site_streetaddress"]
    df["data_suburb"] = df["Suburb"]
    df["data_datetext"] = pd.to_datetime(df["Exposure_date"], format="%d/%m/%Y").dt.strftime("%A %d %B %Y")
    df["data_timetext"] = df["Exposure_time"]
    df["data_added"] = df.apply(lambda row: f"{row['Added_date']} {row['Added_time'] if row['Added_time'] else '00:00:00'}", axis=1)
    df["data_added"] = pd.to_datetime(df["data_added"], format="%d/%m/%Y %H:%M:%S")

    df = df.drop(col_names, axis=1)
    df = df.sort_values(by=["severity"])

    return df
