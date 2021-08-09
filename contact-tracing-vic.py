import pandas as pd
import sqlite3
import yagmail
import utils
import yaml
from datetime import datetime

# Load email config
with open("email_config.yml", "r") as f:
    email_config = yaml.safe_load(f)

# Setup Email details
yag = yagmail.SMTP(email_config["sender"], oauth2_file="oauth2_file.json")

# Open DB Connection
con = sqlite3.connect("contact_tracing_vic.db")
cur = con.cursor()

# Prep database tables
utils.prep_database(con)

# Get time of latest update & create table if missing
max_time = [row for row in cur.execute("SELECT max(data_added) FROM contact_tracing")][0][0]
if not max_time:
    max_time = datetime(1900, 1, 1, 0, 0, 0)

# Get Vic Data
res_dicts = utils.get_data()

df = pd.DataFrame(res_dicts)
df = utils.clean_dataframe(df)

# Get records that have appeared since last entry in the database
# Separate these rows into new rows and updated rows based on updated_flag
new_records = df[df["data_added"] > max_time]

# If there are any new / updated rows, process and email to dist list
if len(new_records) > 0:

    # Email body
    contents = []

    # Create upto two sections depending on presences of new/updated records
    if len(new_records) > 0:
        contents.append("New / Updated Contact Tracing Locations added to the website:")
        contents.append(utils.htmlify(new_records))

    # Send email to dist list
    yag.send(bcc=email_config["dist_list"], subject="New VIC Contact Tracing Locations!", contents=contents)

    # Insert new records into database to mark them as processed
    new_records.to_sql("contact_tracing", con, if_exists="append", index=False)

else:
    # For logging purposes
    print("No updates!")

# Close DB connection
con.close()
