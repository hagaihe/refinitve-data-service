import os.path
import time
from datetime import datetime

import pandas as pd
import refinitiv.data as rd

from app.config import APP
from app.utils import convert_to_ric

conf = APP.conf

session = rd.session.platform.Definition(
    app_key=conf.refinitiv_app_key,
    signon_control=False,
    grant=rd.session.platform.GrantPassword(
        username=conf.refinitiv_username,
        password=conf.refinitiv_password
    )
).get_session()


def check_state(state, message, session):
    print(f"State: {state}")
    print(f"Message: {message}")
    print("\n")


# Add callback to session
session.on_event(check_state)
rd.get_config()["http.request-timeout"] = 300  # Increase timeout to 300 seconds (5 minutes)
session.open()
rd.session.set_default(session)

# Define the start date and end date for the last 5 years
start_date = datetime(2019, 1, 1)
end_date = datetime(2024, 9, 1)

input_universe = ["QQQ"]
fields = ['TR.InvestorFullName', 'TR.PctOfSharesOutHeld', 'TR.SharesHeld.calcdate', 'TR.HoldingsDate', 'TR.SharesHeld',
          'TR.SharesHeldChange', 'TR.SharesHeldValue']

print(f"convert {len(input_universe)} symbols to rics")
converted_symbols_dict = convert_to_ric(input_universe)
qqq_ric = [s for s in converted_symbols_dict.values() if s is not None]
csv_filename = f"{qqq_ric[0].split('.')[0]}.holdings.csv"

try:
    # Initialize the CSV file and write headers if it doesn't exist
    if os.path.exists(csv_filename):
        os.remove(csv_filename)

    print(f"create new {csv_filename} db file")
    with open(csv_filename, mode='w') as file:
        file.write(f"{','.join(fields)}\n")

    # Loop through each period in the last 5 years
    current_date = start_date
    while current_date < end_date:
        # Get the first day of the current month
        sdate_str = current_date.strftime('%Y-%m-%d')

        # Calculate the first day of the next month
        if current_date.month == 12:
            next_date = datetime(current_date.year + 1, 1, 1)
        else:
            next_date = datetime(current_date.year, current_date.month + 1, 1)

        edate_str = next_date.strftime('%Y-%m-%d')

        print(f"Fetching data from {sdate_str} to {edate_str} using {rd.get_config()['http.request-timeout']} "
              f"sec timeout request...")

        # Retry logic for handling timeouts
        max_retries = 3
        retry_delay = 30  # seconds

        for attempt in range(max_retries):
            try:
                # Fetch the data for the current period
                df = rd.get_data(
                    qqq_ric,
                    fields,
                    {'SDate': sdate_str, 'EDate': edate_str, 'Frq': 'Q'}
                )
                if not df.empty:
                    break  # Exit the retry loop if successful
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    raise  # Re-raise the exception after all retries failed

        # Ensure that the DataFrame is not empty and that the 'Holdings Filing Date' column contains valid dates
        if not df.empty and df['Holdings Filing Date'].notna().any():
            # Convert the date column to a proper datetime format
            df['Holdings Filing Date'] = pd.to_datetime(df['Holdings Filing Date'])

            # Filter out any rows where 'Holdings Filing Date' is NaT
            df = df.dropna(subset=['Holdings Filing Date'])

            # Check for duplicates against the existing CSV file if it exists
            if os.path.exists(csv_filename):
                existing_data = pd.read_csv(csv_filename)
                if not existing_data.empty:
                    existing_data['Date'] = pd.to_datetime(existing_data['Date'])
                else:
                    # If the DataFrame is empty (i.e., only headers), initialize the 'Date' column
                    existing_data['Date'] = pd.Series(dtype='datetime64[ns]')
                df = df[~df.set_index(
                    ['Investor Full Name', 'Holdings Pct Of Traded Shares Held', 'Holdings Filing Date'])
                    .index.isin(existing_data.set_index(['Date', 'Holdings']).index)]

            # Continue if there's any new data after filtering out duplicates
            if not df.empty:
                # Group data by date and create the required holdings dictionary
                # grouped = df.groupby('Holdings Filing Date').apply(
                #     lambda x: x[['Investor Full Name', 'Holdings Pct Of Traded Shares Held']]
                #         .sort_values(by='Holdings Pct Of Traded Shares Held', ascending=False)
                #         .set_index('Investor Full Name')['Holdings Pct Of Traded Shares Held']
                #         .to_dict()
                # ).reset_index()
                #
                # grouped.columns = ['Date', 'Holdings']
                # grouped.to_csv(csv_filename, mode='a', header=False, index=False)
                df.to_csv(csv_filename, mode='a', header=False, index=False)
                print(f"Data for {sdate_str} to {edate_str} saved to {csv_filename}")
        else:
            print(f"No data for {sdate_str} to {edate_str}")

        # Move to the next period
        current_date = next_date
except Exception as e:
    print(str(e))
finally:
    # Close the session
    session.close()
