import requests
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO

# Database setup
conn = sqlite3.connect('cycling_data.db')
c = conn.cursor()

# Adjust the table structure according to the actual data structure from the image provided
c.execute('''CREATE TABLE IF NOT EXISTS cycling_data (
                rental_id INTEGER PRIMARY KEY,
                duration INTEGER,
                bike_id INTEGER,
                end_date TEXT,
                end_station_id INTEGER,
                end_station_name TEXT,
                start_date TEXT,
                start_station_id INTEGER,
                start_station_name TEXT
            )''')

def date_range_generator(start_date, end_date, delta):
    current_start_date = datetime.strptime(start_date, '%d%b%Y')
    current_end_date = datetime.strptime(end_date, '%d%b%Y')
    while True:
        yield current_start_date.strftime('%d%b%Y'), current_end_date.strftime('%d%b%Y')
        current_start_date = current_end_date + timedelta(days=1)
        current_end_date = current_start_date + delta

def download_and_insert(file_number, date_start, date_end):
    url = f"https://cycling.data.tfl.gov.uk/usage-stats/{file_number}JourneyDataExtract{date_start}-{date_end}.csv"
    
    try:
        r = requests.get(url)
        r.raise_for_status()  # This will raise an exception for HTTP errors
        
        # Convert the CSV data to a pandas DataFrame
        csv_data = StringIO(r.text)
        df = pd.read_csv(csv_data)
        
        # Insert data into the database
        for _, row in df.iterrows():
            c.execute(
                "INSERT INTO cycling_data (rental_id, duration, bike_id, end_date, end_station_id, "
                "end_station_name, start_date, start_station_id, start_station_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                (row['Rental Id'], row['Duration'], row['Bike Id'], row['End Date'], row['EndStation Id'], 
                 row['EndStation Name'], row['Start Date'], row['StartStation Id'], row['StartStation Name'])
            )
        
        conn.commit()
        print(f"Successfully downloaded and inserted data from {url}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return False

start_date = '30Dec2020'
end_date = '05Jan2021'
file_number = 246
delta = timedelta(days=6)  # Assuming each file covers a week

date_generator = date_range_generator(start_date, end_date, delta)

# Loop until a download fails
while True:
    start, end = next(date_generator)
    success = download_and_insert(file_number, start, end)
    if not success:
        break  # Exit the loop if download fails
    file_number += 1

conn.close()
