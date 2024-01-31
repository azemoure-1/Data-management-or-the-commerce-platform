import pandas as pd
import json
from datetime import datetime
from sqlalchemy import create_engine
import os

# Assuming df is your DataFrame containing the 'title' column
# If the 'title' column is not already a string, you may need to convert it using df['title'].astype(str)
df = pd.read_csv("./csv/df_final1.csv")

# Create a folder named 'data_csv' if it doesn't exist
data_folder = './docker-hadoop/workspace/data_csv'
os.makedirs(data_folder, exist_ok=True)

# Connection string for SQL Server
connection_string = 'mssql+pyodbc://amine:Amine@192.168.56.1/wh?driver=ODBC+Driver+17+for+SQL+Server'

# Create the SQL Alchemy engine
engine = create_engine(connection_string)

# File to store the state of global variables
state_file = 'state.json'

# Initialize variables to keep track of the range of rows to select
start_row = 0

def load_state():
    global start_row
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            state = json.load(f)
            start_row = state.get('start_row', 0)

def save_state():
    state = {'start_row': start_row}
    with open(state_file, 'w') as f:
        json.dump(state, f)

def save_to_csv_and_db():
    global start_row

    # Select the desired subset of rows based on the current range (4 rows)
    subset = df.iloc[start_row:start_row + 4].copy()

    # Add a new column with the current date and time
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    subset['current_datetime'] = current_datetime

    # Save the first 2 rows to CSV
    subset_csv = subset.iloc[:2]
    filename_csv = f"{data_folder}/data_csv_{current_datetime.replace(':', '-')}.csv"
    subset_csv.to_csv(filename_csv, index=False)

    # Save the next 2 rows to the database
    subset_db = subset.iloc[2:]
    subset_db.to_sql('data', con=engine, if_exists='append', index=False)

    # Update the start_row variable for the next execution
    start_row += 4

    return subset.to_dict(orient='records')

if __name__ == '__main__':
    # Load the state from the file
    load_state()

    # Example: Run the function
    result = save_to_csv_and_db()

    # Save the updated state to the file
    save_state()
