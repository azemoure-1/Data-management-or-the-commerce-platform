# from flask import Flask, jsonify
# import pandas as pd
# import time
# from datetime import datetime
# from sqlalchemy import create_engine
# import os

# app = Flask(__name__)

# # Assuming df is your DataFrame containing the 'title' column
# # If the 'title' column is not already a string, you may need to convert it using df['title'].astype(str)
# df = pd.read_csv("csv/df_final.csv")

# # Initialize variables to keep track of the range of rows to select
# start_row = 0
# batch_size = 2

# # Create a folder named 'data_csv' if it doesn't exist
# data_folder = './docker-hadoop/workspace/data_csv'
# os.makedirs(data_folder, exist_ok=True)

# # Set up the SQL Server connection
# # Replace 'your_server', 'your_database', 'your_username', and 'your_password' with your actual SQL Server details
# engine = create_engine('mssql+pyodbc://amine:Amine@localhost/wh?driver=ODBC+Driver+17+for+SQL+Server')

# @app.route('/get_data', methods=['GET'])
# def get_data():
#     global start_row

#     # Select the desired subset of rows based on the current range
#     subset = df.iloc[start_row:start_row + batch_size].copy()  # Copy the subset to avoid modifying the original DataFrame

#     # Add a new column with the current date and time
#     current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     subset['current_datetime'] = current_datetime

#     # Update the start_row variable for the next execution
#     start_row += batch_size

#     # Save the DataFrame as a CSV file in the 'data_csv' folder with the current date and time in the filename
#     filename = f"{data_folder}/subset_data_{current_datetime.replace(':', '-')}.csv"
#     subset.to_csv(filename, index=False)

#     # Introduce a 5-second delay (optional)
#     time.sleep(5)

#     # Insert data into SQL Server
#     subset.to_sql('data', con=engine, if_exists='append', index=False)

#     return jsonify(subset.to_dict(orient='records'))

# if __name__ == '__main__':
#     app.run(debug=True)

from flask import Flask, jsonify
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import create_engine
import os

app = Flask(__name__)

# Assuming df is your DataFrame containing the 'title' column
# If the 'title' column is not already a string, you may need to convert it using df['title'].astype(str)
df = pd.read_csv("csv/df_final.csv")

# Initialize variables to keep track of the range of rows to select
start_row_csv = 0
start_row_db = 2  # Initial value for the database

# Create a folder named 'data_csv' if it doesn't exist
data_folder = './docker-hadoop/workspace/data_csv'
os.makedirs(data_folder, exist_ok=True)

# Set up the SQL Server connection
# Replace 'your_server', 'your_database', 'your_username', and 'your_password' with your actual SQL Server details
engine = create_engine('mssql+pyodbc://amine:Amine@localhost/wh?driver=ODBC+Driver+17+for+SQL+Server')

@app.route('/save_to_csv', methods=['GET'])
def save_to_csv():
    global start_row_csv

    # Select the desired subset of rows based on the current range
    subset_csv = df.iloc[start_row_csv:start_row_csv + 2].copy()  # Copy the subset to avoid modifying the original DataFrame

    # Add a new column with the current date and time
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    subset_csv['current_datetime'] = current_datetime

    # Save the DataFrame as a CSV file in the 'data_csv' folder with the current date and time in the filename
    filename_csv = f"{data_folder}/data_{current_datetime.replace(':', '-')}.csv"
    subset_csv.to_csv(filename_csv, index=False)

    # Update the start_row_csv variable for the next execution
    start_row_csv += 2

    return jsonify(subset_csv.to_dict(orient='records'))

@app.route('/save_to_db', methods=['GET'])
def save_to_db():
    global start_row_db

    # Select the desired subset of rows based on the current range
    subset_db = df.iloc[start_row_db:start_row_db + 2].copy()  # Copy the subset to avoid modifying the original DataFrame

    # Add a new column with the current date and time
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    subset_db['current_datetime'] = current_datetime

    # Insert data into SQL Server
    subset_db.to_sql('data', con=engine, if_exists='append', index=False)

    # Update the start_row_db variable for the next execution
    start_row_db += 2

    return jsonify(subset_db.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)
