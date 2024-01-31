from flask import Flask, jsonify
import pandas as pd
import time

app = Flask(__name__)

# Load the CSV file into a DataFrame
df = pd.read_csv('C:/Users/Youcode/OneDrive/Bureau/workspace/BI/airflow/csv/df_final2.csv')

# Define a global variable to keep track of the current index
current_index = 0

# Define the endpoint to get the next 5 rows
@app.route('/get_next_rows', methods=['GET'])
def get_next_rows():
    global current_index

    # Check if we have reached the end of the DataFrame
    if current_index + 5 > len(df):
        return jsonify({'message': 'End of data reached'})

    # Get the next 5 rows
    next_rows = df.iloc[current_index:current_index + 5].to_dict(orient='records')

    # Increment the current index for the next request
    current_index += 5

    return jsonify(next_rows)

if __name__ == '__main__':
    app.run(debug=True, threaded=True)
