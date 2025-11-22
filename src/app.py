import pandas as pd
from flask import Flask, request, jsonify, send_file
import io
import zipfile
from flask_cors import CORS
import json

app = Flask(__name__)
CORS(app)

@app.route('/')
def serve_frontend():
    return send_file('index.html')

@app.route('/process-data', methods=['POST'])
def process_data():
    csv_file = request.files.get('csv_file')
    # If no new file is uploaded, this will be None, which is handled in the caching scenario
    
    # Get processing parameters
    sort_column = request.form.get('sort_column')
    ignored_columns_str = request.form.get('ignored_columns', '') 
    filter_query = request.form.get('filter_query')
    remove_duplicates = request.form.get('remove_duplicates') == 'true'

    ignored_columns = [col.strip() for col in ignored_columns_str.split(',') if col.strip()]

    # To handle the cached file: the frontend sends the file data *if* it's a new upload.
    # If this endpoint is hit without a 'csv_file', it implies the frontend is reusing a file.
    # We rely on the frontend to manage the file upload state for this single-instance app.
    # For a robust multi-user app, the backend would need to cache the file with a session/key.
    # Since the file is sent on every *processing* request, the logic below uses the submitted file.
    
    if csv_file and csv_file.filename.endswith('.csv'):
        # Read the CSV file into a pandas DataFrame from the uploaded file
        try:
            df = pd.read_csv(csv_file)
        except Exception as e:
            return jsonify({'error': f'Error reading CSV: {e}'}), 400
    else:
        # NOTE: This endpoint assumes the file is ALWAYS resent by the front-end for simplicity
        # of a single-file Flask app and proper data isolation between requests.
        # For requirement (h), the client must cache the file data and send it back with processing
        # options if no *new* file is selected. If no file is received here, it's an error.
        return jsonify({'error': 'No CSV file provided for processing.'}), 400


    # 1. REMOVE DUPLICATES (if flag is set)
    if remove_duplicates:
        initial_rows = len(df)
        df = df.drop_duplicates()
        print(f"Removed {initial_rows - len(df)} duplicate rows.")

    # 2. FILTER EXPRESSION (if provided)
    if filter_query:
        try:
            df = df.query(filter_query, engine='python') # Use 'python' engine for more compatibility
        except Exception as e:
            return jsonify({'error': f'Error applying filter query: {e}'}), 400

    # 3. DELETE ROWS with empty cells (excluding ignored columns)
    all_columns = set(df.columns)
    valid_ignored_columns = set(col for col in ignored_columns if col in df.columns)
    subset_for_dropna = list(all_columns - valid_ignored_columns)
    df_cleaned = df.dropna(subset=subset_for_dropna)

    # 4. SORT (if a column is selected)
    if sort_column and sort_column != 'NO_SORT':
        try:
            df_sorted = df_cleaned.sort_values(by=sort_column, ascending=True)
        except KeyError:
            return jsonify({'error': f'Sort column "{sort_column}" not found.'}), 400
        except Exception as e:
            return jsonify({'error': f'Error during sorting: {e}'}), 500
    else:
        df_sorted = df_cleaned
    
    # Send the processed DataFrame back as CSV text
    buffer = io.StringIO()
    df_sorted.to_csv(buffer, index=False)
    buffer.seek(0)
    
    return buffer.getvalue(), 200, {'Content-Type': 'text/csv'}

@app.route('/get-columns', methods=['POST'])
def get_columns():
    if 'csv_file' not in request.files:
        return jsonify({'error': 'No CSV file provided'}), 400

    csv_file = request.files['csv_file']
    if not csv_file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be a CSV'}), 400

    try:
        # Read file again from the stream to get headers
        df = pd.read_csv(csv_file, nrows=0) 
        columns = df.columns.tolist()
        return jsonify({'columns': columns}), 200
    except Exception as e:
        return jsonify({'error': f'Error reading CSV headers: {e}'}), 500

@app.route('/download-file', methods=['POST'])
def download_file():
    csv_data = request.data.decode('utf-8')
    file_format = request.args.get('format', 'csv').lower()

    if not csv_data:
        return jsonify({'error': 'No processed data received.'}), 400
    
    try:
        df = pd.read_csv(io.StringIO(csv_data))
    except Exception as e:
        return jsonify({'error': f'Error reading CSV data into DataFrame: {e}'}), 500

    buffer = io.BytesIO()
    download_name = 'processed_data'
    mimetype = ''

    if file_format == 'csv':
        df.to_csv(buffer, index=False)
        mimetype = 'text/csv'
        download_name += '.csv'
        buffer.seek(0)
        
    elif file_format == 'json':
        df.to_json(buffer, orient='records', indent=4)
        mimetype = 'application/json'
        download_name += '.json'
        buffer.seek(0)

    elif file_format == 'excel':
        df.to_excel(buffer, index=False, engine='xlsxwriter')
        mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        download_name += '.xlsx'
        buffer.seek(0)

    elif file_format == 'zip':
        with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr('processed_data.csv', csv_data)
        mimetype = 'application/zip'
        download_name += '.zip'
        buffer.seek(0)
    
    else:
        return jsonify({'error': f'Unsupported file format: {file_format}'}), 400

    return send_file(
        buffer,
        mimetype=mimetype,
        as_attachment=True,
        download_name=download_name
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)