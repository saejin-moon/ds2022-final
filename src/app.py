import pandas as pd
from flask import Flask, request, jsonify, send_file
import io
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/')
def serve_frontend():
    return send_file('index.html')

@app.route('/process-data', methods=['POST'])
def process_data():
    if 'csv_file' not in request.files:
        return jsonify({'error': 'No CSV file provided'}), 400

    csv_file = request.files['csv_file']
    if not csv_file.filename.endswith('.csv'):
        return jsonify({'error': 'File must be a CSV'}), 400

    sort_column = request.form.get('sort_column')
    ignored_columns_str = request.form.get('ignored_columns', '') 
    ignored_columns = [col.strip() for col in ignored_columns_str.split(',') if col.strip()]

    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        return jsonify({'error': f'Error reading CSV: {e}'}), 400

    if not sort_column or sort_column not in df.columns:
        return jsonify({'error': f'Invalid or missing sort column: {sort_column}'}), 400
        
    all_columns = set(df.columns)
    valid_ignored_columns = set(col for col in ignored_columns if col in df.columns)
    
    subset_for_dropna = list(all_columns - valid_ignored_columns)

    df_cleaned = df.dropna(subset=subset_for_dropna)
    
    try:
        df_sorted = df_cleaned.sort_values(by=sort_column, ascending=True)
    except KeyError:
        return jsonify({'error': f'Sort column "{sort_column}" not found after cleaning.'}), 400
    except Exception as e:
        return jsonify({'error': f'Error during sorting: {e}'}), 500

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
        df = pd.read_csv(csv_file, nrows=0) 
        columns = df.columns.tolist()
        return jsonify({'columns': columns}), 200
    except Exception as e:
        return jsonify({'error': f'Error reading CSV headers: {e}'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)