import pandas as pd
from flask import Flask, request, jsonify, send_file
import io
from flask_cors import CORS
import zipfile

app = Flask(__name__)
CORS(app)

processed_csv_buffer = None

@app.route('/')
def serve_frontend():
    return send_file('index.html')

@app.route('/process-data', methods=['POST'])
def process_data():
    global processed_csv_buffer

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

    processed_csv_buffer = io.StringIO()
    df_sorted.to_csv(processed_csv_buffer, index=False)
    processed_csv_buffer.seek(0)
    
    return processed_csv_buffer.getvalue(), 200, {'Content-Type': 'text/csv'}

@app.route('/download-zip', methods=['GET'])
def download_zip():
    global processed_csv_buffer
    if processed_csv_buffer is None:
        return jsonify({'error': 'No processed CSV available'}), 400

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr('processed.csv', processed_csv_buffer.getvalue())

    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        mimetype='application/zip',
        download_name='processed_csv.zip',
        as_attachment=True
    )

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
    app.run(host='0.0.0.0', port=5001)
