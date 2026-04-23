from flask import Flask, send_from_directory, request, jsonify
from flask_cors import CORS
import zipfile
import tarfile
import gzip
import csv
import os
import tempfile
import shutil
from typing import List

app = Flask(__name__, static_folder='templates')
CORS(app)

def extract_files_from_archive(file_bytes, filename: str, extract_to: str, file_extensions: List[str] = None) -> List[str]:
    """Extract data files from uploaded archive (ZIP, TAR, GZ, TAR.GZ)."""
    # If no extensions specified, extract ALL files
    extract_all = file_extensions is None or len(file_extensions) == 0
    
    extracted_files = []
    
    suffix = ''
    if filename.lower().endswith('.tar.gz') or filename.lower().endswith('.tgz'):
        suffix = '.tar.gz'
    elif filename.lower().endswith('.gz'):
        suffix = '.gz'
    elif filename.lower().endswith('.tar'):
        suffix = '.tar'
    elif filename.lower().endswith('.zip'):
        suffix = '.zip'
    
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name
    
    os.makedirs(extract_to, exist_ok=True)
    
    def matches_extension(name):
        if extract_all:
            return True
        return any(ext in name.lower() for ext in file_extensions)
    
    try:
        if filename.lower().endswith('.zip'):
            with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    if not file.endswith('/') and matches_extension(file):
                        zip_ref.extract(file, extract_to)
                        extracted_files.append(os.path.join(extract_to, file))
        
        elif filename.lower().endswith('.tar.gz') or filename.lower().endswith('.tgz'):
            with tarfile.open(tmp_path, 'r:gz') as tar_ref:
                for member in tar_ref.getmembers():
                    if member.isfile() and matches_extension(member.name):
                        tar_ref.extract(member, extract_to)
                        extracted_files.append(os.path.join(extract_to, member.name))
        
        elif filename.lower().endswith('.tar'):
            with tarfile.open(tmp_path, 'r:') as tar_ref:
                for member in tar_ref.getmembers():
                    if member.isfile() and matches_extension(member.name):
                        tar_ref.extract(member, extract_to)
                        extracted_files.append(os.path.join(extract_to, member.name))
        
        elif filename.lower().endswith('.gz'):
            output_name = filename[:-3]
            output_path = os.path.join(extract_to, output_name)
            with gzip.open(tmp_path, 'rb') as gz_file:
                with open(output_path, 'wb') as out_file:
                    shutil.copyfileobj(gz_file, out_file)
            if matches_extension(output_path):
                extracted_files.append(output_path)
    
    except Exception as e:
        print(f"Error extracting {filename}: {e}")
    finally:
        os.remove(tmp_path)
    
    return extracted_files

def count_sender_in_csv(csv_file: str, sender_address: str) -> dict:
    """Count occurrences of sender address in the 28th column, grouped by job type (14th column)."""
    jobtype_counts = {}
    total_count = 0
    try:
        with open(csv_file, newline='', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) >= 28:
                    cell_value = row[27].strip()
                    if cell_value and cell_value.lower() == sender_address.lower():
                        jobtype = row[13] if len(row) >= 14 else "Unknown"
                        jobtype = jobtype.strip() if jobtype else "Unknown"
                        if jobtype not in jobtype_counts:
                            jobtype_counts[jobtype] = 0
                        jobtype_counts[jobtype] += 1
                        total_count += 1
    except Exception as e:
        print(f"Error reading {csv_file}: {e}")
    return {"file": os.path.basename(csv_file), "jobtype_counts": jobtype_counts, "total": total_count}

def compare_archives(file1_bytes, file1_name, file2_bytes, file2_name, sender_address: str):
    """Compare two archive files and count sender address occurrences."""
    archive1_results = []
    archive2_results = []
    archive1_total = 0
    archive2_total = 0
    archive1_jobtype_totals = {}
    archive2_jobtype_totals = {}
    
    with tempfile.TemporaryDirectory() as temp_dir:
        if file1_bytes is not None and file1_name:
            csv_files = extract_files_from_archive(file1_bytes, file1_name, os.path.join(temp_dir, "archive1"))
            for csv_file in csv_files:
                result = count_sender_in_csv(csv_file, sender_address)
                archive1_results.append(result)
                archive1_total += result["total"]
                for jobtype, count in result["jobtype_counts"].items():
                    if jobtype not in archive1_jobtype_totals:
                        archive1_jobtype_totals[jobtype] = 0
                    archive1_jobtype_totals[jobtype] += count
        
        if file2_bytes is not None and file2_name:
            csv_files = extract_files_from_archive(file2_bytes, file2_name, os.path.join(temp_dir, "archive2"))
            for csv_file in csv_files:
                result = count_sender_in_csv(csv_file, sender_address)
                archive2_results.append(result)
                archive2_total += result["total"]
                for jobtype, count in result["jobtype_counts"].items():
                    if jobtype not in archive2_jobtype_totals:
                        archive2_jobtype_totals[jobtype] = 0
                    archive2_jobtype_totals[jobtype] += count
    
    return {
        "archive1": {"name": file1_name, "results": archive1_results, "total": archive1_total, "jobtype_totals": archive1_jobtype_totals},
        "archive2": {"name": file2_name, "results": archive2_results, "total": archive2_total, "jobtype_totals": archive2_jobtype_totals},
        "grand_total": archive1_total + archive2_total
    }

def analyze_archive_senders(file_bytes, file_name):
    """Analyze an archive and get counts for all sender addresses grouped by job type and org ID."""
    all_senders = {}
    all_orgs = set()
    all_jobtypes = set()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        if file_bytes is not None and file_name:
            csv_files = extract_files_from_archive(file_bytes, file_name, temp_dir)
            for csv_file in csv_files:
                try:
                    with open(csv_file, newline='', encoding='utf-8', errors='ignore') as f:
                        reader = csv.reader(f)
                        for row in reader:
                            if len(row) >= 28:
                                sender = row[27].strip()
                                if sender:
                                    jobtype = row[13].strip() if len(row) >= 14 and row[13] else "Unknown"
                                    org_id = row[1].strip() if len(row) >= 2 and row[1] else "Unknown"
                                    
                                    all_orgs.add(org_id)
                                    all_jobtypes.add(jobtype)
                                    
                                    if sender not in all_senders:
                                        all_senders[sender] = {"jobtypes": {}, "orgs": {}, "total": 0}
                                    if jobtype not in all_senders[sender]["jobtypes"]:
                                        all_senders[sender]["jobtypes"][jobtype] = 0
                                    all_senders[sender]["jobtypes"][jobtype] += 1
                                    
                                    if org_id not in all_senders[sender]["orgs"]:
                                        all_senders[sender]["orgs"][org_id] = 0
                                    all_senders[sender]["orgs"][org_id] += 1
                                    
                                    all_senders[sender]["total"] += 1
                except Exception as e:
                    print(f"Error reading {csv_file}: {e}")
    
    # Convert to list format for easier React handling
    senders_list = []
    for sender, data in all_senders.items():
        senders_list.append({
            "sender": sender,
            "jobtypes": data["jobtypes"],
            "orgs": data["orgs"],
            "total": data["total"]
        })
    
    # Sort by total descending
    senders_list.sort(key=lambda x: x["total"], reverse=True)
    
    return {
        "name": file_name,
        "senders": senders_list,
        "total_records": sum(s["total"] for s in senders_list),
        "unique_senders": len(senders_list),
        "unique_orgs": sorted(list(all_orgs)),
        "unique_jobtypes": sorted(list(all_jobtypes))
    }

# CDR Field Indices (0-based)
# Based on pipe-delimited CDR format
CDR_FIELDS = {
    's_tm': 2,                 # s_tm time field (index 2, 3rd field)
    'sender_address': 10,      # sender address (index 10, 11th field)
    'mobile_number': 11,       # mobile number
    'status': 27,              # status (DELIVERED, etc.) - 28th field (index 27)
    'camp_id': 37,             # camp id
    'txn_id': 38,              # txn id
    'campname': 39,            # campname
    'recipient_count': 40,     # recipient count
    'user_id': 43,             # user id
    'cp_submit_time': 44,      # cp submit time
    'interface_type': 45,      # interface type
    'is_bulk': 48,             # isBulk
    'jobtype': 50,             # jobtype
    'username': 51,            # username
    'contenttype': 52,         # contenttype
    'recipient': 54,           # recipient
    'category': 55,            # category
    'org_id': 58,              # orgid
    'fragment': 59,            # fragment
    'country': 61,             # country
    'accname': 62,             # accname
    'feature_id': 68,          # featureid
    'bind_cost': 70,           # bindcost
    'email': 71,               # email
    'submit_time': 80,         # submit time
    'c_tm': 89,                # c_tm time field (index 89, 90th field)
}

def parse_cdr_record(row):
    """Parse a CDR record and extract relevant fields."""
    record = {}
    for field_name, index in CDR_FIELDS.items():
        if len(row) > index:
            record[field_name] = row[index].strip() if row[index] else ''
        else:
            record[field_name] = ''
    return record

def analyze_cdr_archive(file_bytes, file_name):
    """Analyze CDR archive and extract all records with field breakdown."""
    records = []
    stats = {
        'total_records': 0,
        'by_status': {},
        'by_country': {},
        'by_sender': {},
        'by_username': {},
        'by_org': {},
        'by_jobtype': {},
    }
    
    with tempfile.TemporaryDirectory() as temp_dir:
        if file_bytes is not None and file_name:
            csv_files = extract_files_from_archive(file_bytes, file_name, temp_dir)
            for csv_file in csv_files:
                try:
                    with open(csv_file, newline='', encoding='utf-8', errors='ignore') as f:
                        reader = csv.reader(f)
                        for row in reader:
                            if len(row) > 10:  # Ensure it's a valid record
                                record = parse_cdr_record(row)
                                records.append(record)
                                stats['total_records'] += 1
                                
                                # Aggregate stats
                                for key, field in [
                                    ('by_status', 'status'),
                                    ('by_country', 'country'),
                                    ('by_sender', 'sender_address'),
                                    ('by_username', 'username'),
                                    ('by_org', 'org_id'),
                                    ('by_jobtype', 'jobtype'),
                                ]:
                                    value = record.get(field, '') or 'Unknown'
                                    if value not in stats[key]:
                                        stats[key][value] = 0
                                    stats[key][value] += 1
                except Exception as e:
                    print(f"Error reading {csv_file}: {e}")
    
    # Sort stats by count descending
    for key in ['by_status', 'by_country', 'by_sender', 'by_username', 'by_org', 'by_jobtype']:
        stats[key] = dict(sorted(stats[key].items(), key=lambda x: x[1], reverse=True))
    
    return {
        'name': file_name,
        'stats': stats,
        'records': records[:1000],  # Limit to first 1000 records for performance
        'total_records': stats['total_records']
    }

def search_cdr_archive(file_bytes, file_name, sender_address, time_field='c_tm'):
    """Search a single CDR archive for sender address with time field selection and status breakdown."""
    # Get the correct time field index
    time_field_index = CDR_FIELDS.get(time_field, CDR_FIELDS['c_tm'])
    sender_index = CDR_FIELDS['sender_address']
    status_index = CDR_FIELDS['status']
    
    result = {
        'name': file_name,
        'sender_address': sender_address,
        'time_field': time_field,
        'total_count': 0,
        'statuses': {},
        'files_processed': 0,
        'total_records_scanned': 0,
        'unique_senders_sample': [],  # Show first few senders found for debugging
        'files_found': []  # Debug: show file names found
    }
    
    unique_senders = set()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        if file_bytes is not None and file_name:
            # Extract ALL files from the archive (no extension filter)
            cdr_files = extract_files_from_archive(file_bytes, file_name, temp_dir, None)
            result['files_processed'] = len(cdr_files)
            result['files_found'] = [os.path.basename(f) for f in cdr_files[:10]]  # First 10 file names
            
            for cdr_file in cdr_files:
                try:
                    with open(cdr_file, 'r', encoding='utf-8', errors='ignore') as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                                
                            # CDR files use pipe | as delimiter
                            row = line.split('|')
                            result['total_records_scanned'] += 1
                            
                            # Check if row has enough columns
                            if len(row) > sender_index:
                                # Get sender address value
                                row_sender = row[sender_index].strip() if row[sender_index] else ''
                                
                                # Collect unique senders for debugging (first 20)
                                if row_sender and len(unique_senders) < 20:
                                    unique_senders.add(row_sender)
                                
                                # Check for exact match with sender address (case-insensitive)
                                if row_sender.lower() == sender_address.lower():
                                    # Check if time field has a value (for counting)
                                    time_value = ''
                                    if len(row) > time_field_index:
                                        time_value = row[time_field_index].strip() if row[time_field_index] else ''
                                    
                                    if time_value:  # Only count if time field has value
                                        result['total_count'] += 1
                                        
                                        # Get status value
                                        status = 'Unknown'
                                        if len(row) > status_index:
                                            status = row[status_index].strip() if row[status_index] else 'Unknown'
                                        
                                        if status not in result['statuses']:
                                            result['statuses'][status] = 0
                                        result['statuses'][status] += 1
                except Exception as e:
                    print(f"Error reading {cdr_file}: {e}")
            
            # Sort statuses by count descending
            result['statuses'] = dict(sorted(
                result['statuses'].items(), 
                key=lambda x: x[1], 
                reverse=True
            ))
            
            # Add sample of unique senders found
            result['unique_senders_sample'] = sorted(list(unique_senders))[:20]
    
    return result

def compare_cdr_archives(file1_bytes, file1_name, file2_bytes, file2_name, sender_address, time_field='c_tm'):
    """Compare two CDR archives based on sender address with time field selection and status breakdown."""
    results = {'file1': None, 'file2': None}
    
    # Get the correct time field index
    time_field_index = CDR_FIELDS.get(time_field, CDR_FIELDS['c_tm'])
    sender_index = CDR_FIELDS['sender_address']
    status_index = CDR_FIELDS['status']
    
    with tempfile.TemporaryDirectory() as temp_dir:
        for idx, (file_bytes, file_name, key) in enumerate([
            (file1_bytes, file1_name, 'file1'), 
            (file2_bytes, file2_name, 'file2')
        ]):
            if file_bytes is not None and file_name:
                archive_data = {
                    'name': file_name,
                    'total_count': 0,
                    'statuses': {},
                    'files_processed': 0
                }
                
                csv_files = extract_files_from_archive(file_bytes, file_name, os.path.join(temp_dir, f"archive{idx}"))
                archive_data['files_processed'] = len(csv_files)
                
                for csv_file in csv_files:
                    try:
                        with open(csv_file, newline='', encoding='utf-8', errors='ignore') as f:
                            reader = csv.reader(f)
                            for row in reader:
                                # Check if row has enough columns for time field check
                                if len(row) > max(sender_index, status_index, time_field_index):
                                    # Get sender address value
                                    row_sender = row[sender_index].strip() if len(row) > sender_index else ''
                                    
                                    # Check for exact match with sender address
                                    if row_sender.lower() == sender_address.lower():
                                        # Check if time field has a value (for counting)
                                        time_value = row[time_field_index].strip() if len(row) > time_field_index else ''
                                        
                                        if time_value:  # Only count if time field has value
                                            archive_data['total_count'] += 1
                                            
                                            # Get status value
                                            status = row[status_index].strip() if len(row) > status_index else 'Unknown'
                                            status = status if status else 'Unknown'
                                            
                                            if status not in archive_data['statuses']:
                                                archive_data['statuses'][status] = 0
                                            archive_data['statuses'][status] += 1
                    except Exception as e:
                        print(f"Error reading {csv_file}: {e}")
                
                # Sort statuses by count descending
                archive_data['statuses'] = dict(sorted(
                    archive_data['statuses'].items(), 
                    key=lambda x: x[1], 
                    reverse=True
                ))
                
                results[key] = archive_data
    
    return {
        'sender_address': sender_address,
        'time_field': time_field,
        'file1': results['file1'],
        'file2': results['file2']
    }

# Routes
@app.route('/')
def index():
    return send_from_directory('templates', 'index.html')

@app.route('/analyze')
def analyze_page():
    return send_from_directory('templates', 'analyze.html')

@app.route('/cdr')
def cdr_page():
    return send_from_directory('templates', 'cdr.html')

# API Endpoints
@app.route('/api/search', methods=['POST'])
def api_search():
    file1 = request.files.get('file1')
    file2 = request.files.get('file2')
    sender_address = request.form.get('sender_address', '').strip()

    if not file1 and not file2:
        return jsonify({"error": "Please upload at least one archive file."}), 400
    if not sender_address:
        return jsonify({"error": "Please enter a sender address."}), 400

    file1_bytes = file1.read() if file1 and file1.filename else None
    file1_name = file1.filename if file1 else None
    file2_bytes = file2.read() if file2 and file2.filename else None
    file2_name = file2.filename if file2 else None
    
    data = compare_archives(file1_bytes, file1_name, file2_bytes, file2_name, sender_address)
    return jsonify(data)

@app.route('/api/analyze', methods=['POST'])
def api_analyze():
    file1 = request.files.get('file1')

    if not file1 or not file1.filename:
        return jsonify({"error": "Please upload an archive file."}), 400

    file1_bytes = file1.read()
    file1_name = file1.filename
    data = analyze_archive_senders(file1_bytes, file1_name)
    return jsonify(data)

@app.route('/api/cdr/analyze', methods=['POST'])
def api_cdr_analyze():
    file1 = request.files.get('file1')

    if not file1 or not file1.filename:
        return jsonify({"error": "Please upload an archive file."}), 400

    file1_bytes = file1.read()
    file1_name = file1.filename
    data = analyze_cdr_archive(file1_bytes, file1_name)
    return jsonify(data)

@app.route('/api/cdr/search', methods=['POST'])
def api_cdr_search():
    file = request.files.get('file')
    sender_address = request.form.get('sender_address', '').strip()
    time_field = request.form.get('time_field', 'c_tm').strip()

    if not file or not file.filename:
        return jsonify({"error": "Please upload an archive file."}), 400
    if not sender_address:
        return jsonify({"error": "Please enter a sender address."}), 400

    file_bytes = file.read()
    file_name = file.filename
    data = search_cdr_archive(file_bytes, file_name, sender_address, time_field)
    return jsonify(data)

@app.route('/api/cdr/compare', methods=['POST'])
def api_cdr_compare():
    file1 = request.files.get('file1')
    file2 = request.files.get('file2')
    sender_address = request.form.get('sender_address', '').strip()
    time_field = request.form.get('time_field', 'c_tm').strip()

    if not file1 and not file2:
        return jsonify({"error": "Please upload at least one archive file."}), 400
    if not sender_address:
        return jsonify({"error": "Please enter a sender address."}), 400

    file1_bytes = file1.read() if file1 and file1.filename else None
    file1_name = file1.filename if file1 else None
    file2_bytes = file2.read() if file2 and file2.filename else None
    file2_name = file2.filename if file2 else None
    
    data = compare_cdr_archives(file1_bytes, file1_name, file2_bytes, file2_name, sender_address, time_field)
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
