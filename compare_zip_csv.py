import zipfile
import csv
import os
import tempfile
from typing import List
import streamlit as st

def extract_csv_files_from_bytes(zip_bytes, extract_to: str) -> List[str]:
    """Extract CSV files from uploaded ZIP bytes."""
    csv_files = []
    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp:
        tmp.write(zip_bytes)
        tmp_path = tmp.name
    with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
        for file in zip_ref.namelist():
            if file.lower().endswith('.csv'):
                zip_ref.extract(file, extract_to)
                csv_files.append(os.path.join(extract_to, file))
    os.remove(tmp_path)
    return csv_files

def count_sender_in_csv(csv_file: str, sender_address: str) -> dict:
    """Count occurrences of sender address in a CSV file."""
    count = 0
    with open(csv_file, newline='', encoding='utf-8', errors='ignore') as f:
        reader = csv.DictReader(f)
        for row in reader:
            for key in row:
                if 'sender' in key.lower():
                    if row[key] and sender_address.lower() in row[key].lower():
                        count += 1
    return {"file": os.path.basename(csv_file), "count": count}

def compare_zip_folders(zip1_bytes, zip2_bytes, sender_address: str):
    """Compare two ZIP files and count sender address occurrences."""
    results = []
    total_count = 0
    with tempfile.TemporaryDirectory() as temp_dir:
        for idx, zip_bytes in enumerate([zip1_bytes, zip2_bytes], 1):
            if zip_bytes is not None:
                csv_files = extract_csv_files_from_bytes(zip_bytes, os.path.join(temp_dir, f"zip{idx}"))
                for csv_file in csv_files:
                    result = count_sender_in_csv(csv_file, sender_address)
                    result["zip"] = f"ZIP {idx}"
                    results.append(result)
                    total_count += result["count"]
    return results, total_count

def main():
    st.set_page_config(page_title="ZIP CSV Sender Address Counter", layout="centered")
    st.title("📁 ZIP CSV Sender Address Counter")
    st.markdown("Upload two ZIP folders containing CSV files and enter a sender address to count occurrences.")

    col1, col2 = st.columns(2)
    with col1:
        zip1 = st.file_uploader("Upload First ZIP File", type=["zip"], key="zip1")
    with col2:
        zip2 = st.file_uploader("Upload Second ZIP File", type=["zip"], key="zip2")

    sender_address = st.text_input("Enter Sender Address to Search")

    if st.button("🔍 Search"):
        if not zip1 and not zip2:
            st.warning("Please upload at least one ZIP file.")
        elif not sender_address:
            st.warning("Please enter a sender address.")
        else:
            zip1_bytes = zip1.read() if zip1 else None
            zip2_bytes = zip2.read() if zip2 else None
            with st.spinner("Processing..."):
                results, total_count = compare_zip_folders(zip1_bytes, zip2_bytes, sender_address)
            st.success(f"**Total occurrences of `{sender_address}`: {total_count}**")
            if results:
                st.subheader("Breakdown by File")
                for r in results:
                    st.write(f"- **{r['zip']}** | `{r['file']}`: {r['count']} occurrence(s)")

if __name__ == '__main__':
    main()
