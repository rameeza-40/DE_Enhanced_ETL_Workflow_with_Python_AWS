import pandas as pd
import os
import zipfile
import requests
import boto3
import sqlalchemy
import credential
import sqlalchemy
from sqlalchemy import create_engine, text
import mysql.connector
from datetime import datetime

# === 1. SETUP FOLDERS & LOGGING ===
project_folder = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(project_folder, "etl_log.txt")
extracted_folder = os.path.join(project_folder, "Extracted_Data")
output_file = os.path.join(project_folder, "Transformed_Data.csv")

# === 2. LOGGING FUNCTION ===
def log_progress(message):
    with open(log_file, "a") as log:
        log.write(f"{datetime.now()}: {message}\n")

log_progress("ETL process started.")

# === 3. DOWNLOAD & EXTRACT DATA ===
url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip"
zip_path = os.path.join(project_folder, "source.zip")

response = requests.get(url)
if response.status_code == 200:
    with open(zip_path, "wb") as file:
        file.write(response.content)
    log_progress("File downloaded successfully.")
else:
    log_progress("Failed to download file.")
    exit()

with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall(extracted_folder)
log_progress("Files extracted.")

# === 4. EXTRACT DATA FROM FILES ===
csv_files = []
json_files = []
xml_files = []

for file in os.listdir(extracted_folder):
    if file.endswith(".csv"):
        csv_files.append(file)
    elif file.endswith(".json"):
        json_files.append(file)
    elif file.endswith(".xml"):
        xml_files.append(file)

combined_data = pd.DataFrame()

def extract_csv(file_path):
    return pd.read_csv(file_path)

def extract_json(file_path):
    return pd.read_json(file_path, lines=True)

def extract_xml(file_path):
    import xml.etree.ElementTree as ET
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = [{"name": p.find("name").text, "height": p.find("height").text, "weight": p.find("weight").text} for p in root.findall("person")]
    return pd.DataFrame(data)

for file in csv_files:
    combined_data = pd.concat([combined_data, extract_csv(os.path.join(extracted_folder, file))], ignore_index=True)
    log_progress(f"Extracted data from {file}")

for file in json_files:
    combined_data = pd.concat([combined_data, extract_json(os.path.join(extracted_folder, file))], ignore_index=True)
    log_progress(f"Extracted data from {file}")

for file in xml_files:
    combined_data = pd.concat([combined_data, extract_xml(os.path.join(extracted_folder, file))], ignore_index=True)
    log_progress(f"Extracted data from {file}")

# === 5. TRANSFORM DATA ===
combined_data["height"] = combined_data["height"].astype(float) * 0.0254  # Inches to meters
combined_data["weight"] = combined_data["weight"].astype(float) * 0.453592  # Pounds to kg
combined_data.to_csv(output_file, index=False)
log_progress("Data transformation completed.")

# === 6. UPLOAD TO AWS S3 ===
Aws_credential = credential.s3_bucket
AWS_ACCESS_KEY = Aws_credential["ACCESS_KEY"]
AWS_SECRET_KEY = Aws_credential["SECRET_KEY"]
BUCKET_NAME = Aws_credential["B_NAME"]
s3_client = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
s3_client.upload_file(output_file, BUCKET_NAME, "Transformed_Data.csv")
log_progress("File uploaded to S3.")

# === 7. LOAD TO AWS RDS (MYSQL) ===
MySql_Credential=credential.R_Mysql_credential
password=credential.MYSQL_CREDENTIALS
 
MySql_User=MySql_Credential['username']
MySql_Password=MySql_Credential['password']
MySql_DataBase=MySql_Credential['Database']
MySql_Host=MySql_Credential['host']
MySql_Port=MySql_Credential['port']
 
engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{MySql_User}:{MySql_Password}@{MySql_Host}/{MySql_DataBase}")
combined_data.to_sql("ETL_Table", con=engine, if_exists="replace", index=False)
log_progress("Data loaded into RDS MySQL.")

log_progress("ETL process completed successfully.")



