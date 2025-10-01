import boto3
import time
import json
import os
import re

#  Initialize AWS Clients
s3_client = boto3.client("s3")
textract_client = boto3.client("textract")

# ⏳ Reduce polling delay (default: 5s → 2s)
POLLING_INTERVAL = 2  # Faster response checking

# 🔍 List PDFs in S3 Bucket
def list_s3_pdfs(bucket_name):
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    if "Contents" in response:
        return [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".pdf")]
    return []

# 🔍 Start AWS Textract Asynchronous Job
def start_textract_job(s3_bucket, file_name):
    print(f"📂 Starting Textract for: {file_name}")
    response = textract_client.start_document_text_detection(
        DocumentLocation={'S3Object': {'Bucket': s3_bucket, 'Name': file_name}}
    )
    return response["JobId"]

# 🔄 Check Textract Job Completion (Fast Polling)
def check_job_complete(job_id):
    print(f"⏳ Waiting for job {job_id} to complete...")
    while True:
        time.sleep(POLLING_INTERVAL)
        response = textract_client.get_document_text_detection(JobId=job_id)
        status = response["JobStatus"]
        print(f"🔍 Job {job_id} status: {status}")

        if status in ["SUCCEEDED", "FAILED"]:
            return status == "SUCCEEDED"

# 📥 Get Textract Job Results (Handles Pagination)
def get_textract_results(job_id):
    response = textract_client.get_document_text_detection(JobId=job_id)
    pages = [response]
    
    next_token = response.get("NextToken")
    while next_token:
        response = textract_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
        pages.append(response)
        next_token = response.get("NextToken")

    return pages

# 🛠 Extract Key Information from Text
def extract_info(text):
    structured_data = {
        "name": "",
        "surname": "",
        "email": "",
        "phone": "",
        "address": "",
        "skills": [],
        "experience": [],
        "education": [],
        "languages": [],
        "certifications": []
    }

    # 🔹 Extract Email
    email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
    if email_match:
        structured_data["email"] = email_match.group(0)

    # 🔹 Extract Phone Number (Handles International Formats)
    phone_match = re.search(r'(\+?\d{1,3}[-.\s]?)?(\(?\d{2,4}\)?[-.\s]?)?\d{3,4}[-.\s]?\d{3,4}', text)
    if phone_match:
        structured_data["phone"] = phone_match.group(0)

    # 🔹 Extract Name & Surname
    name_surname_match = re.findall(r"([A-Z][a-z]+)\s+([A-Z][a-z]+)", text)
    if name_surname_match:
        structured_data["name"], structured_data["surname"] = name_surname_match[0]

    # 🔹 Extract Address (Detects Common Address Words)
    address_match = re.findall(
        r"\d{1,5}[\w\s]+(?:Street|Avenue|Road|Rue|Blvd|Building|Apartment|Block|Tower|City|State|Zip|Postal|Country)", 
        text, re.IGNORECASE
    )
    if address_match:
        structured_data["address"] = " ".join(address_match[:2])  # Keep first two matches

    # 🔹 Extract Skills (Detects Lines Starting with "Skills/Compétences")
    skills_match = re.search(r"(?:skills|compétences)[:\s]+([\w\s,.-]+)", text, re.IGNORECASE)
    if skills_match:
        structured_data["skills"] = [skill.strip() for skill in re.split(r",|\.", skills_match.group(1))]

    # 🔹 Extract Experience (Years & Job Titles)
    exp_match = re.findall(r"(\d{1,2})\s?(?:years?|ans?)\s?(?:of experience|expérience)?", text, re.IGNORECASE)
    structured_data["experience"] = exp_match if exp_match else []

    # 🔹 Extract Education (Degrees & Years)
    edu_match = re.findall(r"(?:B\.?Sc|M\.?Sc|B\.?Tech|M\.?Tech|Ph\.?D|MBA|BE|BS|MS)\s?.*?(\d{4})", text)
    structured_data["education"] = edu_match if edu_match else []

    # 🔹 Extract Languages
    lang_match = re.search(r"(?:languages|langues)[:\s]+([\w\s,.-]+)", text, re.IGNORECASE)
    if lang_match:
        structured_data["languages"] = [lang.strip() for lang in re.split(r",|\.", lang_match.group(1))]

    # 🔹 Extract Certifications
    cert_match = re.search(r"(?:certifications)[:\s]+([\w\s,.-]+)", text, re.IGNORECASE)
    if cert_match:
        structured_data["certifications"] = [cert.strip() for cert in re.split(r",|\.", cert_match.group(1))]

    return structured_data

# 📌 Process All CVs in the S3 Bucket
def process_all_cvs(bucket_name, output_folder="structured_cvs"):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    pdf_files = list_s3_pdfs(bucket_name)
    if not pdf_files:
        print("⚠️ No PDF files found in the S3 bucket.")
        return

    print(f"📂 Found {len(pdf_files)} PDF files to process.")

    for document_name in pdf_files:
        print(f"📑 Processing: {document_name}")

        # Start AWS Textract Job
        job_id = start_textract_job(bucket_name, document_name)
        print(f"⏳ Job started: {job_id}")

        # Wait for job completion
        if check_job_complete(job_id):
            response = get_textract_results(job_id)

            # Extract structured data
            structured_text = {
                "filename": document_name,
                "structured_data": {
                    "name": "",
                    "surname": "",
                    "email": "",
                    "phone": "",
                    "address": "",
                    "skills": [],
                    "experience": [],
                    "education": [],
                    "languages": [],
                    "certifications": []
                }
            }

            # Extract raw text from AWS Textract
            raw_text = "\n".join(item["Text"] for page in response for item in page["Blocks"] if item["BlockType"] == "LINE")

            structured_text["structured_data"] = extract_info(raw_text)

            # Save structured results to JSON
            output_file = os.path.join(output_folder, f"{document_name.replace('.pdf', '.json')}")
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(structured_text, f, ensure_ascii=False, indent=4)

            print(f"✅ Saved: {output_file}")

# 🚀 Run the Processing Function
s3_bucket_name = "tekbootwebsite2"
process_all_cvs(s3_bucket_name)
