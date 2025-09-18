# ZAPIER_WEBHOOK_URL : https://hooks.zapier.com/hooks/catch/9983459/2zhpzxb/
# S3_BUCKET : needacarinventory
# S3_KEY : customers/CUSTOMER-AUTOGROUP.CSV
# CHECKPOINT_S3_BUCKET : needacarinventory
# CHECKPOINT_S3_KEY : customers/checkpoint.txt


import os
import csv
import json
import requests
import boto3
from io import StringIO
from datetime import datetime
from dateutil.relativedelta import relativedelta

ZAPIER_WEBHOOK_URL = os.environ["ZAPIER_WEBHOOK_URL"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_KEY = os.environ["S3_KEY"]
CHECKPOINT_S3_BUCKET = os.environ["CHECKPOINT_S3_BUCKET"]
CHECKPOINT_S3_KEY = os.environ["CHECKPOINT_S3_KEY"]

customer_columns = [
    "customer_id",
    "first_name",
    "last_name",
    "company",
    "address_line1",
    "city",
    "province_state",
    "postal_code",
    "tel_residence",
    "tel_business",
    "tel_other",
    "email",
    "birth_date",
]

deal_columns = [
    "deal_id",
    "customer_id",
    "vehicle_vin",
    "purchase_date",
    "selling_price",
    "payment_type",
    "sman1",
    "term_months",
    "rate",
    "in_house_finance",
    "payment",
    "freq",
    "bank",
    "finance_balance",
    "total_price",
    "vehicle_cost",
    "ro_cost",
]

header_mapping = {
    "CUSTOMER-NO.": "customer_id",
    "CUSTOMER-ID": "customer_id",
    "FIRST NAME": "first_name",
    "LAST NAME": "last_name",
    "COMPANY": "company",
    "ADDRESS LINE.1": "address_line1",
    "CITY": "city",
    "PROVINCE/STATE": "province_state",
    "POSTAL CODE": "postal_code",
    "TEL.RESIDENCE": "tel_residence",
    "TEL.BUSINESS": "tel_business",
    "TEL.OTHER": "tel_other",
    "E-MAIL": "email",
    "BIRTH DATE": "birth_date",
    "DEAL-ID": "deal_id",
    "V.I.N.": "vehicle_vin",
    "PURCHASE DATE": "purchase_date",
    "SELLING PRICE": "selling_price",
    "PAYMENT TYPE": "payment_type",
    "SMAN1": "sman1",
    "TERM (MONTHS)": "term_months",
    "RATE": "rate",
    "IN-HOUSE FINANCE": "in_house_finance",
    "PAYMENT": "payment",
    "FREQUENCY": "freq",
    "BANK": "bank",
    "FINANCE BALANCE": "finance_balance",
    "TOTAL PRICE": "total_price",
    "VEHICLE COST": "vehicle_cost",
    "RO COST": "ro_cost",
}

def load_last_sent_date():
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=CHECKPOINT_S3_BUCKET, Key=CHECKPOINT_S3_KEY)
        date_str = response["Body"].read().decode("utf-8").strip()
        if date_str:
            return datetime.strptime(date_str, "%Y-%m-%d")
    except s3.exceptions.NoSuchKey:
        print(json.dumps({"level": "info", "message": "No checkpoint file found, returning None"}))
    except Exception as e:
        print(json.dumps({"level": "error", "message": "Error loading checkpoint", "error": str(e)}))
    return None

def save_last_sent_date(date_obj):
    s3 = boto3.client("s3")
    try:
        s3.put_object(
            Bucket=CHECKPOINT_S3_BUCKET,
            Key=CHECKPOINT_S3_KEY,
            Body=date_obj.strftime("%Y-%m-%d")
        )
        print(json.dumps({"level": "info", "message": "Checkpoint saved", "date": date_obj.strftime("%Y-%m-%d")}))
    except Exception as e:
        print(json.dumps({"level": "error", "message": "Error saving checkpoint", "error": str(e)}))

def main():
    last_sent_date = load_last_sent_date()
    cutoff_date = datetime.now() - relativedelta(months=6)
    valid_rows = []

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    csv_content = response["Body"].read().decode("utf-8")

    reader = csv.DictReader(
        csv_content.splitlines(),
        delimiter="\t",
        quotechar=None,
        quoting=csv.QUOTE_NONE
    )

    for row in reader:
        raw_cleaned = {}
        for raw_key, raw_value in row.items():
            if raw_key is None:
                continue
            key_no_quotes = raw_key.strip().strip('"')
            val_no_quotes = raw_value.strip().strip('"') if raw_value else ""
            if key_no_quotes in header_mapping:
                mapped_col = header_mapping[key_no_quotes]
                raw_cleaned[mapped_col] = val_no_quotes

        customer_dict = {col: raw_cleaned.get(col, "") for col in customer_columns}
        deal_dict = {col: raw_cleaned.get(col, "") for col in deal_columns}

        purchase_date_str = deal_dict.get("purchase_date", "").strip()
        if purchase_date_str in ["", "  /  /"]:
            continue
        try:
            purchase_date_dt = datetime.strptime(purchase_date_str, "%m/%d/%Y")
        except ValueError:
            continue
        if purchase_date_dt.year != datetime.now().year:
            continue
        if purchase_date_dt < cutoff_date:
            continue
        valid_rows.append((purchase_date_dt, customer_dict, deal_dict))

    valid_rows.sort(key=lambda x: x[0])
    if not valid_rows:
        print(json.dumps({"level": "info", "message": "No valid rows found"}))
        return

    if last_sent_date is None:
        to_send = [valid_rows[-1]]
        print(json.dumps({"level": "info", "message": "No checkpoint found, sending the newest row"}))
    else:
        to_send = [r for r in valid_rows if r[0] > last_sent_date]
        print(json.dumps({
            "level": "info",
            "message": "Filtered rows by last_sent_date",
            "last_sent_date": last_sent_date.strftime("%Y-%m-%d"),
            "to_send_count": len(to_send)
        }))

    if not to_send:
        print(json.dumps({"level": "info", "message": "No new data to send"}))
        return

    for row_data in to_send:
        purchase_date_dt, customer_dict, deal_dict = row_data
        payload = {"customer_details": customer_dict, "deals": deal_dict}
        try:
            resp = requests.post(ZAPIER_WEBHOOK_URL, json=payload, timeout=10)
            resp.raise_for_status()
            print(json.dumps({
                "level": "info",
                "message": "Sent row to Zapier successfully",
                "purchase_date": purchase_date_dt.strftime("%Y-%m-%d"),
            }))
        except Exception as e:
            print(json.dumps({
                "level": "error",
                "message": "Failed to send row to Zapier",
                "purchase_date": purchase_date_dt.strftime("%Y-%m-%d"),
                "error": str(e),
            }))

    newest_date = to_send[-1][0]
    save_last_sent_date(newest_date)

def lambda_handler(event, context):
    main()
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Script executed successfully"})
    }
