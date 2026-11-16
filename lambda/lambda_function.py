import json
import os
import urllib.parse
import boto3

s3 = boto3.client("s3")
sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def lambda_handler(event, context):
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        print(f"New invoice detected: s3://{bucket}/{key}")

        # 1. Read the raw invoice JSON from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        invoice = json.loads(response["Body"].read().decode("utf-8"))

        amount = float(invoice["amount"])

        # 2. Apply 10% bulk discount if amount >= 5000 (before GST)
        if amount >= 5000:
            discount = round(amount * 0.10, 2)
        else:
            discount = 0.0

        taxable_amount = round(amount - discount, 2)
        gst = round(taxable_amount * 0.18, 2)
        grand_total = round(taxable_amount + gst, 2)

        # 3. Build summary with required new keys: discount and taxable_amount
        summary = {
            "invoice_id": invoice["invoice_id"],
            "customer": invoice["customer"],
            "item": invoice["item"],
            "quantity": invoice["quantity"],
            "amount": amount,
            "discount": discount,
            "taxable_amount": taxable_amount,
            "gst_18_percent": gst,
            "grand_total": grand_total,
        }

        # 4. Write summary JSON to processed/
        base_name = os.path.basename(key).replace(".json", "")
        summary_key = f"processed/{base_name}_summary.json"
        s3.put_object(
            Bucket=bucket,
            Key=summary_key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json",
        )
        print(f"Summary written: s3://{bucket}/{summary_key}")

        # 5. Send SNS email with Discount line between Amount and GST
        subject = f"New invoice {invoice['invoice_id']}"
        message = (
            f"Invoice ID: {invoice['invoice_id']}\n"
            f"Customer: {invoice['customer']}\n"
            f"Item: {invoice['item']} x {invoice['quantity']}\n"
            f"Amount: Rs {amount:.2f}\n"
            f"Discount: Rs {discount:.2f}\n"
            f"GST (18%): Rs {gst:.2f}\n"
            f"Grand Total: Rs {grand_total:.2f}\n"
        )
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
        print("Email notification sent through SNS")

    return {"statusCode": 200, "body": json.dumps("Processed successfully")}