import os
import boto3
import smtplib
import traceback

from flask import current_app, render_template
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class SendMessages:
    def send_email(self, to, body, filename=None, subject=None, cc=None, attach_s3_key=None, **kwargs):
        app = current_app._get_current_object()
        CHARSET = "UTF-8"
        BODY_HTML = render_template(f"email/{body}.html", **kwargs)

        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject or "The Seven Seas Group | Información importante"
        msg["From"] = os.getenv("SENDER")

        to_emails = to if os.getenv("ENV") == "production" else [os.getenv("TESTING_EMAIL")]
        msg["To"] = ", ".join(to_emails)

        if cc:
            msg["Cc"] = ", ".join(cc)
            all_recipients = to_emails + cc
        else:
            all_recipients = to_emails

        # Email body
        msg_body = MIMEMultipart("alternative")
        htmlpart = MIMEText(BODY_HTML.encode(CHARSET), "html", CHARSET)
        msg_body.attach(htmlpart)
        msg.attach(msg_body)

        # Optional attachment from S3
        if attach_s3_key:
            try:
                s3_client = boto3.client("s3")
                bucket = f"{os.getenv('S3_BUCKET_PREFIX')}-private-{os.getenv('S3_BUCKET_ENV')}"
                s3_object = s3_client.get_object(Bucket=bucket, Key=attach_s3_key)
                file_data = s3_object["Body"].read()

                attachment = MIMEApplication(file_data)
                attachment.add_header("Content-Disposition", "attachment", filename=filename or os.path.basename(attach_s3_key))
                msg.attach(attachment)
            except Exception:
                print(f"Error attaching file from S3 key: {attach_s3_key}")
                print(traceback.format_exc())

        # Send the email
        try:
            server = smtplib.SMTP(os.getenv("SMTP_SERVER"), os.getenv("SMTP_PORT"))
            server.starttls()
            server.login(os.getenv("SENDER"), os.getenv("EMAIL_PASSWORD"))
            server.sendmail(os.getenv("SENDER"), all_recipients, msg.as_string())
            server.quit()
        except Exception as e:
            print("Error sending email:")
            print(traceback.format_exc())