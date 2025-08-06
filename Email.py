import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os

def send_email_with_smtp():
    # Credentials should be stored securely, e.g., as environment variables
    SMTP_SERVER = "smtp.office365.com"
    SMTP_PORT = 587
    SENDER_EMAIL = "your-corporate-email@yourcompany.com"
    SENDER_PASSWORD = os.environ.get("SMTP_PASSWORD") # Get from secure store
    RECIPIENT_EMAILS = ["user1@example.com"]

    # Create the email
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "EDP Job Successful: Data Processing Complete"
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECIPIENT_EMAILS)

    # Your customized body
    html_body = "<h1>Job Finished</h1><p>The process completed successfully.</p>"

    # Attach the body to the email
    msg.attach(MIMEText(html_body, 'html'))

    # Send the email
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls() # Secure the connection
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
            print("SMTP email sent successfully!")
    except Exception as e:
        print(f"Failed to send SMTP email: {e}")

# You would call this function inside the `try` block of your main() function.
