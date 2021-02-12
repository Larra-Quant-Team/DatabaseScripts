import email, smtplib, ssl
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os 

class Mailer():

    def __init__(self, subject, body, html, reciver):
        self.subject = subject
        self.body = body
        self.html = html
        self.sender_email = os.environ.get('EMAIL_SENDER')
        self.receiver_email = reciver
        self.password = os.environ.get('EMAIL_PASSWORD')
        self.message = None

    def create_message(self, filepath, filename):
        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = self.sender_email
        message["To"] = self.receiver_email
        message["Subject"] = self.subject
        message["Bcc"] = self.receiver_email  # Recommended for mass emails

        # Add body to email
        #message.attach(MIMEText(self.body, "plain"))
        message.attach(MIMEText(self.html, "html"))

        #filename = "test.pdf"  # In same directory as script

        # Open PDF file in binary mode
        with open(filepath, "rb") as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        # Encode file in ASCII characters to send by email    
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment part
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {filename}",
        )

        # Add attachment to message and convert message to string
        message.attach(part)
        self.text = message.as_string()
        return
    
    def send_message(self, reciber):    
        # Log in to server using secure context and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(self.sender_email, self.password)
            server.sendmail(self.sender_email, reciber, self.text)    
            