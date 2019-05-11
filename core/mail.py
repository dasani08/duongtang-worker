import smtplib
import ssl
from os import environ as env

APP_NOREPLY_EMAIL_ADDRESS = 'noreply@localhost'


class MailSender(object):
    def __init__(self):
        self.port = 587
        self.context = ssl.create_default_context()
        self.smtp_server = env.get('MAILGUN_SMTP_SERVER', 'localhost')
        self.username = env.get('MAILGUN_SMTP_USERNAME', 'admin')
        self.password = env.get('MAILGUN_SMTP_PASSWORD', 'admin')
        self.sender_email = APP_NOREPLY_EMAIL_ADDRESS

    def send(self, to_addrs, msg):
        try:
            server = smtplib.SMTP(self.smtp_server, self.port)
            server.starttls(context=self.context)  # Secure the connection
            server.login(self.username, self.password)
            # TODO: Send email here
            server.sendmail(self.sender_email, to_addrs, msg)
        except Exception as e:
            self.app.logger.warning(e)
        finally:
            server.quit()


mailer = MailSender()
