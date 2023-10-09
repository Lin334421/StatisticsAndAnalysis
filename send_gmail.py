# -*-coding:utf-8-*-
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email import encoders
import os

# 你的Gmail帐户凭据
email_address = 'linruo9936@gmail.com'
email_password = 'System797836'

# 收件人邮箱地址
to_email_address = 'linruo9936@163.com'

# 创建SMTP客户端
smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
smtp_server.starttls()  # 启用TLS加密

# 登录到Gmail帐户
smtp_server.login(email_address, email_password)

# 创建邮件对象
msg = MIMEMultipart()
msg['From'] = email_address
msg['To'] = to_email_address
msg['Subject'] = '主题：你好，这是一封通过Python发送的邮件'

# 添加邮件正文
message = '测试使用json发送邮件'
msg.attach(MIMEText(message, 'plain'))

# 添加附件（可选）
file_path = 'path_to_your_attachment.pdf'  # 替换为要附加的文件的实际路径
if os.path.exists(file_path):
    with open(file_path, 'rb') as attachment:
        part = MIMEApplication(attachment.read(), Name=os.path.basename(file_path))
        part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
        msg.attach(part)

# 发送邮件
smtp_server.sendmail(email_address, to_email_address, msg.as_string())

print("发送成功")
# 关闭SMTP客户端
smtp_server.quit()