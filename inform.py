# coding=utf-8
"""
    @Author：零 若
    @file： inform.py
    @date：2025/10/30 21:28
    @Python  : 3.10.18
    别放弃，即使前方荆棘成林！
"""

import logging
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, Any

import requests


class NotificationService:
    """通知服务基类"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def send(self, title: str, message: str) -> bool:
        """发送通知"""
        raise NotImplementedError


class EmailNotification(NotificationService):
    """邮箱通知服务"""

    def send(self, title: str, message: str) -> bool:
        """
        发送邮件通知
        
        Args:
            title: 邮件标题
            message: 邮件内容
            
        Returns:
            是否发送成功
        """
        try:
            smtp_server = self.config.get('smtp_server', 'smtp.qq.com')
            smtp_port = int(self.config.get('smtp_port', 465))
            sender_email = self.config.get('sender_email', '')
            sender_password = self.config.get('sender_password', '')
            receiver_email = self.config.get('receiver_email', '')

            if not all([smtp_server, sender_email, sender_password, receiver_email]):
                self.logger.error("邮箱配置不完整")
                return False

            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = Header(f"班车抢票系统 <{sender_email}>")
            msg['To'] = Header(receiver_email)
            msg['Subject'] = Header(title, 'utf-8')

            # 添加HTML内容
            html_content = f"""
            <html>
                <body style="font-family: Arial, sans-serif;">
                    <div style="background-color: #f0f0f0; padding: 20px;">
                        <div style="background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                            <h2 style="color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px;">
                                {title}
                            </h2>
                            <div style="margin-top: 20px; line-height: 1.6; color: #555;">
                                {message.replace(chr(10), '<br>')}
                            </div>
                            <hr style="margin: 20px 0; border: none; border-top: 1px solid #eee;">
                            <p style="color: #999; font-size: 12px; margin-top: 20px;">
                                此邮件由班车抢票系统自动发送，请勿回复。
                            </p>
                        </div>
                    </div>
                </body>
            </html>
            """
            msg.attach(MIMEText(html_content, 'html', 'utf-8'))

            # 发送邮件
            if smtp_port == 465:
                # SSL连接
                server = smtplib.SMTP_SSL(smtp_server, smtp_port)
            else:
                # TLS连接
                server = smtplib.SMTP(smtp_server, smtp_port)
                server.starttls()

            server.login(sender_email, sender_password)
            server.sendmail(sender_email, [receiver_email], msg.as_string())
            server.quit()

            return True

        except Exception as e:
            self.logger.error(f"邮件发送失败: {str(e)}")
            return False


class WeChatWorkNotification(NotificationService):
    """企业微信通知服务"""

    def send(self, title: str, message: str) -> bool:
        """
        发送企业微信通知
        
        Args:
            title: 通知标题
            message: 通知内容
            
        Returns:
            是否发送成功
        """
        try:
            webhook_url = self.config.get('webhook_url', '')

            if not webhook_url:
                self.logger.error("企业微信 Webhook URL 未配置")
                return False

            # 构建消息内容
            content = f"**{title}**\n\n{message}"

            # 发送请求
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "content": content
                }
            }

            response = requests.post(webhook_url, json=data, timeout=10)
            result = response.json()

            if result.get('errcode') == 0:
                return True
            else:
                self.logger.error(f"企业微信通知发送失败: {result.get('errmsg')}")
                return False

        except Exception as e:
            self.logger.error(f"企业微信通知发送失败: {str(e)}")
            return False


class DingTalkNotification(NotificationService):
    """钉钉自定义机器人通知（关键词安全模式）"""

    def send(self, title: str, message: str) -> bool:
        try:
            webhook_url = str(self.config.get('webhook_url', '')).strip()
            keyword = str(self.config.get('keyword', '')).strip()

            if not webhook_url:
                self.logger.error("钉钉 Webhook URL 未配置")
                return False
            if not keyword:
                self.logger.error("钉钉机器人关键词未配置")
                return False

            # 钉钉 Markdown 的硬换行要求在换行前加两个空格。
            markdown_message = message.replace('\r\n', '\n').replace('\r', '\n')
            markdown_message = markdown_message.replace('\n', '  \n')

            # 关键词同时放入标题和正文，满足钉钉机器人的关键词校验。
            content = f"**{keyword}**\n\n**{title}**\n\n{markdown_message}"
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "title": f"{keyword} {title}",
                    "text": content,
                },
            }

            response = requests.post(webhook_url, json=data, timeout=10)
            response.raise_for_status()
            result = response.json()

            if result.get('errcode') == 0:
                return True

            self.logger.error(f"钉钉通知发送失败: {result.get('errmsg')}")
            return False
        except Exception as e:
            self.logger.error(f"钉钉通知发送失败: {str(e)}")
            return False


class NotificationManager:
    """通知管理器"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化通知管理器
        
        Args:
            config: 配置字典，包含通知方式和各通知服务的配置
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        self.services = []

        # 根据配置启用通知服务
        enabled_methods = config.get('notification_methods', [])

        if 'email' in enabled_methods:
            email_config = config.get('email_config', {})
            if email_config:
                self.services.append(EmailNotification(email_config))
                self.logger.debug("邮箱通知已启用")

        if 'wechat_work' in enabled_methods:
            wechat_config = config.get('wechat_config', {})
            if wechat_config:
                self.services.append(WeChatWorkNotification(wechat_config))
                self.logger.debug("企业微信通知已启用")

        if 'dingtalk' in enabled_methods:
            dingtalk_config = config.get('dingtalk_config', {})
            if dingtalk_config:
                self.services.append(DingTalkNotification(dingtalk_config))
                self.logger.debug("钉钉机器人通知已启用")

        if not self.services:
            self.logger.debug("未启用任何通知服务")

    def send_notification(self, title: str, message: str):
        """
        发送通知到所有已启用的服务
        
        Args:
            title: 通知标题
            message: 通知内容
        """
        if not self.services:
            self.logger.info(
                'No notification service enabled; notification printed locally (title=%s)',
                title,
            )
            print(f"\n{'=' * 60}")
            print(f"📢 【{title}】")
            print(f"{message}")
            print(f"{'=' * 60}\n")
            return

        for service in self.services:
            service_name = service.__class__.__name__
            try:
                sent = service.send(title, message)
                if sent:
                    self.logger.info(
                        'Notification sent (service=%s, title=%s)', service_name, title
                    )
                else:
                    self.logger.warning(
                        'Notification not sent (service=%s, title=%s)', service_name, title
                    )
            except Exception as e:
                self.logger.exception(
                    'Notification failed with an exception (service=%s, title=%s): %s',
                    service_name,
                    title,
                    e,
                )
