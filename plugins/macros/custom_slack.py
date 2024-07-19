from abc import ABC

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base import BaseHook
from airflow.providers.sktvane.macros.vault import get_secrets


class SlackBot:

    def __init__(self, conn_id, username):
        self.conn_id = conn_id
        self.username = username
        self.token = BaseHook.get_connection(self.conn_id).password
        self.proxy = get_secrets(path="proxy")["proxy"]

    def post_message(self, context, message):
        slack_alert = SlackWebhookOperator(
            task_id="slack_alert",
            slack_webhook_conn_id=self.conn_id,
            message=message,
            username=self.username,
            proxy=self.proxy,
        )

        return slack_alert.execute(context=context)

    def __format_message(self, context, **kwargs):
        local_dt = context.get("execution_date").astimezone()
        options = {"icon": ":large_blue_circle:", "title": "[not provided]"}
        options.update(kwargs)
        slack_msg = """
            ==================================================-
            {icon} *{title}*
            ==================================================
            *Task*: {task}
            *Dag*: {dag}
            *Execution Date:*: {execution_date}
            *Log Url*: {log_url}
        """.format(
            icon=options["icon"],
            title=options["title"],
            task=context.get("task_instance").task_id,
            dag=context.get("task_instance").dag_id,
            execution_date=local_dt.strftime("%Y%m%d %H:00:00"),
            log_url=context.get("task_instance").log_url,
        )
        return slack_msg

    def post_alert(self, context, **kwargs):
        kwargs["icon"] = ":red_circle:"
        message = self.__format_message(context, **kwargs)
        return self.post_message(context, message)

    def post_info(self, context, **kwargs):
        kwargs["icon"] = ":large_green_circle:"
        message = self.__format_message(context, **kwargs)
        return self.post_message(context, message)

    def post_warning(self, context, **kwargs):
        kwargs["icon"] = ":large_orange_circle:"
        message = self.__format_message(context, **kwargs)
        return self.post_message(context, message)


class CallbackNotifier:

    SLACK_CONN_ID = "aipa/slack_conn_jh"
    USERNAME = "airflow"
    SELECTED_TASK_IDS = ["start", "end"]

    @classmethod
    def on_failure_callback(cls, context):
        task_id = context["task_instance"].task_id
        if task_id in cls.SELECTED_TASK_IDS:
            bot = SlackBot(cls.SLACK_CONN_ID, cls.USERNAME)
            bot.post_alert(context, title="Failed Task Alert")

    @classmethod
    def on_success_callback(cls, context):
        task_id = context["task_instance"].task_id
        if task_id in cls.SELECTED_TASK_IDS:
            bot = SlackBot(cls.SLACK_CONN_ID, cls.USERNAME)
            bot.post_info(context, title="Success Task Info")

    @classmethod
    def on_retry_callback(cls, context):
        task_id = context["task_instance"].task_id
        if task_id in cls.SELECTED_TASK_IDS:
            bot = SlackBot(cls.SLACK_CONN_ID, cls.USERNAME)
            bot.post_warning(context, title="Retry Task Alert")
