# line-length = 120

import logging
import os
import time

from asyncio import run
from random import uniform

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from BirthdayNotifsBot import BirthdayNotifsBot


# db env vars
cosmos_endpoint = os.getenv("endpoint")
container_name = os.getenv("container_name")
db_name = os.getenv("db_name")
# tg bot keys
key_vault_name = os.getenv("kv_name")
kv_url = f"https://{key_vault_name}.vault.azure.net"
# schema keys
schema_month_key = "month_name"
schema_day_key = "days"


app = func.FunctionApp()
@app.timer_trigger(schedule="0 0 6 * * 1", arg_name="MondayMorningRun", run_on_startup=False, use_monitor=False) 
def orchestrator_with_retries(MondayMorningRun: func.TimerRequest) -> None:
    """This app runs every Monday at 6:00 AM UTC and sends a message with upcoming birthdays to a Telegram bot"""
    retry_attempts = 3
    current_attempt = 1
    while current_attempt <= retry_attempts:
        try:
           run_upcoming_birthdays()
           logging.info(f"Completed upcoming-birthdays run on attempt #{current_attempt}!")
           return None

        except Exception as e:
            logging.error(f"Failed upcoming-birthdays run on attempt #{current_attempt}: {str(e)}")
            exp_backoff = (2*current_attempt)+uniform(0,1)
            time.sleep(exp_backoff)
            current_attempt += 1
            
            if current_attempt >= retry_attempts:
                logging.critical(f"Failed upcoming-birthdays run after max retries: {str(e)}")
                raise
          
                
def run_upcoming_birthdays() -> None:
    """Logs in to Azure/Cosmos account, checks for upcoming birthdays, and sends str output to the Telegram bot"""
    # instantiate client
    birthday_client = BirthdayNotifsBot()

    # log-in to Azure/Cosmos account
    birthday_client.cosmos_login(
        container_name=container_name, 
        db_name=db_name, 
        endpoint=cosmos_endpoint
    )

    # get birthdays for the week
    output = birthday_client.get_upcoming_birthdays(json_month_key=schema_month_key, json_day_key=schema_day_key)
 
    # get access keys from vault for tg bot
    try:
        key_vault_client = SecretClient(vault_url=kv_url, credential=DefaultAzureCredential())
        chat_id = key_vault_client.get_secret('chat-id').value
        tg_bot_token = key_vault_client.get_secret('tg-bot-token').value
    except Exception as e:
        logging.critical(f"Failed fetching secrets from Key Vault: {e}")
        raise

    # send output to tg bot      
    post_attempts = 1
    max_attempts = 3
    while post_attempts <= max_attempts:    
        try:
            run(birthday_client.send_message(bot_token=tg_bot_token, chat_id=chat_id, text=output))
            # log the successful run
            week_start_date, week_end_date = birthday_client._get_week_range()
            start_date = week_start_date.strftime("%m-%d-%Y")
            end_date = week_end_date.strftime("%m-%d-%Y")
            logging.info(f"Successfully posted upcoming birthdays for the week of {start_date} through {end_date}!")
            return None

        except Exception as e:
            logging.error(f"Failed to post output to the Telegram bot: {e}")
            exp_backoff = (2*post_attempts)+uniform(0,1) 
            time.sleep(exp_backoff)
            post_attempts += 1
            
            if post_attempts >= max_attempts:
                logging.critical(f"Failed to send HTTP post after {post_attempts} attempts")
                raise
            