# line-length = 120

from asyncio import run
from os import path

from BirthdayNotifsBot import BirthdayNotifsBot


# setup-file paths
skeleton_path = path.join('setup_files', 'skeleton.json')
xlsx_path = path.join('setup_files', 'birthdays.xlsx')

# required cosmos credentials 
container_name = '<your container name>'
db_name = "<your db name>"
endpoint = "<your Cosmos DB URI>"

# required tg bot credentials
bot_token = '<your Telegram bot token>'
chat_id = '<your Telegram chat ID>'

# schema key map
json_month_key = 'month_name'
json_day_key = 'days'

# instantiate class    
bday_client = BirthdayNotifsBot()

# log in to your azure account
bday_client.cosmos_login(container_name=container_name, db_name=db_name , endpoint=endpoint)

# create and upload skeleton from scratch ...
bday_client.upload_skeleton() 

# ... or upload it from 'skeleton.json' (alternative method)
# bday_client.upload_skeleton(file_path=skeleton_path, json_month_key=json_month_key, json_day_key=json_day_key)

# update records in bulk from an Excel file (col names are case-sensitive)
bday_client.bulk_update_records(
    file_path=xlsx_path, 
    name_col='name', 
    date_col='date',
    upd_col='update', 
    json_month_key=json_month_key, 
    json_day_key=json_day_key
    )        

# update single records (add or delete)
bday_client.update_record(
    month_name='June', 
    day=27, 
    name='King Louis XII', 
    action='add', 
    json_day_key=json_day_key, 
    json_month_key=json_month_key
    )

# after setting up, you can now get upcoming birthdays (omit date param to use current date)
output_text = bday_client.get_upcoming_birthdays(date=None, json_month_key=json_month_key, json_day_key=json_day_key)

# send message to the tg bot (must run with asyncio)
run(
    bday_client.send_message(
    bot_token=bot_token,
    chat_id=chat_id, 
    text=output_text,
    parse_mode='Markdown'
    )
)

