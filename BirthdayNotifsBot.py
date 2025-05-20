# line-length = 120

import logging
from calendar import monthrange, month_name as calendar_month_names 
from collections import defaultdict, OrderedDict
from datetime import datetime, timedelta
from json import JSONDecodeError, load 

from pandas import DataFrame, read_excel, to_datetime
from telegram import Bot

from azure.cosmos import CosmosClient, ContainerProxy
from azure.cosmos.exceptions import CosmosResourceExistsError, CosmosResourceNotFoundError
from azure.identity import DefaultAzureCredential


class BirthdayNotifsBot:
    """Checks an Azure Cosmos DB for upcoming birthdays for a given week, and sends notifications via Telegram bot"""
    def __init__(self):
        self.credential = DefaultAzureCredential()
        self.container: None | ContainerProxy = None     
           
    def cosmos_login(self, container_name: str, db_name: str, endpoint: str) -> None:
        """
        Connects to your Azure Cosmos DB account via `DefaultAzureCredential` and populates the `container` attribute 
        
        Parameters:
            `container_name (str): The name of your Cosmos DB container
            `db_name` (str): The name of your Cosmos DB database 
            `endpoint` (str): The URI of your Azure Cosmos DB account            
        """    
        try:
            client = CosmosClient(url=endpoint, credential=self.credential)
            database = client.get_database_client(db_name)
            self.container = database.get_container_client(container_name)
            self.container.read()  # will raise if credentials fail
            logging.info("Successfully logged in to your Azure Cosmos DB account!")        
                                    
        except CosmosResourceNotFoundError as c:
            logging.critical(f"Ensure your container/db names are correct and that the container exists: {str(c)}")
            raise
        
        except Exception as e:  # generic az errors are descriptive enough - leaving this as a catch-all 
            logging.critical(f"Error logging into your Azure Cosmos DB account: {str(e)}")
            raise

    def get_upcoming_birthdays(
        self, 
        date: None | str = None, 
        json_month_key: str = 'month_name', 
        json_day_key: str = 'days'
    ) -> str:
        """
        Returns all upcoming birthdays for the week of the `date` parameter passed (Mon-Sun) 
        
        Parameters:
            `date` (str): Defaults to the current date if omitted (format='MM-DD-YYYY') 
            `json_month_key` (str): The name of the Cosmos DB month key (default="month_name")
            `json_day_key` (str): The name of the Cosmos DB day key containing the day-name pairs (default='days')  
            
        Returns:
            `str`: The markdown-formatted text of upcoming birthdays           
        """
        # validation steps
        self.__check_for_container_instance()   
        
        for key in [json_day_key, json_month_key]:
            self.__validate_str_param(str_input=key)
        
        # get week range (date input is also validated in here)
        week_start_date, week_end_date = self._get_week_range(date=date)

        # access Cosmos DB starting month container
        start_month = week_start_date.strftime("%B")
        try:
            main_container = self.container.read_item(item=start_month, partition_key=start_month)  
            self.__validate_month_container_schema(main_container, json_month_key, json_day_key)      
            containers_to_iterate = {start_month: main_container}
        except CosmosResourceNotFoundError:
            logging.critical(f"Could not locate starting month container '{start_month}' in your Cosmos DB")
            raise  # letting any other errors raise as-is, this is the most crucial one
                           
        # open second container IF the week range spans into the neighboring month
        if week_start_date.month != week_end_date.month:
            end_month = week_end_date.strftime("%B")
            try:
                second_container = self.container.read_item(item=end_month, partition_key=end_month)
                self.__validate_month_container_schema(second_container, json_month_key, json_day_key)  
                containers_to_iterate[end_month] = second_container
            except CosmosResourceNotFoundError:
                logging.critical(f"Could not locate ending month container '{end_month}' in your Cosmos DB")
                raise
                
        # locate the 7 days of the weekly range in the container(s) and pick out any upcoming birthdays
        upcoming_birthdays = 0
        final_text_output = []
        current_date = week_start_date
        while current_date <= week_end_date:  
                      
            current_month_name = current_date.strftime("%B")            
            current_day = str(current_date.day)
            container = containers_to_iterate[current_month_name]                            
            days_and_names_dict = container[json_day_key]  # contains date-name pairs (e.g. {'23: ['sam']})
            names = days_and_names_dict.get(current_day, [])

            # only get days where there is at least 1 birthday
            if names:
                current_date_formatted = current_date.strftime("%B %d")
                current_weekday_name = current_date.strftime("%A")
                _date = f"\n🎂 *{current_date_formatted} ({current_weekday_name})* 🎂"
                final_text_output.append(_date)
                                
                for name in names:
                    final_text_output.append(f"🔴 _{name.title()}_")
                    upcoming_birthdays += 1
            
            current_date += timedelta(days=1)

        # assemble final text output
        header = f"❗❗❗ *There are {upcoming_birthdays} birthdays coming up this week* ❗❗❗"                   
        return header + '\n' + '\n'.join(final_text_output)

    @staticmethod  # placing this method here, as it is integral to the intended 'flow' of the class
    async def send_message(bot_token: str, chat_id: str, text: str, parse_mode: str ="Markdown") -> None:
        """Sends a text message to a Telegram bot 
        
        Parameters:
            `bot_token` (str): The access token for your Telegram bot
            `chat_id` (str): The chat ID for your Telegram bot
            `text` (str): The text to post to the Telegram bot        
            `parse_mode` (str): Format of the text (default="Markdown")
        """
        async with Bot(token=bot_token) as bot:
            await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode)  

    # ____________________ container set-up and records uploading/updating: ____________________

    def upload_skeleton(
        self, 
        file_path: str | None = None, 
        json_month_key: str = "month_name", 
        json_day_key: str = "days"
        ) -> None:
        """
        Uploads the JSON yearly calendar schema to your Azure Cosmos DB account - overwrites any existing contents
        
        Parameters:
            `file_path` (str): The path to the JSON file - if None, will generate one automatically (default=None)
            `json_month_key` (str): The name of the schema month key (default="month_name")
            `json_day_key` (str): The name of the key containing day-name pairs (default='keys')  
        """
        self.__check_for_container_instance()

        # read-in the skeleton file - if path not provided then just generate a skeleton from scratch         
        if file_path:
            document = self._load_json_skeleton_file(file_path=file_path)
        else:
            document = self._generate_json_skeleton(json_month_key=json_month_key, json_day_key=json_day_key)            
        
        # upload in monthly container chunks (raise if even one container has an issue - going for integrity here)
        try:
            for i, month_chunk in enumerate(document, start=1):
                logging.debug(f"Now processing chunk #{i}")
                self.__validate_month_container_schema(month_chunk, json_month_key, json_day_key)                        
                month_chunk['id'] = month_chunk[json_month_key].title()  # required by cosmos
                month_chunk[json_month_key] = month_chunk[json_month_key].title()  # normalize inputs
                self.container.upsert_item(month_chunk)
                      
        except CosmosResourceExistsError as c:
            logging.critical(f"You may be trying to overwrite the schema without first reseting the 'id' key: {str(c)}")
            raise

        except Exception as e:
            logging.critical(f"Could not upload your JSON document: {str(e)}")
            raise
            
        logging.info("Successfully uploaded your skeleton to your Azure Cosmos DB account")

    def bulk_update_records(self, file_path: str,
        name_col: str = "Name",
        date_col: str = "Date",
        upd_col: str = "Update",
        json_month_key: str = "month_name", 
        json_day_key: str = "days",
    ) -> None:
        """
        Upload birthday records to the Azure Cosmos DB using an .xlsx file (cols=[name_col, date_col, upd_col])
        
        Parameters:
            `file_path` (str): The path to the .xlsx file containing bulk records  
            `name_col` (str): The name of the column in `df` containing the `name` entries (default="Name")
            `date_col` (str): The name of the column in `df` containing the `date` entries (default="Date")
            `upd_col` (str): The name of the column in `df` containing the `update` entries (default="Update")
            `json_month_key` (str): The name of the month key (default="month_name")
            `json_day_key` (str): The name of the key containing day-name pairs (default='keys'),
        """    
        # validation steps
        self.__check_for_container_instance()

        for param in [name_col, date_col, upd_col, json_month_key, json_day_key]:
            self.__validate_str_param(str_input=param)
    
        # load in df and validate data
        df = self._pre_process_bulk_data(file_path=file_path, name_col=name_col, date_col=date_col, upd_col=upd_col)

        # store .xlsx entries in a dict by month - will upsert in bulk to save on RU        
        records_by_month = defaultdict(lambda: defaultdict(lambda: {"add": [], "delete": []}))
        for _, row in df.iterrows():
            month_name = row['month name']  # hard coding these since they come from _pre_process_bulk_data()
            day = str(row['day']) 
            action = row[upd_col]
            name = row[name_col] 
            records_by_month[month_name][day][action].append(name)

        # iterate container(s) and add/delete records 
        failures = []
        for month, days in records_by_month.items():
            # open container
            try:
                logging.debug(f"Now processing container '{month}'")
                month_container = self.container.read_item(item=month, partition_key=month)
                self.__validate_month_container_schema(month_container, json_month_key, json_day_key)
            except Exception as e:
                logging.error(f"Could not read container '{month}': {e}")
                failures.append(month)    
                continue
            # iterate through day records and perform updates
            for day, records in days.items():
                    for update_type, names in records.items():
                        
                        if update_type == 'add':
                            month_container[json_day_key][day].extend(names)
                        
                        elif update_type == 'delete':
                            for name in names:
                                if name in month_container[json_day_key][day]:
                                    month_container[json_day_key][day].remove(name)
                                else:
                                    logging.warning(f"Record '{name}' does not exist for '{month} {day}'")
                                    continue
            # publish in monthly chunks 
            try:
                self.container.upsert_item(month_container)
            except Exception as e:
                logging.error(f"Could not update container with records for '{month}': {str(e)}")
                failures.append(month)
                continue
        
        # success/failure logs 
        if failures:
            logging.warning(f"The following container(s) could not be read and were skipped: {', '.join(failures)}")
        
        logging.info("Completed run of updating .xlsx records in your Cosmos DB skeleton")

    def update_record(
        self, 
        month_name: str, 
        day: str | int, 
        name: str, 
        action: str = 'add', 
        json_month_key: str = 'month_name',
        json_day_key: str = 'days'
    ) -> None:
        """
        Adds/removes name records from a Cosmos DB monthly container
        
        Parameters:
            `name` (str): The 'name' record to be updated (e.g. 'Sam')
            `month_name` (str): The full month name of the birthday (e.g. 'January')
            `day` (str | int): The day of their birthday (e.g. 20)
            `action` (str): The update type ('add' or 'delete')
            `json_month_key` (str): The name of the month key (default="month_name")
            `json_day_key` (str): The name of the key containing day-name pairs (default='keys')        
        """        
        # logic validation (heavy gating here to maintain quality/integrity of the the DB)
        self.__check_for_container_instance()

        for param in [json_day_key, json_month_key, name]:
            self.__validate_str_param(str_input=param)

        month_name = self.__validate_month_parameter(month=month_name)
        day = self.__validate_day_parameter(day=day)
        action = self.__validate_action_parameter(action=action)
                
        if month_name == 'February' and day == '29':
            raise ValueError('Schema does not support February 29 - please defer to February 28 or March 1 instead')
        
        month_number = str(datetime.strptime(month_name, '%B').month)
        dummy_year = '2023'  # any arbitrary non-leap year        
        full_date = f"{month_number}-{int(day)}-{dummy_year}"
        self.__validate_date_input(date=full_date)

        # upsert record to container
        try:                         
            container = self.container.read_item(item=month_name, partition_key=month_name)
            self.__validate_month_container_schema(container, json_month_key, json_day_key)
        
            if action == 'add':
                container[json_day_key][day].append(name.title())
            else:
                container[json_day_key][day].remove(name.title())
            
            self.container.upsert_item(container)
            logging.info(f"Successfully {action}ed record '{name}' for '{month_name} {day}'")
        
        except KeyError:
            logging.critical(f"Key name '{json_day_key}' does not exist in the container schema for '{month_name}'")
            raise

        except Exception as e:
            logging.critical(f"Failed to {action} record '{name}' for '{month_name} {day}': {str(e)}") 
            raise      

    # ____________________ validator methods and misc under-the-hood private stuff: ____________________
    
    def _get_week_range(self, date: None | str = None) -> tuple[datetime, datetime]:
        """Returns a tuple with the start and end dates for the week of the current date (Mon-Sun)"""
        date = self.__validate_date_input(date=date)        
        current_weekday = timedelta(days=date.weekday())
        week_start_date = date - current_weekday
        week_end_date = week_start_date + timedelta(days=6)
        return week_start_date, week_end_date
   
    def _pre_process_bulk_data(
        self, 
        file_path: str, 
        name_col: str = "Name", 
        date_col: str = "Date", 
        upd_col: str = "Update"
    ) -> DataFrame:
        """
        Validates and cleans the .xlsx data containing your birthday records (cols=['Name', 'Date', 'Update'])
        
        Parameters:
            `file_path` (str): The path to your .xlsx file containing the birthday records
            `name_col` (str): The name of the 'name' column in your .xlsx file (default="Name")
            `date_col` (str): The name of the 'date' column in your .xlsx file (default="Date")
            `upd_col` (str): The name of the 'update' column in your .xlsx file (default="Update")
        
        Returns:
            (DataFrame): Cleaned df containing columns=['name', 'update', 'month name', 'day']
        """
        if not file_path.endswith('.xlsx'):
            raise TypeError("Birthday records must be .xlsx files - other formats not supported at this time")

        df = read_excel(file_path, usecols=[name_col, date_col, upd_col])  # will raise if cols dont match params

        for col in [name_col, date_col, upd_col]:
            if df[col].isna().any():
                raise ValueError(f"Error parsing '{col}' column - ensure that there are no empty cells, or formulas")

        df[name_col] = df[name_col].astype(str).str.strip().str.title()

        df[upd_col] = df[upd_col].astype(str).str.strip().str.lower()

        if not df[upd_col].isin(['add', 'delete']).all():
            raise ValueError(f"Error parsing '{upd_col}' column - ensure that all cells are either 'add' or 'delete'")        

        try:
            df[date_col] = to_datetime(df[date_col], errors='raise')  # performs date validation 
        except Exception as e:
            logging.critical(
                f"Error parsing '{date_col}' column - ensure that all cells are in a proper xlsx-supported date format"
                )
            raise

        df['month name'] = df[date_col].dt.month_name()
        df['day'] = df[date_col].dt.day
        df.drop(columns=[date_col], inplace=True)

        if ((df['month name'] == 'February') & (df['day'] == 29)).any():
            raise ValueError("Leap years are not supported. Please defer to February 28 or March 1 instead")
                                    
        return df

    def __check_for_container_instance(self) -> None:
        """Raises RuntimeError if `container` attribute is not instantiated"""
        if self.container is None:
            raise RuntimeError("No active container detected - must run cosmos_login() first")
                                            
    def __validate_month_container_schema(self, container: dict, json_month_key: str, json_day_key: str) -> None:
        """Ensures that the JSON container being accessed contains the expected month-based schema"""
        # confirm that the json key-names exist 
        for key in [json_month_key, json_day_key]:
            if key not in container.keys():
                raise ValueError(f"Could not locate key '{key}' in the JSON container")
        
        # confirm that full, correct month names are used
        month_name = container[json_month_key]
        try:            
            self.__validate_month_parameter(month=month_name) 
        except Exception as e:
            logging.critical(f"Error with key '{month_name}' in the JSON container: {str(e)}")
            raise
        
        # confirm that json_day_key is a dict with struct {str: list[str]} 
        days_dict = container[json_day_key]
        if not isinstance(days_dict, dict):
            raise TypeError(f"'{json_day_key}' must be a dict, got {type(days_dict).__name__}")
        
        for day_num, names in days_dict.items():
            # confirm that the day keys are valid
            self.__validate_day_parameter(day=day_num) 

            # confirm that each value in the dict is a list
            if not isinstance(names, list):
                raise TypeError(f"Values in '{json_day_key}' dict must be of type list, got {type(names).__name__}")
            
            # confirm that all list contents are str 
            for name in names:
                if not isinstance(name, str):
                    raise TypeError(
                        f"The inner 'names' list must contain only str values, got {type(name).__name__}")

        # confirm there are no missing/extra days for the given month
        month_number = datetime.strptime(month_name, '%B').month  
        arbitrary_year = 2025  # skeleton does not have February 29, so any non-leap-year will do
        days_in_month = monthrange(year=arbitrary_year, month=month_number)[1]

        if days_in_month != len(days_dict):        
            located_days = set(int(day) for day in days_dict.keys())
            actual_days = set(range(1, days_in_month+1))            
            extra_days = located_days - actual_days
            missing_days = actual_days - located_days
            
            if month_number == 2 and 29 in extra_days:
                raise ValueError("'February' container should NOT contain '29' - please update the schema") 
            
            if missing_days:
                raise ValueError(f"Found missing days: {missing_days}, in your JSON schema for month '{month_name}'")

            if extra_days:
                raise ValueError(f"Found extra days: {extra_days}, in your JSON schema for month '{month_name}'")

    @staticmethod
    def _generate_json_skeleton(json_month_key: str = 'month_name', json_day_key: str = 'days') -> list:
        """Generates a full Cosmos DB calendar skeleton (non-leap-year) and returns it as a list of OrderedDict"""
        master_list = []
        for month_number in range(1,13):
            month_dict = OrderedDict()
            month_dict[json_month_key] = calendar_month_names[month_number]
            
            n_days_current_month = monthrange(2025, month_number)[1]  # using 2025 as arbitrary non-leap year
            days = range(1, n_days_current_month+1)
            month_dict[json_day_key] = OrderedDict()
            for day in days:
                month_dict[json_day_key][str(day)] = list()

            master_list.append(month_dict)
            
        return master_list

    @staticmethod
    def _load_json_skeleton_file(file_path: str) -> list:
        """Reads in the `skeleton.json` calendar document from `file_path` and validates its basic structure"""
        try:            
            if not file_path.lower().endswith('.json'):
                raise ValueError("Must pass a valid JSON file")

            with open(file_path, "r", encoding='utf-8') as f:
                document = load(f)
                                            
                if not isinstance(document, list):
                    raise TypeError(f"Expected document of type list, got {type(document).__name__}")

                if len(document) != 12:
                    raise ValueError(f"Expected 12 months in JSON document, got {len(document)}")

        except JSONDecodeError as j:
            logging.critical(f"Your JSON schema is corrupt - please use 'skeleton.json' from 'setup_files': {str(j)}")
            raise
        
        except Exception as e:
            logging.critical(f"Could not read your json document from path: {str(e)}")   
            raise

        return document
    
    @staticmethod
    def __validate_date_input(date: None | str = None) -> datetime:
        """Raises if `date` parameter is not of str format 'MM-DD-YYYY' - returns current date if None"""               
        if not isinstance(date, str) and date is not None:
            raise TypeError(f"Parameter 'date' must be of str or None type, got {type(date).__name__} for '{date}'")

        if date:            
            return datetime.strptime(date.strip(), '%m-%d-%Y').date()  # dt lib has descriptive enough tracebacks
                
        return datetime.now().date()                  

    @staticmethod
    def __validate_action_parameter(action: str) -> str:
        """Raises ValueError if `action` parameter is not 'add' or 'delete, and TypeError if not a str"""
        if not isinstance(action, str):
            raise TypeError(f"Parameter 'action' must be of str type, got {type(action).__name__} for '{action}'")           
        
        action = action.lower().strip()
        
        if action not in ('add', 'delete'):
            raise ValueError("Parameter 'action' only accepts inputs 'add' or 'delete'")

        return action

    @staticmethod
    def __validate_month_parameter(month: str) -> str:
        """Raises ValueError if `month` parameter is not a full month name, and TypeError if not of type str"""
        if not isinstance(month, str):
            raise TypeError(f"Parameter 'month' must be of str type, got {type(month).__name__} for '{month}'")        
        
        month = month.title().strip()

        all_month_names = [calendar_month_names[i].title() for i in range(1,13)]

        if month not in all_month_names:
            raise ValueError(f"Expecting a valid, full month name, got '{month}'")

        return month

    @staticmethod
    def __validate_day_parameter(day: str | int) -> str:
        """Raises TypeError if `day` parameter is not either str or int, and ValueError if not a digit between 1-31"""
        if not isinstance(day, (str, int)):
            raise TypeError(f"Parameter 'day' must be of str or int type, got {type(day).__name__}")

        day = str(day).strip()

        if isinstance(day, str):
            if not day.isdigit():
                raise ValueError(f"Day '{day}' is invalid - expected a digit between 1-31")

        if not 1 <= int(day) <= 31:
            raise ValueError(f"Parameter 'day' should be a month day between 1 and 31, got '{day}'")                            

        return day

    @staticmethod
    def __validate_str_param(str_input: str) -> str:
        """Raises TypeError if `str_input` parameter is not of type str"""
        if not isinstance(str_input, str):
            raise TypeError(f"Parameter '{str_input}' must be of str type, got {type(str_input).__name__}")
        return str_input.strip()
