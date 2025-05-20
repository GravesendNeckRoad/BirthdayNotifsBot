# <p align="center"> @BirthdayNotifsBot </p>


#### 🎂 :cake: I'm horrible at remembering dates so I wrote this script to notify me of upcoming birthdays! <br><br>



:clock6: :running: An Azure Function App runs every Monday (UTC) and checks a Cosmos DB for any birthdays for that week (Mon-Sun), then sends a notification via Telegram bot. <br><br>


:lock: :key: The orchestration utilizes a **Function App**, **Key Vault**, and **Cosmos DB**, all sharing a virtual network and managed identity permissions. Since it runs on a CRON trigger, there are no public endpoints . The credential type used is DefaultAzureCredential. <br><br>



:construction_worker: :hammer: Setup is very simple—built-in methods allow you to load and populate the schema in just a few lines of code. All you need is an Azure Cosmos DB account and an Excel file with the birthdays of your friends/relatives (see setup_demo.py) <br><br>


:moneybag: :money_with_wings: Runtime costs are minimal—the Function App uses a consumption plan with cold/archive storage. All uploads are processed in chunks to minimize RUs, and no server is required for the Telegram bot, as the notifications are simple markdown text.

![mockup2](https://github.com/user-attachments/assets/839791b9-9a32-4e6c-bc65-5c4e92f62fbb)
