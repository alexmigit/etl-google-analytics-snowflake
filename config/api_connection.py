import os
from dotenv import load_dotenv   

load_dotenv()

def get_slack_webhook_url():
    return os.getenv('SLACK_WEBHOOK_URL')
