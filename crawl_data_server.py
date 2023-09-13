from time import sleep

from selenium.webdriver.chrome.options import Options
from crawl_utils import crawl_diegogo_project_data
from crawl_utils import crawl_crowdfunder_project_data
from dotenv import load_dotenv
import os
import threading
import json
load_dotenv()
url = 'https://www.kickstarter.com/discover/advanced?woe_id=0&sort=magic&seed=2811224&page='

# get the current page
with open("./data/indiegogo_checkpoint.json", "r") as jf:
    indiegogo_cur_page = json.load(jf)["page"]
with open("./data/crowdfunder_checkpoint.json", "r") as jf:
    crowdfunder_cur_page = json.load(jf)["page"]
chrome_options = Options()
chrome_options.add_experimental_option('detach', True)
kafka_broker = os.environ.get("kafka_broker")
kafka_broker = "PhuongLy:9092"
topic = "project"

kafka_server = [kafka_broker, topic]
#kafka_server = []
i_thread = threading.Thread(target=crawl_diegogo_project_data, args=(
    indiegogo_cur_page, kafka_server, 5))
c_thread = threading.Thread(target=crawl_crowdfunder_project_data, args=(
    crowdfunder_cur_page, kafka_server))
#i_thread.start()
c_thread.start()
#i_thread.join()
c_thread.join()
sleep(60)