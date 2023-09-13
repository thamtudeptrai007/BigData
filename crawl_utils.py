import threading
import json
import traceback
from time import sleep

import pytz
import requests
from kafka import KafkaProducer
from datetime import datetime
from forex_python.converter import CurrencyRates
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait

from n_kafka.consumer import save_to_json_file
from n_kafka.producer import ProjectProducer
from utils import set_up_browser, round_number, split_list


def get_time_now():
    current_time = datetime.now()
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time


def convert_currency(amount, base_currency, target_currency):
    # Create a CurrencyRates object
    c = CurrencyRates()

    # Get the exchange rate
    rate = c.get_rate(base_currency, target_currency)

    return amount * rate


# Đây là một đối tượng lock (khóa) của thread được sử dụng để đồng bộ hóa truy cập vào một phần dữ liệu chia sẻ.
# Trong trường hợp này, lock được sử dụng để đảm bảo rằng chỉ một thread có thể ghi dữ liệu vào tệp JSON cùng một lúc.
# Điều này giúp tránh việc ghi đè dữ liệu khi nhiều thread cùng ghi vào tệp.
mutex = threading.Lock()


def send_data(data=[], producer=[]):
    if len(producer) == 2:
        kafka_broker, topic = producer
        print("[*] Data from kickstarter sending to broker:",
              kafka_broker, ", topic:", topic)
        # print(f"data: {data}")
        projectProducer = ProjectProducer(broker=kafka_broker, topic=topic)
        for item in data:
            item["timestamp"] = get_time_now()
            projectProducer.send_msg(item)
    else:
        # save to mongodb
        # print("[*] Save data to mongodb.")
        mutex.acquire()
        try:
            file_path = "C:/Users/admin/Desktop/BigData/data/data.json"
            with open(file_path, 'a') as file:
                file.write('\n')
                json.dump(data[0], file)
                # save_to_json_file("./data/data.json", item)
        finally:
            mutex.release()


def convert_timestring_to_unix(time_string):
    # Hàm này tạo một đối tượng datetime từ time_string đã cho.
    # time_string phải có định dạng ISO 8601 ("2023-07-24T12:34:56").
    dt = datetime.fromisoformat(time_string)

    # define timezone
    timezone = pytz.timezone("America/Los_Angeles")

    # apply timezone for datetime
    localized_dt = timezone.localize(dt.replace(tzinfo=None))

    # convert datetime object to unix timestamp
    unix_time = int(localized_dt.timestamp())

    return unix_time


def get_and_format_indiegogo_project_data(input, producer=[], web_driver_wait=0):
    data = input

    # change category
    del data["category_url"]
    data["category"] = [{"name": data.pop("category")}]

    # change close date -> deadline
    data["deadline"] = convert_timestring_to_unix(data.pop("close_date"))

    # change open date -> launched_at
    data["launched_at"] = convert_timestring_to_unix(data.pop("open_date"))

    # change fund_raised_amount -> pledged
    data["pledged"] = data.pop("funds_raised_amount")

    # change funds_raised_percent -> percent_funded
    data["percent_funded"] = data.pop("funds_raised_percent")

    # change title -> name
    data["name"] = data.pop("title")

    # change tagline -> blurb
    data["blurb"] = data.pop("tagline")

    if data["pledged"] == 0 or data["percent_funded"] == 0:
        data["goal"] = 0
    else:
        data["goal"] = round_number(
            int(data["pledged"] / data["percent_funded"]))

    # open browser to get backers_count,location, goal
    browser = set_up_browser()
    try:
        browser.get("https://www.indiegogo.com" + data["clickthrough_url"])
        print(f"url: {'https: // www.indiegogo.com' + data['clickthrough_url']}")
        wait = WebDriverWait(browser, web_driver_wait)
        # browser.get_screenshot_as_file("screenshot.png")
        backer_elements = browser.find_elements(
            By.CSS_SELECTOR, "span.basicsGoalProgress-claimedOrBackers > span")
        # print(f"backers_elements: {backer_elements}")
        try:
            backers_count = backer_elements[-2].text
        except:
            backers_count = 0
        country_elements = browser.find_elements(
            By.CSS_SELECTOR, "div.basicsCampaignOwner-details-city"
        )
        try:
            country = country_elements[0].text.split(", ")[-1]
        except:
            country = ""
        data["backers_count"] = backers_count
        data["country"] = country
        data["timestamp"] = get_time_now()
        data["web"] = "indiegogo"
        lmao = {"id": data["project_id"], "name": data["name"], "category": data["category"],
                "deadline": data["deadline"],
                "launched_at": data["launched_at"],
                "pledged": convert_currency(data["pledged"], data["currency"], "USD"),
                "goal": convert_currency(data["goal"], data["currency"], "USD"), "country": data["country"],
                "backers_count": data["backers_count"],
                "timestamp": data["timestamp"], "web": data["web"]}
        send_data([lmao], producer=producer)
    except:
        traceback.print_exc()
        # change project_type -> ????

        # change tags to -> ?????
    return data


def crawl_diegogo_project_data(current_page, producer=[],
                               web_driver_wait=0,
                               num_of_thread=5,
                               url="https://www.indiegogo.com/private_api/discover",
                               err_page_file="./data/indiegogo_err_page.json",
                               check_point_file="./data/indiegogo_checkpoint.json",
                               sleep_per_crawling_time=0,
                               ):
    page = current_page
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    while (1):
        try:
            request_body = json.dumps({"sort": "trending", "category_main": None, "category_top_level": None,
                                       "project_timing": "all", "project_type": "campaign", "page_num": page,
                                       "per_page": 100, "q": "", "tags": []})
            res = requests.post(url, data=request_body, headers=headers)
            if res.status_code == 200:
                data = json.loads(res.text)["response"]["discoverables"]
                split_data = split_list(data, num_of_thread)
                for dts in split_data:
                    threads = []
                    for dt in dts:
                        threads.append(threading.Thread(
                            target=get_and_format_indiegogo_project_data, args=(dt, producer, web_driver_wait)))
                    for thr in threads:
                        thr.start()
                    for thr in threads:
                        thr.join()
            # save_to_json_file("./data/indiegogo_data.json", data)
            elif res.status_code == 400:
                page = -1
                sleep(sleep_per_crawling_time)
                # save_to_json_file(check_point_file, {"page": 0})
                # break
        except Exception as e:
            err_page = {
                "page": page,
                "err": str(e),
            }
            save_to_json_file(err_page_file, err_page)

            traceback.print_exc()
        page = page + 1
        save_to_json_file(check_point_file, {"page": page})


def format_crowdfunder_project_data(input):
    data = {
        "id": input["idproject"],
        "name": input["title"],
        "goal": input["amount"],
        "pledged": input["amount_pledged"],
        "percent_funded": input["percent_funded"],
        "country": input["country"],
        "launched_at": convert_timestring_to_unix(input["created_at"]),
        "deadline": convert_timestring_to_unix(input["closing_at"]),
        "backers_count": input["num_backers"],
        "category": [{"name": cate} for cate in input["category"]],
        "timestamp": get_time_now(),
        "web": "crowdfunder"
    }
    return data


def crawl_crowdfunder_project_data(current_page, producer=[],
                                   url="https://7izdzrqwm2-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.17.0)%3B%20Browser%20(lite)&x-algolia-api-key=9767ce6d672cff99e513892e0b798ae2&x-algolia-application-id=7IZDZRQWM2&paginationLimitedTo=2000",
                                   err_page_file="./data/crowdfunder_err_page.json",
                                   check_point_file="./data/crowdfunder_checkpoint.json",
                                   sleep_per_crawling_time=0
                                   ):
    page = current_page
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }
    while (1):
        for i in range(page, 50):
            try:
                request_body = json.dumps({"requests": [
                    {"indexName": "frontendsearch",
                     "params": "aroundPrecision=1000&distinct=true&facetFilters=%5B%5B%5D%5D&facets=%5B%5D&hitsPerPage=20&insideBoundingBox=&page=" + str(
                         i) + "&query=&tagFilters="}]})
                res = requests.post(url, data=request_body, headers=headers)
                if (res.status_code == 200):
                    data = list(map(lambda x: format_crowdfunder_project_data(
                        x), json.loads(res.text)["results"][0]["hits"]))
                    send_data(data, producer)
                else:
                    print(f"status_code: {res.status_code}")
                    raise Exception("Error.")
            except Exception as e:
                err_page = {
                    "page": i,
                    "err": str(e),
                }
                #save_to_json_file(err_page_file, err_page)
                traceback.print_exc()
                # break
            save_to_json_file(check_point_file, {"page": i})
        page = 0
        save_to_json_file(check_point_file, {"page": page})
