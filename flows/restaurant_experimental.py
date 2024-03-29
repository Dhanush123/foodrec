import json
import random
import time
from typing import List, Optional

from bs4 import BeautifulSoup
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from prefect_ray.task_runners import RayTaskRunner
from prefect_ray.context import remote_options
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils.utils import (
    BASE_HEADERS,
    BASE_UE_URL,
    parse_city,
    setup_browser,
)
from utils.db_utils import (
    DB_ENGINE,
    Category,
    CategoryInfo,
    Item,
    ItemInfo,
    Restaurant,
    RestaurantInfo,
    create_db_tables,
    get_categories_from_db,
    get_items_from_db,
    get_restaurants_from_db,
    populate_item_in_db,
    save_categories_to_db,
    save_restaurants_to_db,
    save_items_to_db,
)

DEFAULT_SLEEP_SEC = 10


@task(timeout_seconds=60)
def get_categories_from_db_task() -> List[Category]:
    return get_categories_from_db()


@task(timeout_seconds=60)
def get_restaurants_from_db_task() -> List[Restaurant]:
    return get_restaurants_from_db()


@task(timeout_seconds=60)
def get_items_from_db_task() -> List[Item]:
    return get_items_from_db()


@task
def get_and_populate_items_experimental(restaurant: Restaurant) -> None:
    time.sleep(random.uniform(0, 10))
    # full_url = "https://www.ubereats.com/store/la-estrella-food-truck/1S1RJ9zXQC23uwBxwtXR3A?diningMode=DELIVERY&pl=JTdCJTIyYWRkcmVzcyUyMiUzQSUyMkNvdmFyaWFudC5haSUyMiUyQyUyMnJlZmVyZW5jZSUyMiUzQSUyMkNoSUpFdzRlTTBaX2hZQVJVY21OTmp4MlREbyUyMiUyQyUyMnJlZmVyZW5jZVR5cGUlMjIlM0ElMjJnb29nbGVfcGxhY2VzJTIyJTJDJTIybGF0aXR1ZGUlMjIlM0EzNy44NDExNTc2JTJDJTIybG9uZ2l0dWRlJTIyJTNBLTEyMi4yOTU4MTMxJTdE"
    full_url = f"{BASE_UE_URL}{restaurant.rel_url}?diningMode=DELIVERY&pl=JTdCJTIyYWRkcmVzcyUyMiUzQSUyMkNvdmFyaWFudC5haSUyMiUyQyUyMnJlZmVyZW5jZSUyMiUzQSUyMkNoSUpFdzRlTTBaX2hZQVJVY21OTmp4MlREbyUyMiUyQyUyMnJlZmVyZW5jZVR5cGUlMjIlM0ElMjJnb29nbGVfcGxhY2VzJTIyJTJDJTIybGF0aXR1ZGUlMjIlM0EzNy44NDExNTc2JTJDJTIybG9uZ2l0dWRlJTIyJTNBLTEyMi4yOTU4MTMxJTdE"
    print(f"Getting items from restaurant: {restaurant.name} with url: {full_url}")
    res = requests.get(full_url, headers=BASE_HEADERS)
    page_info = BeautifulSoup(res.text, features="html.parser")
    matches = page_info.find_all("script", type="application/ld+json")
    all_item_infos = []
    try:
        for match in matches:
            match = json.loads(match.text)
            if match.get("@type") == "Restaurant":
                menu = match.get("hasMenu")
                if menu:
                    menu_selection = menu.get("hasMenuSection")
                    if menu_selection:
                        for menu in menu_selection:
                            menu_items = menu.get("hasMenuItem")
                            if menu_items:
                                for item in menu_items:
                                    name, description = item.get("name"), item.get(
                                        "description"
                                    )
                                    dummy_rel_url = f"{name}+{restaurant.id}"
                                    # TODO: drop rel_url col fro DB and info
                                    item_info = ItemInfo(
                                        name, description, dummy_rel_url
                                    )
                                    all_item_infos.append(item_info)
                break
        save_items_to_db(restaurant, all_item_infos)
    except Exception as e:
        print(
            f"While getting experimental items from restaurant: {restaurant.name}, got exception: {e}"
        )
    finally:
        return all_item_infos


@flow(task_runner=RayTaskRunner())
def experimental_restaurants_flow():
    # cities = ["Emeryville", "Oakland"]
    # categories_limit, restaurants_limit, items_limit = 2, 2, 2
    cities = ["Emeryville", "Oakland", "Berkeley", "Alameda", "Albany"]
    categories_limit, restaurants_limit, items_limit = None, 5, 50
    num_cpus = 20
    sleep_sec = num_cpus * 0.5

    print(
        f"Starting the experimental flow with cities {cities}, {categories_limit} categories, {restaurants_limit} restaurants, and {items_limit} items per restaurant for the DB."
    )

    with remote_options(num_cpus=num_cpus):
        restaurants = get_restaurants_from_db_task()
        for restaurant in restaurants:
            get_and_populate_items_experimental.submit(restaurant)


# @task(timeout_seconds=120)
# def populate_item(
#     item: Item,
#     sleep_sec: Optional[float] = DEFAULT_SLEEP_SEC,
# ) -> Optional[ItemInfo]:
#     time.sleep(random.uniform(0, sleep_sec))

#     try:
#         print(f"Populating item: {item.rel_url}")
#         browser: webdriver.Chrome = setup_browser()
#         # example full_url = "https://www.ubereats.com/store/homeroom-to-go/32drpQtyRNeTXI0jm6wP0A?diningMode=DELIVERY&mod=quickView&modctx=%257B%2522storeUuid%2522%253A%2522df676ba5-0b72-44d7-935c-8d239bac0fd0%2522%252C%2522sectionUuid%2522%253A%25227d980256-87b4-5f9d-980e-f5ceda776ec4%2522%252C%2522subsectionUuid%2522%253A%25227e12b8f3-15c3-520f-bd0a-58c7cee4ca68%2522%252C%2522itemUuid%2522%253A%2522386a3b04-283a-53f8-b6c2-4734846be037%2522%257D&ps=1"
#         full_url = f"{BASE_UE_URL}{item.rel_url}&diningMode=DELIVERY&pl=JTdCJTIyYWRkcmVzcyUyMiUzQSUyMkNvdmFyaWFudC5haSUyMiUyQyUyMnJlZmVyZW5jZSUyMiUzQSUyMkNoSUpFdzRlTTBaX2hZQVJVY21OTmp4MlREbyUyMiUyQyUyMnJlZmVyZW5jZVR5cGUlMjIlM0ElMjJnb29nbGVfcGxhY2VzJTIyJTJDJTIybGF0aXR1ZGUlMjIlM0EzNy44NDExNTc2JTJDJTIybG9uZ2l0dWRlJTIyJTNBLTEyMi4yOTU4MTMxJTdE"
#         browser.get(full_url)

#         # element 0 is the restaurant name, element 1 is the item name
#         h1_elements = WebDriverWait(browser, 30).until(
#             EC.visibility_of_all_elements_located((By.TAG_NAME, "h1"))
#         )
#         item_element = h1_elements[1]
#         item_name = item_element.text
#         item_description = None

#         try:
#             parent_element = item_element.find_element(By.XPATH, "..")
#             child_elements = parent_element.find_elements(By.TAG_NAME, "div")
#             for e in child_elements:
#                 if e.text:
#                     item_description = e.text
#                     break
#         except Exception as e:
#             print(
#                 f"populate_item item_description exception for item full_url {full_url}: {e}"
#             )
#         finally:
#             item_info = ItemInfo(item_name, item_description, item.rel_url)
#             populate_item_in_db(item, item_info)
#             browser.quit()
#             return item_info
#     except Exception as e:
#         print(f"populate_item exception for item {full_url}: {e}")
#         return None


# @task(timeout_seconds=TASK_TIMEOUT_SECONDS)
# def get_items(
#     restaurant: Restaurant,
#     items_limit: Optional[int],
#     sleep_sec: Optional[float] = DEFAULT_SLEEP_SEC,
# ) -> List[ItemInfo]:
#     time.sleep(random.uniform(0, sleep_sec))
#     items = []

#     try:
#         restaurant_res = requests.get(
#             f"{BASE_UE_URL}{restaurant.rel_url}", headers=BASE_HEADERS
#         )
#         menu_info = BeautifulSoup(restaurant_res.text, features="html.parser")

#         # There seem to be duplicate items/hrefs
#         item_hrefs = set(
#             item_url_element["href"]
#             for item_url_element in menu_info.find_all(
#                 "a", href=lambda href: href and href.startswith(restaurant.rel_url)
#             )
#         )

#         # Ensure valid data
#         items_hrefs_to_remove = set()
#         for href in item_hrefs:
#             if (
#                 href == restaurant.rel_url
#                 or "storeInfo" in href
#                 or "?mod=quickView" not in href
#             ):
#                 items_hrefs_to_remove.add(href)
#         item_hrefs -= items_hrefs_to_remove

#         # Transform data
#         item_hrefs = list(item_hrefs)
#         final_items_limit = items_limit or len(item_hrefs)
#         items = [
#             ItemInfo(name=None, description=None, rel_url=url)
#             for url in item_hrefs[:final_items_limit]
#         ]

#         print(f"Found {len(items)} items in {restaurant.name}")
#         save_items_to_db(restaurant, items)
#     except Exception as e:
#         print(
#             f"While getting items in restaurant: {restaurant.name}, got exception: {e}"
#         )
#     finally:
#         return items
