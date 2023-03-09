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
