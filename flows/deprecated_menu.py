import os
from pathlib import Path
import random
import time
from typing import Dict, List, NamedTuple, Optional
import concurrent.futures
import sqlite3

from bs4 import BeautifulSoup
import persistqueue
import requests
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


from utils import (
    BASE_HEADERS,
    BASE_UE_URL,
    CategoryInfo,
    GracefulKiller,
    ItemInfo,
    RestaurantInfo,
    get_db_con,
    setup_browser,
)


def get_queue_con() -> sqlite3.Connection:
    queue_path = Path("item_queue.db")
    return persistqueue.SQLiteQueue(queue_path, auto_commit=True, multithreading=True)


def get_item(item: ItemInfo, browser: webdriver.Chrome) -> Optional[ItemInfo]:
    # example full_url = "https://www.ubereats.com/store/homeroom-to-go/32drpQtyRNeTXI0jm6wP0A?diningMode=DELIVERY&mod=quickView&modctx=%257B%2522storeUuid%2522%253A%2522df676ba5-0b72-44d7-935c-8d239bac0fd0%2522%252C%2522sectionUuid%2522%253A%25227d980256-87b4-5f9d-980e-f5ceda776ec4%2522%252C%2522subsectionUuid%2522%253A%25227e12b8f3-15c3-520f-bd0a-58c7cee4ca68%2522%252C%2522itemUuid%2522%253A%2522386a3b04-283a-53f8-b6c2-4734846be037%2522%257D&ps=1"
    try:
        full_url = f"{BASE_UE_URL}{item.rel_url}&diningMode=DELIVERY&pl=JTdCJTIyYWRkcmVzcyUyMiUzQSUyMkNvdmFyaWFudC5haSUyMiUyQyUyMnJlZmVyZW5jZSUyMiUzQSUyMkNoSUpFdzRlTTBaX2hZQVJVY21OTmp4MlREbyUyMiUyQyUyMnJlZmVyZW5jZVR5cGUlMjIlM0ElMjJnb29nbGVfcGxhY2VzJTIyJTJDJTIybGF0aXR1ZGUlMjIlM0EzNy44NDExNTc2JTJDJTIybG9uZ2l0dWRlJTIyJTNBLTEyMi4yOTU4MTMxJTdE"
        browser.get(full_url)
        # element 0 is the restaurant name, element 1 is the item name
        item_element = browser.find_elements(By.TAG_NAME, "h1")[1]
        item_name = item_element.text
        item_description = None
        try:
            parent_element = item_element.find_element(By.XPATH, "..")
            child_elements = parent_element.find_elements(By.TAG_NAME, "div")
            for e in child_elements:
                if e.text:
                    item_description = e.text
                    break
            print(f"get_item success: {item_name}, {item_description}")
            return ItemInfo(item_name, item_description)
        except Exception as e:
            print(f"get_item item_description exception: {e}")
            # item_description will probably be None
            return ItemInfo(item_name, item_description)
    except Exception as e:
        print(f"get_item exception: {e}")
        return None


def save_item(restaurant: RestaurantInfo, db_con: sqlite3.Connection) -> None:
    try:
        if item_info is None:
            print(f"href {href} item_info is None")
            continue
        con.execute(
            "INSERT INTO items (restaurant_name, restaurant_rating, restaurant_rel_url, category_name, category_rel_url, item_name, item_description) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                restaurant.name,
                restaurant.rating,
                restaurant.rel_url,
                restaurant.category.name,
                restaurant.category.rel_url,
                item_info.name,
                item_info.description,
            ),
        )
    except Exception as exc:
        print(f"href {href} future/db exception: {exc}")


if __name__ == "__main__":
    with get_db_con() as con:
        con.execute(
            "CREATE TABLE IF NOT EXISTS items (restaurant_name TEXT, restaurant_rating REAL, restaurant_rel_url TEXT, category_name TEXT, category_rel_url TEXT, item_name TEXT, item_description TEXT, UNIQUE(item_name, item_description) ON CONFLICT REPLACE)"
        )
    killer = GracefulKiller()
    queue = get_queue_con()
    future_to_item = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        queue = get_queue_con()
        while not killer.kill_now:
            try:
                queue_task = queue.get()
                item = ItemInfo(**queue.get())
                future = executor.submit(get_item, item, setup_browser())
                future_to_item[future] = item
                time.sleep(random.uniform(1, 10))
            except Exception as e:
                print(f"queue exception: {e}")
                break
