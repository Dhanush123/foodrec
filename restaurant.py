import os
from pathlib import Path
import random
import time
from typing import Dict, List, NamedTuple, Optional
import concurrent.futures
import sqlite3

from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


from utils import BASE_HEADERS, BASE_UE_URL, CategoryInfo, ItemInfo, RestaurantInfo, get_queue_con, parse_city


def get_rating_from_restaurant_box(restaurant_box: BeautifulSoup) -> int:
    for child in restaurant_box.findChildren("div", recursive=True):
        try:
            rating = float(child.text)
            return rating
        except Exception:
            continue
    return 0


def get_restaurants_in_category(
    category: CategoryInfo, restaurants_limit: Optional[int]
) -> List[RestaurantInfo]:
    # TODO: filter out restaurants that are too far for delivery
    category_res = requests.get(
        f"{BASE_UE_URL}{category.rel_url}", headers=BASE_HEADERS
    )
    page_info = BeautifulSoup(category_res.text, features="html.parser")

    info_list = []
    enumerated = 0
    for header in page_info.find_all("h3"):
        if restaurants_limit is not None and enumerated == restaurants_limit:
            break
        try:
            rel_restaurant_url = header.parent.get("href")
            # Some urls might not be a restaurant
            if rel_restaurant_url.startswith("/store"):
                restaurant_name = header.get_text()
                rating = get_rating_from_restaurant_box(header.parent.parent)
                info_list.append(
                    RestaurantInfo(restaurant_name, rating, rel_restaurant_url)
                )
                enumerated += 1
        except Exception:
            continue
    return info_list


def get_categories_info_for_city(city: str) -> List[CategoryInfo]:
    categories = []
    categories_url = f"{BASE_UE_URL}/category/{parse_city(city)}"
    categories_res = requests.get(categories_url, headers=BASE_HEADERS)
    page_info = BeautifulSoup(categories_res.text, features="html.parser")
    matches = page_info.find_all(
        "a", href=lambda href: href and href.startswith("/category")
    )
    for match in matches:
        try:
            categories.append(CategoryInfo(match["data-test"], match["href"]))
        except Exception:
            continue
    return categories


def get_menu_items(
    restaurant: RestaurantInfo,
    items_limit: Optional[int],
) -> List[ItemInfo]:
    restaurant_res = requests.get(
        f"{BASE_UE_URL}{restaurant.rel_url}", headers=BASE_HEADERS
    )
    menu_info = BeautifulSoup(restaurant_res.text, features="html.parser")
    # There seem to be duplicate items/hrefs
    item_hrefs = set(
        item_url_element["href"]
        for item_url_element in menu_info.find_all(
            "a", href=lambda href: href and href.startswith(restaurant.rel_url)
        )
    )

    # Ensure valid data
    items_hrefs_to_remove = set()
    for href in item_hrefs:
        if (
            href == restaurant.rel_url
            or "storeInfo" in href
            or "?mod=quickView" not in href
        ):
            items_hrefs_to_remove.add(href)
    item_hrefs -= items_hrefs_to_remove

    # futures_to_href = {}
    item_hrefs = list(item_hrefs)
    final_items_limit = items_limit or len(item_hrefs)
    return [ItemInfo(name=None, description=None, rel_url=url) for url in item_hrefs[:final_items_limit]]
    # for i in tqdm(range(final_items_limit)):
    #     # Sleep to avoid getting blocked
    #     time.sleep(sleep_sec or random.uniform(1, 10))
    #     item_info_future = executor.submit(get_item_info, item_hrefs[i], browser)
    #     futures_to_href[item_info_future] = href
    #     enumerated += 1

    # save_menu(restaurant, futures_to_href)

def get_categories_in_city(
    city: str,
    categories_limit: Optional[int] = None,
) -> List[RestaurantInfo]:

    categories = []
    categories_url = f"{BASE_UE_URL}/category/{parse_city(city)}"
    categories_res = requests.get(categories_url, headers=BASE_HEADERS)
    page_info = BeautifulSoup(categories_res.text, features="html.parser")
    matches = page_info.find_all(
        "a", href=lambda href: href and href.startswith("/category")
    )
    final_categories_limit = categories_limit or len(matches)
    print(f"Found {len(matches)} categories for {city}.")
    for i in range(final_categories_limit):
        try:
            categories.append(CategoryInfo(matches[i]["data-test"], matches[i]["href"]))
        except Exception:
            continue
    print(f"Returning {final_categories_limit} categories for {city}.")
    return categories

    # futures_to_category = {}
    # for category in categories_info:
    #     future = executor.submit(get_restaurants_in_category, category, restaurants_limit)
    #     futures_to_category[future] = category
    
    # queue = get_queue_con()
    # for future in concurrent.futures.as_completed(futures_to_category):
    #     restaurants = future.result()
    #     print(f"Found {len(restaurants)} restaurants for {futures_to_category[future].name}.")
    #     for restaurant in restaurants:
    #         get_menu(restaurant, executor, items_limit=5, sleep_sec=5)
    #         # queue.put(restaurant._asdict())


if __name__ == "__main__":
    # cities = ["Emeryville", "Oakland", "Berkeley", "Alameda", "Albany"]
    cities = ["Emeryville"]
    future_to_city = {}
    future_to_category = {}
    queue = get_queue_con()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for city in cities:
            city_future = executor.submit(get_categories_in_city, city, categories_limit=5)
            future_to_city[city_future] = city
        for future in concurrent.futures.as_completed(future_to_city):
            category = future.result()
            executor.submit(get_restaurants_in_category, category, restaurants_limit=3)
        for future in concurrent.futures.as_completed(future_to_category):
            items = future.result()
            for item in items:
                queue.put(item._asdict())