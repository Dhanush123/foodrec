import random
import time
from typing import List, Optional
import concurrent.futures

from bs4 import BeautifulSoup
from prefect import flow, task, unmapped
from prefect.task_runners import ConcurrentTaskRunner
from prefect_ray.task_runners import RayTaskRunner
from prefect_ray.context import remote_options

import requests


from utils import (
    BASE_HEADERS,
    BASE_UE_URL,
    CategoryInfo,
    ItemInfo,
    RestaurantInfo,
    add_items_to_db,
    create_db,
    get_db_con,
    parse_city,
)

CATEGORIES_LIMIT, RESTAURANT_LIMIT, ITEMS_LIMIT, DB_TIMEOUT_SEC = 1, 1, 1, 300
CITIES = ["Emeryville", "Oakland"]
# cities = ["Emeryville", "Oakland", "Berkeley", "Alameda", "Albany"]


def get_rating_from_restaurant_box(restaurant_box: BeautifulSoup) -> int:
    for child in restaurant_box.findChildren("div", recursive=True):
        try:
            rating = float(child.text)
            return rating
        except Exception:
            continue
    return 0


@flow
def save_item_to_db(items: List[ItemInfo], db_timeout_sec: int):
    try:
        time.sleep(random.uniform(0, 4))
        with get_db_con(db_timeout_sec) as db_con:
            add_items_to_db(db_con, items)
        db_con.close()
    except Exception as e:
        print(f"While saving items to db, got exception: {e}")


@flow
def get_menu_items_for_restaurant(
    restaurant: RestaurantInfo,
    items_limit: Optional[int],
) -> List[ItemInfo]:
    global DB_TIMEOUT_SEC
    time.sleep(random.uniform(0, 4))
    items = []

    try:
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

        # Transform data
        item_hrefs = list(item_hrefs)
        final_items_limit = items_limit or len(item_hrefs)
        items = [
            ItemInfo(name=None, description=None, rel_url=url, restaurant=restaurant)
            for url in item_hrefs[:final_items_limit]
        ]

        print(f"Found {len(items)} items in {restaurant.name}")
        save_item_to_db.submit(items, DB_TIMEOUT_SEC)
    except Exception as e:
        print(f"While getting items in restaurant: {restaurant}, got exception: {e}")
    finally:
        return items


@flow
def get_restaurants_in_category(
    category: CategoryInfo, restaurants_limit: Optional[int]
) -> List[RestaurantInfo]:
    global ITEMS_LIMIT
    restaurants = []
    time.sleep(random.uniform(0, 4))

    try:
        # TODO: filter out restaurants that are too far for delivery
        category_res = requests.get(
            f"{BASE_UE_URL}{category.rel_url}", headers=BASE_HEADERS
        )
        page_info = BeautifulSoup(category_res.text, features="html.parser")
        enumerated = 0

        for header in page_info.find_all("h3"):
            if restaurants_limit is not None and enumerated == restaurants_limit:
                break
            try:
                if header.parent is None or header.parent.get("href") is None:
                    continue
                rel_restaurant_url = header.parent.get("href")
                # Some urls might not be a restaurant
                if rel_restaurant_url.startswith("/store"):
                    restaurant_name = header.get_text()
                    rating = get_rating_from_restaurant_box(header.parent.parent)
                    restaurants.append(
                        RestaurantInfo(
                            restaurant_name, rating, rel_restaurant_url, category
                        )
                    )
                    enumerated += 1
            except Exception as e:
                print(
                    f"While getting restaurant from match: {header}, got exception: {e}"
                )
                continue

        print(f"Found {len(restaurants)} restaurants in {category.name}")
        for restaurant in restaurants:
            get_menu_items_for_restaurant.submit(restaurant, ITEMS_LIMIT)
    except Exception as e:
        print(f"While getting restaurants in category: {category}, got exception: {e}")
        return
    finally:
        return restaurants


@flow
def get_categories_in_city(
    city: str,
    categories_limit: Optional[int] = None,
) -> List[CategoryInfo]:
    global RESTAURANT_LIMIT
    time.sleep(random.uniform(0, 4))
    categories = []

    try:
        categories_url = f"{BASE_UE_URL}/category/{parse_city(city)}"
        categories_res = requests.get(categories_url, headers=BASE_HEADERS)
        page_info = BeautifulSoup(categories_res.text, features="html.parser")
        matches = page_info.find("main").find_all(
            "a", href=lambda href: href and href.startswith("/category")
        )
        final_categories_limit = categories_limit or len(matches)

        for i, match in enumerate(matches):
            if i == final_categories_limit:
                break
            try:
                categories.append(
                    CategoryInfo(match.get("data-test"), match.get("href"))
                )
            except Exception as e:
                print(f"While getting category from match: {match}, got exception: {e}")
                continue

        print(
            f"Returning {final_categories_limit} categories out of {len(matches)} matches for {city}."
        )
        for category in categories:
            get_restaurants_in_category.submit(category, RESTAURANT_LIMIT)
    except Exception as e:
        print(f"While getting categories for city: {city}, got exception: {e}")
    finally:
        return categories


@flow(task_runner=RayTaskRunner())
def restaurants_flow():
    global CATEGORIES_LIMIT, CITIES

    for city in CITIES:
        get_categories_in_city.submit(city, CATEGORIES_LIMIT)


if __name__ == "__main__":
    with get_db_con() as db_con:
        create_db(db_con)
    db_con.close()

    restaurants_flow()
