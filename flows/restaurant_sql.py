import random
import time
from typing import List, Optional

from bs4 import BeautifulSoup
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import requests
from sqlalchemy import select

from utils.utils import (
    BASE_HEADERS,
    BASE_UE_URL,
    parse_city,
)
from utils.db_utils import (
    DB_ENGINE,
    Category,
    CategoryInfo,
    ItemInfo,
    Restaurant,
    RestaurantInfo,
    create_db_tables,
    save_categories_to_db,
    save_restaurants_to_db,
    save_items_to_db,
)
from sqlalchemy.orm import Session


def _get_rating_from_restaurant_box(restaurant_box: BeautifulSoup) -> int:
    for child in restaurant_box.findChildren("div", recursive=True):
        try:
            rating = float(child.text)
            return rating
        except Exception:
            continue
    return 0


@task
def get_menu_items(
    restaurant: Restaurant,
    items_limit: Optional[int],
) -> List[ItemInfo]:
    items = []
    time.sleep(random.uniform(0, 2))

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
        save_items_to_db(restaurant, items)
    except Exception as e:
        print(f"While getting items in restaurant: {restaurant}, got exception: {e}")
    finally:
        return items


@task
def get_restaurants_in_category(
    category: Category,
    restaurants_limit: Optional[int],
) -> List[RestaurantInfo]:
    time.sleep(random.uniform(0, 2))
    restaurants = []

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
                    rating = _get_rating_from_restaurant_box(header.parent.parent)
                    restaurants.append(
                        RestaurantInfo(restaurant_name, rating, rel_restaurant_url)
                    )
                    enumerated += 1
            except Exception as e:
                print(
                    f"While getting restaurant from match: {header}, got exception: {e}"
                )
                continue

        print(f"Found {len(restaurants)} restaurants in {category.name}")
        save_restaurants_to_db(category, restaurants)
    except Exception as e:
        print(f"While getting restaurants in category: {category}, got exception: {e}")
    finally:
        return restaurants


@task
def get_categories_in_city(
    city: str,
    categories_limit: Optional[int] = None,
) -> List[CategoryInfo]:
    time.sleep(random.uniform(0, 2))
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
                name, rel_url = match.get("data-test"), match.get("href")
                categories.append(CategoryInfo(name, rel_url))
            except Exception as e:
                print(f"While getting category from match: {match}, got exception: {e}")
                continue

        print(
            f"Saving {final_categories_limit} categories out of {len(matches)} matches for {city}."
        )
        save_categories_to_db(categories)
    except Exception as e:
        print(f"While getting categories for city: {city}, got exception: {e}")
    finally:
        return categories


@flow(task_runner=ConcurrentTaskRunner())
def restaurants_flow():
    cities = ["Emeryville", "Oakland"]
    # cities = ["Emeryville", "Oakland", "Berkeley", "Alameda", "Albany"]
    # categories_limit, restaurants_limit, items_limit = None, 5, 50
    categories_limit, restaurants_limit, items_limit = 2, 2, 2
    # num_cpus = 16
    db_timeout_sec = 300

    print(
        f"Starting the flow with cities {cities}, {categories_limit} categories, {restaurants_limit} restaurants, {items_limit} items per restaurant, and {db_timeout_sec} seconds timeout for the DB."
    )

    # 1
    create_db_tables()
    # 2
    get_categories_in_city.map(cities, categories_limit)
    # 3
    with Session(DB_ENGINE) as session, session.begin():
        categories_query = select(Category)
        categories = session.execute(categories_query).scalars().all()
    get_restaurants_in_category.map(categories, categories_limit)
    # 4
    with Session(DB_ENGINE) as session, session.begin():
        restaurants_query = select(Restaurant)
        restaurants = session.execute(restaurants_query).scalars().all()
    get_menu_items.map(restaurants, items_limit)

    print("Finished the flow.")
