import asyncio
import random
import sqlite3
import time
from typing import List, Optional, Tuple
import aiohttp

from bs4 import BeautifulSoup

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


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


def get_rating_from_restaurant_box(restaurant_box: BeautifulSoup) -> int:
    for child in restaurant_box.findChildren("div", recursive=True):
        try:
            rating = float(child.text)
            return rating
        except Exception:
            continue
    return 0


@task(timeout_seconds=60)
async def save_item_to_db(
    items: List[ItemInfo],
    db_timeout_sec: int,
    db_con: Optional[sqlite3.Connection] = None,
):
    try:
        time.sleep(random.uniform(0, 4))
        with db_con or get_db_con(db_timeout_sec) as final_db_con:
            await add_items_to_db(final_db_con, items)
        db_con.close()
    except Exception as e:
        print(f"While saving items to db, got exception: {e}")


@task(timeout_seconds=60)
async def get_menu_items_from_restaurant(
    restaurant: RestaurantInfo,
    items_limit: Optional[int],
) -> List[ItemInfo]:
    items = []
    try:
        time.sleep(random.uniform(0, 4))

        restaurant_res = await requests.get(
            f"{BASE_UE_URL}{restaurant.rel_url}", headers=BASE_HEADERS
        )
        menu_info = BeautifulSoup(restaurant_res.text(), features="html.parser")
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

        item_hrefs = list(item_hrefs)
        final_items_limit = items_limit or len(item_hrefs)
        items = [
            ItemInfo(name=None, description=None, rel_url=url, restaurant=restaurant)
            for url in item_hrefs[:final_items_limit]
        ]

        print(f"Found {len(items)} items in {restaurant.name}")
    except Exception as e:
        print(f"While getting items in restaurant: {restaurant}, got exception: {e}")
    finally:
        return items


@task(timeout_seconds=60)
async def get_restaurants_in_category(category: CategoryInfo) -> Tuple[str, str]:
    restaurants = []

    try:
        time.sleep(random.uniform(0, 4))
        # TODO: filter out restaurants that are too far for delivery
        async with aiohttp.ClientSession() as session:
            url = f"{BASE_UE_URL}{category.rel_url}"
            async with session.get(url, headers=BASE_HEADERS) as response:
                return (url, await response.text())
    except Exception as e:
        print(f"While getting restaurants in category: {category}, got exception: {e}")
    finally:
        return restaurants


@task(timeout_seconds=60)
async def parse_restaurants_in_category(
    category_response: CategoryInfo, restaurants_limit: Optional[int]
) -> List[RestaurantInfo]:
    restaurants = []
    category_response_url, category_response_text = category_response

    try:
        page_info = BeautifulSoup(category_response_text, features="html.parser")

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
    except Exception as e:
        print(f"While getting restaurants in category: {category}, got exception: {e}")
    finally:
        return restaurants


@task(timeout_seconds=60)
async def get_categories_in_city(city: str) -> Tuple[str, str]:
    time.sleep(random.uniform(0, 4))

    try:
        async with aiohttp.ClientSession() as session:
            url = f"{BASE_UE_URL}/category/{parse_city(city)}"
            async with session.get(url, headers=BASE_HEADERS) as response:
                return (url, await response.text())
    except Exception as e:
        print(f"While getting categories for city: {city}, got exception: {e}")


@task(timeout_seconds=60)
def parse_categories_in_city(
    city_response: Tuple[str, str],
    categories_limit: Optional[int] = None,
) -> List[CategoryInfo]:
    categories = []
    city_response_url, city_response_text = city_response

    try:
        page_info = BeautifulSoup(city_response_text, features="html.parser")
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
            f"Returning {final_categories_limit} categories out of {len(matches)} matches for {city_response_url}."
        )
    except Exception as e:
        print(
            f"While parsing categories for city: {city_response_url}, got exception: {e}"
        )
    finally:
        return categories


@flow(task_runner=ConcurrentTaskRunner())
async def restaurants_flow():
    cities = ["Emeryville", "Oakland"]
    categories_limit, restaurants_limit, items_limit = 2, 2, 2
    db_timeout_sec = 300

    with get_db_con() as db_con:
        create_db(db_con)
    db_con.close()

    print(
        f"Starting the flow with cities {cities}, {categories_limit} categories, {restaurants_limit} restaurants, {items_limit} items per restaurant."
    )

    category_tasks = [get_categories_in_city(city) for city in cities]
    category_responses = await asyncio.gather(*category_tasks, return_exceptions=True)
    categories = parse_categories_in_city.map(category_responses, categories_limit)

    restaurant_tasks = [
        get_restaurants_in_category(category) for category in categories
    ]
    restaurant_responses = await asyncio.gather(
        *restaurant_tasks, return_exceptions=True
    )
    restaurants = parse_restaurants_in_category.map(
        restaurant_responses, categories_limit
    )

    item_tasks = [
        get_menu_items_from_restaurant(restaurant) for restaurant in restaurants
    ]
    item_responses = await asyncio.gather(*item_tasks, return_exceptions=True)
    items = parse_menu_items_from_restaurant.map(item_responses, items_limit)

    for item in items:
        await save_item_to_db(items, db_timeout_sec)

    # cities = ["Emeryville", "Oakland"]
    # # cities = ["Emeryville", "Oakland", "Berkeley", "Alameda", "Albany"]
    # # categories_limit, restaurants_limit, items_limit = None, 5, 50
    # categories_limit, restaurants_limit, items_limit = 2, 2, 2
    # # num_cpus = 16
    # db_timeout_sec = 300

    # with get_db_con() as db_con:
    #     create_db(db_con)
    # db_con.close()

    # print(
    #     f"Starting the flow with cities {cities}, {categories_limit} categories, {restaurants_limit} restaurants, {items_limit} items per restaurant, and {db_timeout_sec} seconds timeout for the DB."
    # )

    # category_responses = [
    #     get_categories_in_city(city, categories_limit) for city in cities
    # ]
    # restaurant_responses = []
    # item_responses = []
    # for categories_res in asyncio.as_completed(category_responses):
    #     categories = await categories_res
    #     for category in categories:
    #         restaurant_responses.append(
    #             get_restaurants_in_category(category, restaurants_limit)
    #         )
    # for restaurants_res in asyncio.as_completed(restaurant_responses):
    #     restaurants = await restaurants_res
    #     for restaurant in restaurants:
    #         item_responses.append(
    #             get_menu_items_from_restaurant(restaurant, items_limit)
    #         )
    # for items_res in asyncio.as_completed(item_responses):
    #     items = await items_res
    #     print(f"Got {len(items)} items from {items[0].restaurant.name}")
    #     # print("")
    #     # await save_item_to_db(items, db_timeout_sec)

    print("Finished the flow.")


if __name__ == "__main__":
    asyncio.run(restaurants_flow())
