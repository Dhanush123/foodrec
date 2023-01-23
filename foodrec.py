from pathlib import Path
from typing import Dict, List, NamedTuple, Optional
import csv
from uuid import uuid4

from bs4 import BeautifulSoup, UnicodeDammit
import requests
from tqdm import tqdm

BASE_UE_URL = 'https://www.ubereats.com'
BASE_HEADERS = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'}

class RestaurantInfo(NamedTuple):
    name: str
    rating: float
    rel_url: str
    menu: Optional[Dict[str, str]] = None

class CategoryInfo(NamedTuple):
    name: str
    rel_url: str

class ItemInfo(NamedTuple):
    name: str
    description: str

def parse_city(city: str) -> str:
    return f"{city.replace(' ', '-').lower()}-ca"

def get_city_url(city: str) -> str:
    city = city.replace(' ', '-').lower()
    url = f'{BASE_UE_URL}/city/{city.lower()}-ca'
    return url

def get_rating_from_restaurant_box(restaurant_box: BeautifulSoup) -> int:
    for child in restaurant_box.findChildren('div', recursive=True):
        try:
            rating = float(child.text)
            return rating
        except Exception:
            continue
    return 0


def get_restaurants_in_category(city: str, category: CategoryInfo) -> List[RestaurantInfo]:
    # TODO: filter out restaurants that are too far for delivery
    raw_restaurants_csv_path = Path(f'restaurants-list-{parse_city(city)}.csv')
    category_res = requests.get(f'{BASE_UE_URL}{category.rel_url}', headers=BASE_HEADERS)
    page_info = BeautifulSoup(category_res.text, features='html.parser') 
    with open(raw_restaurants_csv_path, 'w') as f:
        f.write(page_info.prettify())
    info_list = []

    limit = 1
    for i, header in tqdm(enumerate(page_info.find_all('h3'))):
        # TODO: limiting num for testing
        if i == limit:
            break
        try:
            rel_restaurant_url = header.parent.get('href')
            # Some urls might not be a restaurant
            if rel_restaurant_url.startswith('/store'):
                restaurant_name = header.get_text()
                rating = get_rating_from_restaurant_box(header.parent.parent)
                info_list.append(RestaurantInfo(restaurant_name, rating, rel_restaurant_url))
        except Exception:
            continue
    return info_list

def get_categories_info_for_city(city: str) -> List[CategoryInfo]:
    categories = []
    categories_url = f'{BASE_UE_URL}/category/{parse_city(city)}'
    categories_res = requests.get(categories_url, headers=BASE_HEADERS)
    page_info = BeautifulSoup(categories_res.text, features='html.parser') 
    matches = page_info.find_all('a', href=lambda href: href and href.startswith('/category'))
    for match in tqdm(matches):
        try:
            categories.append(CategoryInfo(match['data-test'], match['href']))
        except Exception:
            continue
    return categories

def get_item_info(item_url: str, temp_num: int) -> Optional[ItemInfo]:
    # TODO: figure out why the item name isn't showing up in H1 unlike manually in Chrome, seems to show up in a span which is filter out
    # diningMode and pl were manually added to see if they make a difference
    print(f'item_url {item_url}')
    full_url = f'{BASE_UE_URL}{item_url}&diningMode=DELIVERY&pl=JTdCJTIyYWRkcmVzcyUyMiUzQSUyMkNvdmFyaWFudC5haSUyMiUyQyUyMnJlZmVyZW5jZSUyMiUzQSUyMkNoSUpFdzRlTTBaX2hZQVJVY21OTmp4MlREbyUyMiUyQyUyMnJlZmVyZW5jZVR5cGUlMjIlM0ElMjJnb29nbGVfcGxhY2VzJTIyJTJDJTIybGF0aXR1ZGUlMjIlM0EzNy44NDExNTc2JTJDJTIybG9uZ2l0dWRlJTIyJTNBLTEyMi4yOTU4MTMxJTdE'
    item_res = requests.get(full_url, headers=BASE_HEADERS, allow_redirects=True)
    item_info = BeautifulSoup(item_res.text, features='html.parser')
    try:
        # title_elements = item_info.find_all('h1')
        # for title_element in title_elements:
        #     print("title_element", title_element)
        with open(f"{temp_num}.txt", 'w') as f:
            f.write(item_info.prettify())
        # # Another filtering criteria
        # print("item_name_element", item_name_element)
        # print("item_name_element.parent", item_name_element.parent)
        # if not item_name_element.parent.find('span', text=lambda text: text and text.startswith('$'), recursive=True):
        #     return None

        # item_name = item_name_element.text
        # print("item_name", item_name)
        # item_description = item_name_element.parent.find('div', text=lambda x: x is not None, recursive=True).text
        # print("item_description", item_description)
        # return ItemInfo(item_name, item_description)
    except Exception as e:
        print(e)
        return None
       

def get_restaurant_info(restaurant: RestaurantInfo) -> RestaurantInfo:
    restaurant_res = requests.get(f'{BASE_UE_URL}{restaurant.rel_url}', headers=BASE_HEADERS)
    menu_info = BeautifulSoup(restaurant_res.text, features='html.parser') 
    item_url_matches = menu_info.find_all('a', href=lambda href: href and href.startswith(restaurant.rel_url))
    menu = {}
    limit = 30
    for i, item_url_element in tqdm(enumerate(item_url_matches)):
        # print("item_url_element", item_url_element)
        # TODO: limiting num for testing
        if i == limit:
            break
        href = item_url_element['href']
        if href == restaurant.rel_url or 'storeInfo' in href or '?mod=quickView' not in href:
            continue
        print("item_url_element href", i)
        item_info = get_item_info(href, i)
        if item_info:
            menu[item_info.name] = item_info.description
    if menu:
        return RestaurantInfo(restaurant.name, restaurant.rating, restaurant.rel_url, menu.prettify())
    return restaurant

def get_restaurants(city: str) -> List[RestaurantInfo]:
    restaurants_csv_path = Path(f'restaurants-info-{parse_city(city)}.csv')
    # TODO: limiting num for testing
    categories_info = get_categories_info_for_city(city)[:1]
    for category in tqdm(categories_info):
        restaurants = get_restaurants_in_category(city, category)
        for i in tqdm(range(len(restaurants))):
            restaurants[i] = get_restaurant_info(restaurants[i])
        with open(restaurants_csv_path, 'a') as f:
            w = csv.writer(f)
            if not restaurants_csv_path.exists():
                w.writerow(['name', 'rating', 'url', 'category'])
            w.writerows([tuple(restaurant) + (category.name,) for restaurant in restaurants])

if __name__ == "__main__":
    city = "Dublin"
    # city = input("Enter a city: ")
    get_restaurants(city)