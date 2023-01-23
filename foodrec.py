from typing import List, NamedTuple

import requests
from bs4 import BeautifulSoup, UnicodeDammit

BASE_UE_URL = "https://www.ubereats.com"

def construct_city_url(city: str) -> str:
    city = city.replace(" ", "-").lower()
    url = f"{BASE_UE_URL}/city/{city.lower()}-ca"
    return url

class RestaurantInfo(NamedTuple):
    name: str
    url: str

def get_restaurants_in_city(city: str) -> List[RestaurantInfo]:
    city_url = construct_city_url(city)
    city_retaurants_res = requests.get(city_url, headers={'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'})
    page_info = BeautifulSoup(city_retaurants_res.text, 'html.parser') 
    info_list = []
    for header in page_info.findAll("h3"):
        try:
            restaurant_name = header.get_text()
            restaurant_url = f"{BASE_UE_URL}{header.parent.get('href')}"
            info_list.append(RestaurantInfo(restaurant_name, restaurant_url))
        except Exception:
            continue
    return info_list

if __name__ == "__main__":
    city = "Dublin"
    # city = input("Enter a city: ")
    restaurants = get_restaurants_in_city(city)
    # soup = BeautifulSoup(restaurants, 'html.parser')
    with open('tmp.txt', 'w') as f:
        for restaurant in restaurants:
            f.write(f"{restaurant.name} | {restaurant.url}\n")
        # f.write(UnicodeDammit(soup.prettify()).unicode_markup)