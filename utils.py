import os
from pathlib import Path
import signal
import sqlite3
from typing import NamedTuple

import persistqueue
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

BASE_UE_URL = "https://www.ubereats.com"
BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
}

class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now = True


class CategoryInfo(NamedTuple):
    name: str
    rel_url: str


class ItemInfo(NamedTuple):
    name: str
    description: str
    rel_url: str

class RestaurantInfo(NamedTuple):
    name: str
    rating: float
    rel_url: str
    category: CategoryInfo

def parse_city(city: str) -> str:
    return f"{city.replace(' ', '-').lower()}-ca"


def get_city_url(city: str) -> str:
    city = city.replace(" ", "-").lower()
    url = f"{BASE_UE_URL}/city/{city.lower()}-ca"
    return url

def setup_browser() -> webdriver.Chrome:
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Ensure GUI is off
    chrome_options.add_argument("--no-sandbox")
    homedir = os.path.expanduser("~")
    webdriver_service = Service(f"{homedir}/chromedriver/stable/chromedriver")
    browser = webdriver.Chrome(service=webdriver_service, options=chrome_options)
    return browser

def get_queue_con() -> persistqueue.SQLiteQueue:
    queue_path = Path("item_queue.db")
    return persistqueue.SQLiteQueue(queue_path, auto_commit=True, multithreading=True)

def get_db_con() -> sqlite3.Connection:
    db_path = Path("menu.db")
    return sqlite3.connect(db_path)
