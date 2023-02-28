from typing import List, NamedTuple

from sqlalchemy import (
    ForeignKey,
    create_engine,
    Column,
    Integer,
    String,
    select,
    update,
)
from sqlalchemy.orm import Session, sessionmaker, declarative_base

DB_ENGINE = create_engine("sqlite:///data/menu.db?timeout=60", echo=False)
Session = sessionmaker(DB_ENGINE)


class CategoryInfo(NamedTuple):
    name: str
    rel_url: str


class RestaurantInfo(NamedTuple):
    name: str
    rating: float
    rel_url: str


class ItemInfo(NamedTuple):
    name: str
    description: str
    rel_url: str


# This old syntax is due to using SQLAlchemy 1.4.22, latest 2.0.+ is not compatible with Prefect
# Gives error "prefect 2.7.11 requires sqlalchemy[asyncio]!=1.4.33,<2.0,>=1.4.22, but you have sqlalchemy 2.0.4 which is incompatible"
Base = declarative_base()


class Category(Base):
    __tablename__ = "category"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    rel_url = Column(String, unique=True)


class Restaurant(Base):
    __tablename__ = "restaurant"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    rating = Column(Integer)
    rel_url = Column(String, unique=True)
    category_id = Column(Integer, ForeignKey("category.id"))


class Item(Base):
    __tablename__ = "item"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    rel_url = Column(String, unique=True)
    restaurant_id = Column(Integer, ForeignKey("restaurant.id"))


class City(Base):
    __tablename__ = "city"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    rel_url = Column(String, unique=True)


def create_db_tables() -> None:
    Base.metadata.create_all(DB_ENGINE)


def save_categories_to_db(categories: List[CategoryInfo]) -> None:
    # combining the contexts means commit and close are implicitly called
    with Session() as session, session.begin():
        db_transformed_categories = [
            Category(**category._asdict()) for category in categories
        ]
        session.add_all(db_transformed_categories)


def save_restaurants_to_db(
    category: Category, restaurants: List[RestaurantInfo]
) -> None:
    # combining the contexts means commit and close are implicitly called
    with Session() as session, session.begin():
        db_transformed_restaurants = [
            Restaurant(category_id=category.id, **restaurant._asdict())
            for restaurant in restaurants
        ]
        session.add_all(db_transformed_restaurants)


def save_items_to_db(restaurant: Restaurant, items: List[ItemInfo]) -> None:
    with Session() as session, session.begin():
        db_transformed_items = [
            Item(restaurant_id=restaurant.id, **item._asdict()) for item in items
        ]
        session.add_all(db_transformed_items)


def get_restaurants_from_db() -> List[Restaurant]:
    with Session(expire_on_commit=False) as session, session.begin():
        restaurants_query = select(Restaurant)
        restaurants = session.execute(restaurants_query).scalars().all()
        return restaurants


def get_categories_from_db() -> List[Category]:
    with Session(expire_on_commit=False) as session, session.begin():
        categories_query = select(Category)
        categories = session.execute(categories_query).scalars().all()
        return categories


def get_items_from_db() -> List[Item]:
    with Session(expire_on_commit=False) as session, session.begin():
        items_query = select(Item)
        items = session.execute(items_query).scalars().all()
        return items


def populate_item_in_db(item: Item, item_info: ItemInfo) -> None:
    with Session() as session, session.begin():
        session.execute(
            update(Item)
            .where(Item.id == item.id)
            .values(name=item_info.name, description=item_info.description)
        )
