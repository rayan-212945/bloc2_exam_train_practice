"""
========================================================
SCRIPT : db.py
OBJECTIF :
Créer les tables relationnelles avec SQLAlchemy ORM
========================================================
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, Text
from sqlalchemy.orm import declarative_base

# Chargement du .env
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
load_dotenv(os.path.join(PROJECT_DIR, ".env"))

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
Base = declarative_base()


class Product(Base):
    __tablename__ = "dim_product"

    product_id = Column(String, primary_key=True)
    product_name = Column(String)
    category = Column(String)
    brand = Column(String)
    cost_price = Column(Float)
    sale_price = Column(Float)


class Store(Base):
    __tablename__ = "dim_store"

    store_id = Column(String, primary_key=True)
    store_name = Column(String)
    city = Column(String)
    region = Column(String)
    opening_date = Column(String)


class Order(Base):
    __tablename__ = "fact_orders"

    order_id = Column(String, primary_key=True)
    order_date = Column(String)
    customer_id = Column(String)
    product_id = Column(String)
    store_id = Column(String)
    quantity = Column(Integer)
    unit_price = Column(Float)
    channel = Column(String)


class Inventory(Base):
    __tablename__ = "fact_inventory"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String)
    store_id = Column(String)
    stock_qty = Column(Integer)
    last_update = Column(String)


class StockRisk(Base):
    __tablename__ = "fact_stock_risk"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String)
    store_id = Column(String)
    sales_7d = Column(Integer)
    sales_30d = Column(Integer)
    avg_rating = Column(Float)
    stock_qty = Column(Integer)
    category = Column(String)
    region = Column(String)
    web_signal = Column(Integer)
    stockout_risk = Column(Integer)


def create_tables():
    Base.metadata.create_all(engine)
    print("✔ Tables créées avec succès.")


if __name__ == "__main__":
    create_tables()