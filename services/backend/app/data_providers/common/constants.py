from datetime import date
import json
import os

script_path = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(script_path, "database-futures.json"), "r") as f:
    FUTURES = json.load(f)

FUTURE_TYPE = "future"

LETTERS = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]

LIBOR_BEFORE_2001 = 6.65125

START_DATE = date(2000, 1, 1)
