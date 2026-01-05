from enum import Enum


class ColumnsToAnalyze(Enum):
    capitalization = "capitalization"
    price = "price"
    volume = "volume"
    average_price = "avg_price"
    average_volume = "avg_volume"
    average_capitalization = "avg_capitalization"
