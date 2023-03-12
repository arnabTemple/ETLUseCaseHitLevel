from enum import Enum


class Events(Enum):
    Purchase = 1
    Product_View = 2
    Shopping_Cart_Open = 10
    Shopping_Cart_Checkout = 11
    Shopping_Cart_Add = 12
    Shopping_Cart_Remove = 13
    Shopping_Cart_View = 14


external_search_engines = ["google.com",
                           "yahoo.com",
                           "bing.com"]

