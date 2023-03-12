from urllib.parse import urlparse
from urllib.parse import parse_qs
from SchemaManagement import Product, Events
from datetime import datetime


def get_search_engine_domain_from_referrer(referrer: str) -> str:
    parsed = urlparse(referrer)
    return str(parsed.netloc)


def get_search_keyword_from_referrer(referrer: str) -> str:
    parsed = urlparse(referrer)
    elements = parse_qs(parsed.query)
    if 'q' in elements:
        return ' '.join(elements['q']).lower()
    elif 'p' in elements:
        return ' '.join(elements['p']).lower()
    else:
        return ""


def has_purchase_event(event_list: str) -> bool:
    if Events.Events.Purchase.value in [int(evt) if evt.strip() else -999 for evt in str(event_list).split(",")]:
        return True
    else:
        return False


def calculate_revenue_from_product_list(product_list: str) -> float:
    products = [Product.Product(product) for product in str(product_list).split(",")]
    total_revenue = 0
    for product in products:
        total_revenue += product.total_revenue * product.number_of_items
    return total_revenue


def get_output_file_nm(base_folder: str, file_name: str) -> str:
    curr_dt = datetime.now().strftime('%Y-%m-%d')
    return base_folder + "/" + curr_dt + "_" + file_name


def is_external_search_engine(search_engine_domain: str) -> bool:
    for se in Events.external_search_engines:
        if se in search_engine_domain:
            return True
    return False
