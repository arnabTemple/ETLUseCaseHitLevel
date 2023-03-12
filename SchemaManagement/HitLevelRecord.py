import Product


class HitLevelRecord:
    def __init__(self, in_record):
        try:
            self.hit_time_gmt = int(in_record[0])
            self.date_time = str(in_record[1])
            self.user_agent = str(in_record[2])
            self.ip = str(in_record[3])
            self.event_list = str(in_record[4]).split(",")
            self.geo_city = str(in_record[5])
            self.geo_region = str(in_record[6])
            self.geo_country = str(in_record[7])
            self.pagename = str(in_record[8])
            self.page_url = str(in_record[9])
            product_list_string = str(in_record[10]).split(",")
            self.product_list = []
            for product_string in product_list_string:
                self.product_list.append(Product.Product(product_string))
            self.referrer = in_record[11]
        except Exception:
            print("Failure parsing record: " + str(in_record))
            raise
