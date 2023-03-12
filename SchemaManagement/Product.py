class Product:
    def __init__(self, product_string):
        self.number_of_items = 0
        self.total_revenue = 0.0
        try:
            product = product_string.split(";")
            self.category = str(product[0])
            self.product_name = str(product[1])
            self.number_of_items = int(product[2]) if product[2].isnumeric() else 0
            self.total_revenue = float(product[3]) if product[3].isnumeric() else 0.0
            self.custom_event = str(product[4])
            self.merchandising_evar = str(product[5])
        except IndexError:
            pass
        except Exception:
            raise
