class OutputRecord:
    def __init__(self):
        self.search_engine_domain = ""
        self.search_keyword = ""
        self.revenue = 0.0

    def get_search_engine_domain(self):
        return self.search_engine_domain

    def get_search_keyword(self):
        return self.search_keyword

    def get_revenue(self):
        return self.revenue

    def set_search_engine_domain(self, search_engine_domain):
        self.search_engine_domain = search_engine_domain

    def set_search_keyword(self, search_keyword):
        self.search_keyword = search_keyword

    def set_revenue(self, revenue):
        self.revenue = revenue
