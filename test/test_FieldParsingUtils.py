import unittest
from SimplePipeline import FieldParsingUtils
from datetime import date


class Test(unittest.TestCase):
    def test_get_search_engine_domain_from_referrer(self):
        referrer = 'http://www.google.com/search?hl=en&client=firefox-a&rls=org.mozilla%3Aen-US%3Aofficial&hs=ZzP&q=Ipod&aq=f&oq=&aqi='
        self.assertEqual(FieldParsingUtils.get_search_engine_domain_from_referrer(referrer), 'www.google.com')
        referrer = 'https://www.google.com/search?hl=en&client=firefox-a&rls=org.mozilla%3Aen-US%3Aofficial&hs=ZzP&q=Ipod&aq=f&oq=&aqi='
        self.assertEqual(FieldParsingUtils.get_search_engine_domain_from_referrer(referrer), 'www.google.com')

    def test_get_search_keyword_from_referrer(self):
        referrer = 'http://search.yahoo.com/search?p=CD+Player&toggle=1&cop=mss&ei=UTF-8&fr=yfp-t-701'
        self.assertEqual(FieldParsingUtils.get_search_keyword_from_referrer(referrer), "cd player")

    def test_has_purchase_event(self):
        event_list = "2,1,12,10"
        self.assertEqual(FieldParsingUtils.has_purchase_event(event_list), True)
        event_list = ""
        self.assertEqual(FieldParsingUtils.has_purchase_event(event_list), False)
        event_list = "2,10,12"
        self.assertEqual(FieldParsingUtils.has_purchase_event(event_list), False)
        event_list = "1"
        self.assertEqual(FieldParsingUtils.has_purchase_event(event_list), True)

    def test_calculate_revenue_from_product_list(self):
        product_list = "Electronics;Ipod - Nano - 8GB;2;190;,Electronics;Ipod - Touch - 32GB;1;290;"
        self.assertEqual(FieldParsingUtils.calculate_revenue_from_product_list(product_list), 670.0)
        product_list = ""
        self.assertEqual(FieldParsingUtils.calculate_revenue_from_product_list(product_list), 0.0)

    def test_get_output_file_nm(self):
        base_folder = "/xyz"
        file_name = "SearchKeywordPerformance.tab"
        today = date.today().strftime("%Y-%m-%d")
        self.assertEqual(FieldParsingUtils.get_output_file_nm(base_folder, file_name), "/xyz/" + today + "_" + file_name)

    def test_is_external_search_engine(self):
        search_engine_domain = "search.yahoo.com"
        self.assertEqual(FieldParsingUtils.is_external_search_engine(search_engine_domain), True)
        search_engine_domain = "www.esshopzilla.com"
        self.assertEqual(FieldParsingUtils.is_external_search_engine(search_engine_domain), False)


if __name__ == '__main__':
    unittest.main()
    