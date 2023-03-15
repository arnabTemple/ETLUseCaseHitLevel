import pandas as pd
from SimplePipeline.FieldParsingUtils import *


class ProcessHitLevelFile:
    fileName = "SearchKeywordPerformance.tab"

    def __init__(self, in_file):
        usecols = {"hit_time_gmt": int,
                   "date_time": str,
                   "user_agent": str,
                   "ip": str,
                   "event_list": str,
                   "geo_city": str,
                   "geo_region": str,
                   "geo_country": str,
                   "pagename": str,
                   "page_url": str,
                   "product_list": str,
                   "referrer": str}
        self.df = pd.read_csv(in_file, sep='\t', header=0, dtype=usecols).fillna('')

    def process(self):
        df = self.df.copy()
        df.loc[:, "hasPurchase"] = df["event_list"].apply(has_purchase_event)
        df.loc[:, "isExternalSearch"] = df["referrer"].apply(is_external_search_engine)
        filtered1 = df[(df["hasPurchase"])]
        filtered_pur = filtered1[["ip", "product_list"]]
        filtered2 = df[(df["isExternalSearch"])]
        filtered_ext = filtered2[["ip", "referrer"]]
        joined = filtered_pur.merge(filtered_ext, left_on='ip', right_on='ip', how="inner")
        joined.loc[:, ["search_engine_domain"]] = joined["referrer"].apply(get_search_engine_domain_from_referrer)
        joined.loc[:, ["search_keyword"]] = joined["referrer"].apply(get_search_keyword_from_referrer)
        joined.loc[:, "revenue"] = joined["product_list"].apply(calculate_revenue_from_product_list)
        grouped = joined.groupby(["search_engine_domain", "search_keyword"], as_index=False)["revenue"].sum()
        if grouped.empty:
            self.df = pd.DataFrame({'search_engine_domain': [],
                                    'search_keyword': [],
                                    'revenue': []})
        else:
            self.df = grouped.sort_values(by="revenue", ascending=False)

    def output(self, output_base_path):
        file_name = get_output_file_nm(output_base_path, self.fileName)
        self.df.to_csv(file_name, sep='\t', header=True, index=False)
