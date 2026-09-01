"""
EchoChain :: Web Scraping Infrastructure (Scrapy)
=====================================================
Spider template that crawls secondary-market electronics listings
(structured after eBay's laptop listing pages) and emits records in
the same schema as EchoChain_Raw_Data_Sets.csv, ready to land in the
Bronze layer of the Lakehouse.

Run with:
    scrapy runspider scrapy_secondary_market_spider.py -o listings.jsonl

Notes:
- Selectors below are illustrative placeholders (`.s-item__...` style
  classes are common on eBay-like listing grids) -- update them to match
  the live DOM of the target site, and always confirm scraping is
  permitted by the target site's robots.txt / terms of service before
  deploying a crawl.
- For production, run under Scrapy's AutoThrottle + a rotating proxy
  middleware, and schedule via Databricks Jobs or Airflow so Bronze
  data refreshes daily.
"""

import scrapy


class SecondaryMarketListingsSpider(scrapy.Spider):
    name = "secondary_market_listings"
    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "DOWNLOAD_DELAY": 1.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    start_urls = [
        "https://www.example-marketplace.com/laptops?category=notebooks-laptops",
    ]

    def parse(self, response):
        for item in response.css("li.listing-card"):
            yield {
                "Brand": item.css(".item-brand::text").get(),
                "Price": self._parse_price(item.css(".item-price::text").get()),
                "Currency": "$",
                "Color": item.css(".item-color::text").get(),
                "Features": ", ".join(item.css(".item-feature::text").getall()),
                "Condition": item.css(".item-condition::text").get(),
                "Condition Description": item.css(".item-condition-desc::text").get(),
                "Seller Note": item.css(".seller-note::text").get(),
                "GPU": item.css(".spec-gpu::text").get(),
                "Processor": item.css(".spec-cpu::text").get(),
                "Processor Speed": item.css(".spec-cpu-speed::attr(data-ghz)").get(),
                "Processor Speed Unit": "GHz",
                "Type": item.css(".spec-type::text").get() or "notebook/laptop",
                "Width of the Display": item.css(".spec-res-w::attr(data-px)").get(),
                "Height of the Display": item.css(".spec-res-h::attr(data-px)").get(),
                "OS": item.css(".spec-os::text").get(),
                "Storage Type": item.css(".spec-storage-type::text").get(),
                "Hard Drive Capacity": item.css(".spec-hdd::attr(data-cap)").get(),
                "Hard Drive Capacity Unit": item.css(".spec-hdd::attr(data-unit)").get(),
                "SSD Capacity": item.css(".spec-ssd::attr(data-cap)").get(),
                "SSD Capacity Unit": item.css(".spec-ssd::attr(data-unit)").get(),
                "Screen Size (inch)": item.css(".spec-screen::attr(data-inch)").get(),
                "Ram Size": item.css(".spec-ram::attr(data-size)").get(),
                "Ram Size Unit": item.css(".spec-ram::attr(data-unit)").get(),
                "listing_url": item.css("a.item-link::attr(href)").get(),
                "scraped_at": response.headers.get("Date", b"").decode(),
            }

        next_page = response.css("a.pagination-next::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    @staticmethod
    def _parse_price(raw):
        if not raw:
            return None
        return float(raw.replace("$", "").replace(",", "").strip())


# ---------------------------------------------------------------------
# A second spider (recommended) should separately crawl the site's
# "parts" / "components" category (screens, motherboards, batteries)
# to populate the component-level resale table used by the Circularity
# Score engine -- see 03_synthetic_component_resale_values.csv for the
# target schema this spider's output should match in production.
# ---------------------------------------------------------------------
class SecondaryMarketPartsSpider(scrapy.Spider):
    name = "secondary_market_parts"
    start_urls = [
        "https://www.example-marketplace.com/laptop-parts?category=replacement-parts",
    ]

    def parse(self, response):
        for item in response.css("li.listing-card"):
            yield {
                "component_type": item.css(".part-type::text").get(),  # e.g. "Display Panel"
                "compatible_brand": item.css(".part-brand::text").get(),
                "compatible_model_hint": item.css(".part-model::text").get(),
                "price": SecondaryMarketListingsSpider._parse_price(
                    item.css(".item-price::text").get()),
                "condition": item.css(".item-condition::text").get(),
                "listing_url": item.css("a.item-link::attr(href)").get(),
            }
