import scrapy

class Census(scrapy.Spider):
    name = "census"
    allowed_domains = ["http://www2.census.gov/"]
    start_urls = [
        "http://www2.census.gov/"
    ]

    def parse(self, response):
        filename = response.url.split("/")[-2]
        with open(filename, 'wb') as f:
            f.write(response.body)