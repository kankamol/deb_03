import csv

import scrapy
from scrapy.crawler import CrawlerProcess


URL = "https://ทองคําราคา.com/"


class MySpider(scrapy.Spider):
    name = "gold_price_spider"
    start_urls = [URL,]

    def parse(self, response):
        header = response.css("#divDaily h3::text").get().strip()
        print(header)

        table = response.css("#divDaily .pdtable")
        # print(table)

        rows = table.css("tr")
        # rows = table.xpath("//tr")
        # print(rows)
        # name of csv file 
        filename = "gold_self.csv"
    
        # writing to csv file 
        with open(filename, 'w') as csvfile:
            csvwriter = csv.writer(csvfile) 
            for row in rows:
                csvwriter.writerow(row.css("td::text").extract())
                #print(row.css("td::text").extract())
                # print(row.xpath("td//text()").extract())


if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(MySpider)
    process.start()
