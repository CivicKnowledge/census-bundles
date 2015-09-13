# -*- coding: utf-8 -*-

# Scrapy settings for census project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'census'

SPIDER_MODULES = ['census.spiders']
NEWSPIDER_MODULE = 'census.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'census (+http://www.yourdomain.com)'
