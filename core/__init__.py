from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_fixed, RetryError
from core.logs import logger
import requests
from bs4 import BeautifulSoup

