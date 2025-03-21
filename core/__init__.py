from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_fixed, RetryError, wait_exponential
from core.logs import logger
import requests
from bs4 import BeautifulSoup
import re
import json
import csv
import os
import time
import random
