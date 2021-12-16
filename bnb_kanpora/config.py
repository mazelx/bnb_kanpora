#!/usr/bin/python3
# ============================================================================
# Airbnb Configuration module, for use in web scraping and analytics
# ============================================================================
import logging
import os
import configparser
import sys
from bnb_kanpora.models import RoomModel, SurveyModel, SearchAreaModel, SurveyProgressModel
from peewee import PostgresqlDatabase

MODELS = [RoomModel, SurveyModel, SearchAreaModel, SurveyProgressModel]

logger = logging.getLogger()

class Config():

    def __init__(self, config_file=None, verbose=False):
        """ Read the configuration file <username>.config to set up the run
        """
        self.config_file = config_file
        self.log_level = logging.DEBUG if verbose else logging.INFO
        self.connection = None
        #self.FLAGS_ADD = 1
        #self.FLAGS_PRINT = 9
        #self.FLAGS_INSERT_REPLACE = True
        #self.FLAGS_INSERT_NO_REPLACE = False
        self.URL_ROOT = "https://www.airbnb.com/"
        self.URL_ROOM_ROOT = self.URL_ROOT + "rooms/"
        self.URL_HOST_ROOT = self.URL_ROOT + "users/show/"
        # self.URL_API_SEARCH_ROOT = self.URL_ROOT + "search/search_results"
        self.URL_API_SEARCH_ROOT = self.URL_ROOT + "s/homes"
        self.SEARCH_AREA_GLOBAL = "UNKNOWN"  # special case: sample listings globally
        # self.SEARCH_RECTANGLE_EDGE_BLUR = 0.1
        self.SEARCH_RECTANGLE_EDGE_BLUR = 0.0
        self.SEARCH_BY_NEIGHBORHOOD = 'neighborhood'  # default
        self.SEARCH_BY_ZIPCODE = 'zipcode'
        self.SEARCH_BY_BOUNDING_BOX = 'bounding box'
        self.SEARCH_LISTINGS_ON_FULL_PAGE = 18
        self.SEARCH_DO_LOOP_OVER_PRICES = False
        self.HTTP_PROXY_LIST = []
        self.HTTP_PROXY_LIST_COMPLETE = []
        self.GOOGLE_API_KEY = None
        self.AWS_KEY = None
        self.AWS_SECRET = None
        self.USE_ROTATING_IP = False
        
        try:
            config = configparser.ConfigParser()

            if self.config_file is None:
                # look for username.config on both Windows (USERNAME) and Linux (USER)
                if os.name == "nt":
                    username = os.environ['USERNAME']
                else:
                    username = os.environ['USER']
                self.config_file = username + ".config"
            logging.info("Reading configuration file %s", self.config_file)
            if not os.path.isfile(self.config_file):
                logging.error("Configuration file %s not found.", self.config_file)
                sys.exit()
            config.read(self.config_file)

            # database
            try:
                self.database =  PostgresqlDatabase(
                    config["DATABASE"]["db_name"], 
                    user=config["DATABASE"]["db_user"], 
                    password=config["DATABASE"]["db_password"],
                    host=config["DATABASE"]["db_host"] if ("db_host" in config["DATABASE"]) else None, 
                    port=config["DATABASE"]["db_port"],
                    autorollback=True
                )
                self.database.bind(MODELS)
                self.database.connect()
                self.database.create_tables(MODELS)
            except Exception:
                logger.error("Incomplete database information in %s: cannot continue",
                             self.config_file)
                sys.exit()

            # network
            try:
                self.USE_ROTATING_IP = config["NETWORK"].getboolean("rotating_ip_proxy")
            except Exception:
                logger.warning("Rotating IP has not been activated")

            if not(self.USE_ROTATING_IP):
                try:
                    self.HTTP_PROXY_LIST = config["NETWORK"]["proxy_list"].split(",")
                    self.HTTP_PROXY_LIST = [x.strip() for x in self.HTTP_PROXY_LIST]
                    # Remove any empty strings from the list of proxies
                    self.HTTP_PROXY_LIST = [x for x in self.HTTP_PROXY_LIST if x]
                except Exception:
                    logger.warningf("No proxy_list in {self.config_file}: not using proxies")
                    self.HTTP_PROXY_LIST = []
                self.HTTP_PROXY_LIST_COMPLETE = list(self.HTTP_PROXY_LIST)
                logger.info(f"Complete proxy list has {len(self.HTTP_PROXY_LIST_COMPLETE)} proxies")
            try:
                self.USER_AGENT_LIST = config["NETWORK"]["user_agent_list"].split(",,")
                self.USER_AGENT_LIST = [x.strip() for x in self.USER_AGENT_LIST]
                self.USER_AGENT_LIST = [x.strip('"') for x in self.USER_AGENT_LIST]
            except Exception:
                logger.info("No user agent list in " + username +
                             ".config: not using user agents")
                self.USER_AGENT_LIST = []
            self.MAX_CONNECTION_ATTEMPTS = \
                int(config["NETWORK"]["max_connection_attempts"])
            self.REQUEST_SLEEP = float(config["NETWORK"]["request_sleep"])
            self.HTTP_TIMEOUT = float(config["NETWORK"]["http_timeout"])
            try:
                self.URL_API_SEARCH_ROOT = config["NETWORK"]["url_api_search_root"]
            except: 
                logger.warning("Missing config file entry: url_api_search_root.")
                logger.warning("For more information, see example.config")
                self.URL_API_SEARCH_ROOT = self.URL_ROOT + "s/homes"
            try:
                self.API_KEY = config["NETWORK"]["api_key"]
            except: 
                logger.warning("Missing config file entry: api_key.")
                logger.warning("For more information, see example.config")
                self.API_KEY = None
            if self.API_KEY is None or self.API_KEY=="":
                self.URL_API_SEARCH_ROOT = self.URL_ROOT + "s/homes"
            try:
                self.CLIENT_SESSION_ID = config["NETWORK"]["client_session_id"]
            except: 
                logger.warning("Missing config file entry: client_session_id.")
                logger.warning("For more information, see example.config")
                self.CLIENT_SESSION_ID = None

            # survey
            self.FILL_MAX_ROOM_COUNT = int(config["SURVEY"]["fill_max_room_count"])
            self.ROOM_ID_UPPER_BOUND = int(config["SURVEY"]["room_id_upper_bound"])
            self.SEARCH_MAX_PAGES = int(config["SURVEY"]["search_max_pages"])
            self.SEARCH_MAX_GUESTS = int(config["SURVEY"]["search_max_guests"])
            self.SEARCH_MAX_RECTANGLE_ZOOM = int(
                config["SURVEY"]["search_max_rectangle_zoom"])
            try:
                self.SEARCH_DO_LOOP_OVER_PRICES = int(
                    config["SURVEY"]["search_do_loop_over_prices"])
            except:
                logger.warning(
                    "Missing config file entry: search_do_loop_over_prices.")
                logger.warning("For more information, see example.config")
            self.RE_INIT_SLEEP_TIME = float(config["SURVEY"]["re_init_sleep_time"])
            try:
                self.SEARCH_RECTANGLE_EDGE_BLUR = float(
                    config["SURVEY"]["search_rectangle_edge_blur"])
            except:
                logger.warning(
                    "Missing config file entry: search_rectangle_edge_blur.")
                logger.warning("For more information, see example.config")

            # account
            try:
                self.GOOGLE_API_KEY = config["ACCOUNT"]["google_api_key"]
            except:
                logger.warning(
                    "Missing config file entry: Google API Key. Needed only for geocoding")
                logger.warning("For more information, see example.config")

            try: 
                self.AWS_KEY = config["ACCOUNT"]["aws_key"]
                self.AWS_SECRET = config["ACCOUNT"]["aws_secret"]
            except:
                logger.warning(
                    "Missing config file entry: AWS API Key. Needed only for proxies")
                logger.warning("For more information, see example.config")

        except Exception:
            logger.exception("Failed to read config file properly")
            raise