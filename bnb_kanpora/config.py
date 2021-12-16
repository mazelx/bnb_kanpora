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
        self.URL_ROOT = "https://www.airbnb.com/"
        self.URL_ROOM_ROOT = self.URL_ROOT + "rooms/"
        self.URL_HOST_ROOT = self.URL_ROOT + "users/show/"
        self.URL_API_SEARCH_ROOT = self.URL_ROOT + "s/homes"
        self.SEARCH_LISTINGS_ON_FULL_PAGE = 18
        self.HTTP_PROXY_LIST = []
        self.GOOGLE_API_KEY = None
        self.AWS_KEY = None
        self.AWS_SECRET = None
        self.USE_ROTATING_IP = False
        
        try:
            config = configparser.ConfigParser()

            if self.config_file is None:
                # look for username.config on both Windows (USERNAME) and Linux (USER)
                self.config_file = "app.config"
            
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
                self.HTTP_PROXY_LIST = config["NETWORK"]["proxy_list"].split(",")
                self.HTTP_PROXY_LIST = [x.strip() for x in self.HTTP_PROXY_LIST]
                # Remove any empty strings from the list of proxies
                self.HTTP_PROXY_LIST = [x for x in self.HTTP_PROXY_LIST if x]
            except Exception:
                logger.warningf("No proxy_list in {self.config_file}: not using proxies")
                self.HTTP_PROXY_LIST = []

            try:
                self.USER_AGENT_LIST = config["NETWORK"]["user_agent_list"].split(",,")
                self.USER_AGENT_LIST = [x.strip() for x in self.USER_AGENT_LIST]
                self.USER_AGENT_LIST = [x.strip('"') for x in self.USER_AGENT_LIST]
            except Exception:
                logger.info(f"No user agent list in {config_file}: not using user-agents")
                self.USER_AGENT_LIST = []

            self.MAX_CONNECTION_ATTEMPTS = int(config["NETWORK"]["max_connection_attempts"])
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
            self.SEARCH_MAX_PAGES = int(config["SURVEY"]["search_max_pages"])
            self.SEARCH_MAX_GUESTS = int(config["SURVEY"]["search_max_guests"])
            self.RE_INIT_SLEEP_TIME = float(config["SURVEY"]["re_init_sleep_time"])

            # account
            try:
                self.GOOGLE_API_KEY = config["ACCOUNT"]["google_api_key"]
            except:
                logger.warning("Missing config file entry: Google API Key. Needed only for geocoding")
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