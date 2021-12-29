#!/usr/bin/python3
"""
Functions to request data from the Airbnb web site, and to manage
a set of requests.

Tom Slee, 2013--2017.
"""
from json.decoder import JSONDecodeError
import logging
import random
import re
import requests
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bnb_kanpora.config import Config
from bnb_kanpora.utils import GeoBox, SearchResults

# Set up logging
logger = logging.getLogger()

LOW_INVENTORY_ENLARGE_FACTOR = 0.33

# requests retry strategy
MAX_RETRY_FOR_SESSION = 3
BACK_OFF_FACTOR = 0.2
TIME_BETWEEN_RETRIES = 1000

class HTTPRequest():

    def __init__(self, config:Config) -> None:
        self.config = config
        self.session = self._get_session()

    def get_params(self, geobox:GeoBox=None, room_type:str=None, items_offset:str=None, section_offset:str=None) -> dict:
        params = {}
        params["_format"] = "for_explore_search_web"
        params["_intents"] = "p1"
        params["adults"] = str(0)
        params["allow_override[]"] = ""
        params["auto_ib"] = str(False)
        params["children"] = str(0)
        params["client_session_id"] = self.config.CLIENT_SESSION_ID
        params["currency"] = "EUR"
        params["experiences_per_grid"] = str(20)
        params["federated_search_session_id"] = "45de42ea-60d4-49a9-9335-9e52789cd306"
        params["fetch_filters"] = str(True)
        params["guests"] = str(0)
        params["guidebooks_per_grid"] = str(20)
        params["has_zero_guest_treatment"] = str(True)
        params["infants"] = str(0)
        params["is_guided_search"] = str(True)
        params["is_new_cards_experiment"] = str(True)
        params["is_standard_search"] = str(True)
        #params["items_offset"] = str(18)
        params["items_per_grid"] = str(18)
        params["locale"] = "fr-FR"
        params["key"] = self.config.API_KEY
        params["luxury_pre_launch"] = str(False)
        params["metadata_only"] = str(False)
        # params["query"] = "Lisbon Portugal"
        params["query_understanding_enabled"] = str(True)
        params["refinement_paths[]"] = "/homes"
        params["search_type"] = "PAGINATION"
        params["search_by_map"] = str(True)
        params["selected_tab_id"] = "home_tab"
        params["show_groupings"] = str(True)
        params["supports_for_you_v3"] = str(True)
        params["timezone_offset"] = "-240"
        params["screen_size"] = "medium"
        params["zoom"] = str(True)
        params["treatmentFlags"] = str(["flex_destinations_june_2021_launch_web_treatment","new_filter_bar_v2_fm_header","merch_header_breakpoint_expansion_web","flexible_dates_12_month_lead_time","storefronts_nov23_2021_homepage_web_treatment","flexible_dates_options_extend_one_three_seven_days","super_date_flexibility","micro_flex_improvements","micro_flex_show_by_default","search_input_placeholder_phrases","pets_fee_treatment"])
        # params["version"] = "1.4.8"
        
        if geobox:
            params["ne_lat"] = geobox.n_lat
            params["ne_lng"] = geobox.e_lng
            params["sw_lat"] = geobox.s_lat
            params["sw_lng"] = geobox.w_lng
        
        if room_type:
            params["room_types[]"] = room_type
        
        if items_offset:
            params["items_offset"]   = str(items_offset)
            # params["items_offset"]   = str(18*items_offset)
            #params["section_offset"]   = str(8)

        return params

    def get_rooms_from_box(self, geobox:GeoBox, section_offset:int, items_offset:int) -> SearchResults:
        params = self.get_params(geobox=geobox, section_offset=section_offset, items_offset=items_offset)
        response = self.search_rooms(self.config.URL_API_SEARCH_ROOT, params)
        rooms = []
        if response:
            try:
                response_dict = json.loads(response.text)
                if len(response_dict['explore_tabs']) != 1:
                    raise KeyError('JSON Explore_tabs should only contain a single element')
                
                nb_rooms_expected = response_dict['explore_tabs'][0]['home_tab_metadata']['listings_count']

                if nb_rooms_expected > 0:
                    for response_section in response_dict['explore_tabs'][0]['sections']:
                        if response_section['section_type_uid'] == 'HOMES_LOW_INVENTORY_ZOOM_OUT':
                            return SearchResults() 
                        if response_section['section_type_uid'] == 'PAGINATED_HOMES':
                            rooms = response_section['listings']
            except KeyError as e:
                logger.warning(f"Unexpected JSON format : {e}")
                return SearchResults() 
            except JSONDecodeError as e:
                logger.warning(f"Parsing JSON from response failed: {e}")
                logger.warning(f"Reponse code : {response.status_code}, text: {response.text}")
                return SearchResults() 

            return SearchResults(
                nb_rooms_expected = nb_rooms_expected, 
                rooms = rooms, 
                geobox = geobox
                )

        # Bad Response
        return SearchResults()

    def _get_session(self):
        if len(self.config.USER_AGENT_LIST) > 0:
            user_agent = random.choice(self.config.USER_AGENT_LIST)
            headers = {"User-Agent": user_agent}
        else:
            headers = {'User-Agent': 'Mozilla/5.0'}

        # Now make the request
        # cookie to avoid auto-redirect
        cookies = dict(sticky_locale='en')
        session = requests.session()
        session.headers.update(headers)
        session.cookies.update(cookies)

        retry = Retry(total=MAX_RETRY_FOR_SESSION, read=MAX_RETRY_FOR_SESSION, connect=MAX_RETRY_FOR_SESSION,
                    backoff_factor=BACK_OFF_FACTOR,
                    method_whitelist=frozenset(['GET', 'POST']))
        adapter = HTTPAdapter(max_retries=retry)

        if self.config.HTTP_PROXY_LIST:
            http_proxy = random.choice(self.config.HTTP_PROXY_LIST)
            session.proxies = {
                'http': f'http://{http_proxy}',
                'https': f'http://{http_proxy}',
                }

        session.mount("http://www.airbnb.com", adapter)
        session.mount("https://www.airbnb.com", adapter)
        session.mount("https://ipinfo.io", adapter)
        return session

    def search_rooms(self, url, params=None):
        retry_attempts = 0
        while(retry_attempts < self.config.MAX_CONNECTION_ATTEMPTS):
            try:
                response = self.session.get(url=url, params=params, timeout=self.config.HTTP_TIMEOUT)
                if response.status_code == 200:
                    if len(response.text) > 0:
                        return response
                    else:
                        retry_attempts += 1
                        self.session = self._get_session()    
                elif response.status_code == 403:
                    logger.info(f"Access forbidden, will try to open a new connection... attempt {retry_attempts} on {self.config.MAX_CONNECTION_ATTEMPTS}")
                    retry_attempts += 1
                    self.session = self._get_session()
            except Exception as e:
                raise e
        return None

def get_public_ip(session):
    endpoint = 'https://ipinfo.io/json'
    response = session.get(endpoint, verify = True)

    if response.status_code != 200:
        return 'Status:', response.status_code, 'Problem with the request. Exiting.'
        exit()

    data = response.json()

    return data['ip']
