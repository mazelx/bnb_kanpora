#!/usr/bin/python3
# ============================================================================
# Tom Slee, 2013--2017.
#
# An ABListing represents and individual Airbnb listing
# ===========================================================================

from http_requests import ABRequest
from config import ABConfig
from models import RoomModel, SurveyModel, SurveyProgressModel, SearchAreaModel, DBUtils
from utils import GeoBox, SearchResults, SurveyResults

import logging
import re
from lxml import html
import json
import datetime
import time
import peewee

logger = logging.getLogger()


class DatabaseController():
    """Control the underlying database

    Attributes:
    ---
        config: Config
            Configuration object
    """
    def __init__(self, config: ABConfig) -> None:
        self.config = config

    ## DB
    def db_check_connection(self) -> bool:
        return DBUtils.check_connection()

    def create_tables(self) -> None:
        DBUtils.create_tables()

    def drop_tables(self) -> None:
        DBUtils.drop_tables()


class SearchAreaController():
    """Control a search area for search surveys

    Attributes:
    ---
        config: Config
            Configuration object

    Methods:
    ---
        add(search_area_name:str, geography:Geobox) -> int
        delete(search_area_id) -> bool
    """
    def  __init__(self, config:ABConfig) -> None:
        config.self = config

    ### Search Area
    def add(self, search_area_name:str, geobox:GeoBox) -> int:
        """
        Add a search_area to the database.
        """
        try:
            logging.info("Adding search_area to database as new search area")
            
            # Compute an abbreviation, which is optional and can be used
            # as a suffix for search_area views (based on a shapefile)
            # The abbreviation is lower case, has no whitespace, is 10 characters
            # or less, and does not end with a whitespace character
            # (translated as an underscore)
            
            abbreviation = search_area_name.lower()[:10].replace(" ", "_")
            while abbreviation[-1] == "_":
                abbreviation = abbreviation[:-1]
            
            search_area = SearchAreaModel.create(
                name = search_area_name,
                abbreviation = abbreviation,
                bb_n_lat = geobox.n_lat,
                bb_s_lat = geobox.s_lat,
                bb_e_lng = geobox.e_lng,
                bb_w_lng = geobox.w_lng
            )
            print(f"Search area created with following id : {search_area}")
            return search_area.search_area_id

        except Exception:
            print("Error adding search area to database")
            raise

    def delete(self, search_area_id:int) -> bool:
        SearchAreaModel.get(search_area_id).delete_instance() > 0


class SearchSurveyController():
    """Controls a search survey
    
    Attributes:
    ---
        config: Config
            Configuration object

    Methods:
    ---
        add(search_area_id) -> int
        delete(survey_id) -> bool
        search(geobox:GeoBox, survey_id:int) -> int
        search_box(geobox:GeoBox, survey_id:int) -> int
    """

    def __init__(self, config:ABConfig) -> None:
        # Set up logging
        logger.setLevel(config.log_level)

        self.search_node_counter = 0
        self.config = config
        self.request = ABRequest(config)
        #self.logged_progress = self._get_logged_progress()
        #self.bounding_box = self._get_bounding_box()
    
    def add(self, search_area_id:int) -> int:
        survey = SurveyModel.create(search_area_id = search_area_id)
        return survey.survey_id

    def delete(self, survey_id: int) -> bool:
        survey = SurveyModel.get_by_id(survey_id)
        rows_deleted = survey.delete_instance()
        return rows_deleted == 1

    def run(self, survey_id:int) -> int:
        """
        Initialize bounding box search.
        A bounding box is a rectangle around a city, specified in the
        search_area table. The loop goes to quadrants of the bounding box
        rectangle and, if new listings are found, breaks that rectangle
        into four quadrants and tries again, recursively.
        The rectangles, including the bounding box, are represented by
        [n_lat, e_lng, s_lat, w_lng], because Airbnb uses the SW and NE
        corners of the box.
        """
        
        # create a file handler
        logfile = f"survey-{survey_id}.log"
        filelog_handler = logging.FileHandler(logfile, encoding="utf-8")
        filelog_handler.setLevel(self.config.log_level)
        filelog_formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s%(message)s')
        filelog_handler.setFormatter(filelog_formatter)

        # logging: set log file name, format, and level
        logger.addHandler(filelog_handler)

        # Suppress informational logging from requests module
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logger.propagate = False

        survey = SurveyModel.get_by_id(survey_id)
        logger.info("=" * 70)
        logger.info(f"Survey {survey.survey_id}, for {survey.search_area_id.name}")
        
        survey.survey_date = datetime.datetime.now()
        survey.save()

        logger.info("Searching by bounding box, max_zoom=%s",
                    self.config.SEARCH_MAX_RECTANGLE_ZOOM)

        # Initialize search parameters
        # quadtree_node holds the quadtree: each rectangle is
        # divided into 00 | 01 | 10 | 11, and the next level down adds
        # set starting point
        quadtree_node = [] # list of [0,0] etc coordinates
        #median_node = [] # median lat, long to define optimal quadrants
        # set starting point for survey being resumed
        logged_progress = SurveyProgressModel.select().where(SurveyProgressModel.survey_id == survey_id)

        if logged_progress:
            logger.info("Restarting incomplete survey")
        
        for room_type in self.ROOM_TYPES:
            logger.info("-" * 70)
            logger.info("Beginning of search for %s", room_type)
            self.search()
            self._search_quadtree(quadtree_node, room_type)
    
        try:
            survey.date = RoomModel.select(peewee.fn.Min(RoomModel.last_modified)).where(RoomModel.survey_id == survey.survey_id)
            survey.status = 1
            survey.save()
            return True
        except:
            logger.exception("Survey run failed")
            return False

    def search(self, survey_id, tree_idx:str = '0', survey_results:SurveyResults=SurveyResults()) -> SurveyResults:
        """Search for a geographical bounding box

        Keyword arguments:
        geobox:Geobox -- geographical bounding box
        survey_id:int -- survey id
        tree_idx:str -- Recursive index, root is 0, childs are 0-0, 0-1, 0-2, 0-3, childs of childs are 0-0-1, 0-0-2, ... and so on
        """
        survey:SurveyModel = SurveyModel.get_by_id(survey_id)
        search_area:SearchAreaModel = survey.search_area_id
        results = self._search_box(search_area.geobox)
        
        survey_results.search_results[tree_idx] = results
        logger.info(f"{tree_idx} - {len(results.rooms)} on {results.nb_rooms_expected}")

        # need to split the box to search further (node on tree)
        # TODO get the expected nb rooms from first query and directly split the box
        if len(results.rooms) >= (self.config.SEARCH_MAX_PAGES * self.config.SEARCH_LISTINGS_ON_FULL_PAGE):
            # replace last box with 4-split 
            for idx, child_bbox in enumerate(search_area.geobox.get_four_splits(enlarge_pct=0)):
                self.search(geobox=child_bbox, 
                            tree_idx = f"{tree_idx}-{idx}", 
                            survey_results = survey_results)

        if tree_idx == '0':
            # last iteration of recursive search (root)
            return survey_results.get_uniques_search_results()

        return survey_results

    def _search_box(self, box:GeoBox) -> SearchResults:
        items_offset = 0
        results_acc = SearchResults()
        results_acc.geobox = box

        # iterate over pages
        for section_offset in range(0, self.config.SEARCH_MAX_PAGES):
            # TODO should probably get the value from response
            items_offset = section_offset * self.config.SEARCH_LISTINGS_ON_FULL_PAGE 

            results = self.request.get_rooms_from_box(box, section_offset, items_offset)
            results_acc.rooms.extend(results.rooms)
            results_acc.nb_rooms_expected = results.nb_rooms_expected

            if len(results.rooms) < self.config.SEARCH_LISTINGS_ON_FULL_PAGE:
                # If a full page of listings is not returned by Airbnb,
                # this branch of the search is complete.
                break

        return results_acc

    def save_results(self, survey_results:SurveyResults, survey_id:int) -> int:
        nb_saved = 0

        for tree_idx, search_results in survey_results.search_results.items():
            for room in search_results.rooms:
                room_id = int(room["listing"]["id"])
                if room_id is not None: 
                    listing_id = SearchResultsController(self.config).create_room_from_search_result(room, survey_id)
                    if listing_id:
                        nb_saved += 1  
        survey_results.total_nb_saved = nb_saved
        return survey_results

class SearchResultsController():
    """Controls a search result
    
    Attributes:
    ---
        config: Config
            Configuration object

    Methods:
    ---
        create_room_from_search_result(search_result:dict, survey_id:int) -> int
    """
    def __init__(self, config:ABConfig) -> None:
        self.config = config
        """ """
        logger.setLevel(config.log_level)

    def create_room_from_search_result(self, search_result:dict, survey_id:int) -> int :
        """
        Some fields occasionally extend beyond the varchar(255) limit.
        """
        room_dict = search_result["listing"]

        try:
            room = RoomModel.create(   
                room_id = room_dict.get("id"),
                room_type = room_dict.get("room_type"),
                host_id = room_dict.get("user").get("id") if room_dict.get("room_type") else None,
                address = room_dict.get("public_address"),
                reviews = room_dict.get("reviews_count"),
                overall_satisfaction = room_dict.get("star_rating"),
                accommodates = room_dict.get("person_capacity"),
                bedrooms = room_dict.get("bedrooms"),
                bathrooms = room_dict.get("bathrooms"),
                latitude = room_dict.get("lat"),
                longitude = room_dict.get("lng"),
                coworker_hosted = room_dict.get("coworker_hosted"),
                extra_host_languages = room_dict.get("extra_host_languages")[:254] if room_dict.get("extra_host_languages") else None,
                name = room_dict.get("name")[:254] if room_dict.get("name") else None,
                license = room_dict.get("license"),
                city = room_dict.get("localized_city") or room_dict.get("city"), # TODO check 'localized_city'
                picture_url = room_dict.get("picture_url"),
                neighborhood = room_dict.get("neighborhood"),
                survey_id = survey_id,
            )
            return room.room_id
        except peewee.IntegrityError as e:
            logger.debug(f'Room with {room_dict.get("id")} already saved for this survey')
        except Exception as e:
            logger.info(f'Unknown error : {e}')

        # pricing
        # TODO implement pricings
        #json_pricing = listing_dict["pricing_quote"]
        #room.price = json_pricing["rate"]["amount"] if "rate" in json_pricing else None
        #room.currency = json_pricing["rate"]["currency"] if "rate" in json_pricing else None
        #room.rate_type = json_pricing["rate_type"] if "rate_type" in json_pricing else None
        return None


class ABListingExtraController():
    def __init__(self, config:ABConfig) -> None:
        """ Get the room properties from the web site """
        self.config = config

    def fill_loop_by_room(self, survey_id):
        # TODO refacto
        """
        Master routine for looping over rooms (after a search)
        to fill in the properties.
        """
        room_count = 0
        while room_count < self.config.FILL_MAX_ROOM_COUNT:
            if not self.config.HTTP_PROXY_LIST:
                logging.info(
                    "No proxies left: re-initialize after %s seconds",
                    self.config.RE_INIT_SLEEP_TIME)
                time.sleep(self.config.RE_INIT_SLEEP_TIME)  # be nice
            room_count += 1
            room_id = self._get_room_to_fill(survey_id)
            if room_id is None:
                return None
            else:
                self.get_extras(room_id)

    def get_extras(self, survey_id:int, room_id:int) -> bool:
        try:
            # initialization
            logger.info("-" * 70)
            logger.info(f"Room {str(room_id)}: getting from Airbnb web site")
            
            room = RoomModel.get((RoomModel.room_id == room_id) & (RoomModel.survey_id == survey_id))

            room_url = self.config.URL_ROOM_ROOT + str(room.room_id)
            response = ABRequest(self.config).ws_request_with_repeats(room_url)
            if response is not None:
                page = response.text
                tree = html.fromstring(page)
                self.__get_room_info_from_tree(survey_id, room_id, tree)
                logger.info("Room %s: found", room_id)
                return True
            else:
                logger.info("Room %s: not found", room_id)
                return False
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as ex:
            logger.exception("Room " + str(room_id) +
                             ": failed to retrieve from web site.")
            logger.error("Exception: " + str(type(ex)))
            raise

    def _get_room_to_fill(self, survey_id:int) -> int:
        # TODO refacto
        """
        For "fill" runs (loops over room pages), choose a random room that has
        not yet been visited in this "fill".
        """
        for attempt in range(self.config.MAX_CONNECTION_ATTEMPTS):
            try:
                room = RoomModel.select().where(RoomModel.survey_id == survey_id).order_by(peewee.fn.Random()).limit(1).get()
                return room.room_id
            except TypeError as e:
                # TODO should test nbrows
                logging.info("Finishing: no unfilled rooms in database --")
                return None
            except Exception as e:
                logging.exception(f"Error retrieving room to fill from db: {str(e)}")
        return None
    
    def __get_country(self, tree):
        temp = tree.xpath(
            "//meta[contains(@property,'airbedandbreakfast:country')]"
            "/@content"
            )
        if len(temp) > 0:
            return temp[0]

    def __get_city(self, tree):
        temp = tree.xpath(
            "//meta[contains(@property,'airbedandbreakfast:city')]"
            "/@content"
            )
        if len(temp) > 0:
            return temp[0]

    def __get_rating(self, tree):
        s = tree.xpath("//meta[@id='_bootstrap-listing']/@content")
        temp = tree.xpath(
            "//meta[contains(@property,'airbedandbreakfast:rating')]"
            "/@content"
            )
        if len(s)>0:
            j = json.loads(s[0])
            return j["listing"]["star_rating"]
        elif len(temp) > 0:
            return temp[0]

    def __get_latitude(self, tree):
        temp = tree.xpath("//meta"
                            "[contains(@property,"
                            "'airbedandbreakfast:location:latitude')]"
                            "/@content")
        if len(temp) > 0:
            return temp[0]

    def __get_longitude(self, tree):
        temp = tree.xpath(
            "//meta"
            "[contains(@property,'airbedandbreakfast:location:longitude')]"
            "/@content")
        if len(temp) > 0:
            return temp[0]

    def __get_host_id(self, tree):
        s = tree.xpath("//meta[@id='_bootstrap-listing']/@content")
        temp = tree.xpath(
            "//div[@id='host-profile']"
            "//a[contains(@href,'/users/show')]"
            "/@href"
        )
        if len(s)>0:
            j = json.loads(s[0])
            return j["listing"]["user"]["id"]
        elif len(temp) > 0:
            host_id_element = temp[0]
            host_id_offset = len('/users/show/')
            return int(host_id_element[host_id_offset:])
        else:
            temp = tree.xpath(
                "//div[@id='user']"
                "//a[contains(@href,'/users/show')]"
                "/@href")
            if len(temp) > 0:
                host_id_element = temp[0]
                host_id_offset = len('/users/show/')
                return int(host_id_element[host_id_offset:])

    def __get_room_type(self, tree):
        temp = tree.xpath(
            "//div[@class='col-md-6']"
            "/div/span[text()[contains(.,'Room type:')]]"
            "/../strong/text()"
            )
        if len(temp) > 0:
            return temp[0].strip()
        else:
            # new page format 2014-12-26
            temp_entire = tree.xpath(
                "//div[@id='summary']"
                "//i[contains(concat(' ', @class, ' '),"
                " ' icon-entire-place ')]"
                )
            if len(temp_entire) > 0:
                return "Entire home/apt"
            temp_private = tree.xpath(
                "//div[@id='summary']"
                "//i[contains(concat(' ', @class, ' '),"
                " ' icon-private-room ')]"
                )
            if len(temp_private) > 0:
                return "Private room"
            temp_shared = tree.xpath(
                "//div[@id='summary']"
                "//i[contains(concat(' ', @class, ' '),"
                " ' icon-shared-room ')]"
                )
            if len(temp_shared) > 0:
                return "Shared room"

    def __get_neighborhood(self, tree):
        temp2 = tree.xpath(
            "//div[contains(@class,'rich-toggle')]/@data-address"
            )
        temp1 = tree.xpath("//table[@id='description_details']"
                            "//td[text()[contains(.,'Neighborhood:')]]"
                            "/following-sibling::td/descendant::text()")
        if len(temp2) > 0:
            temp = temp2[0].strip()
            return temp[temp.find("(")+1:temp.find(")")]
        elif len(temp1) > 0:
            return temp1[0].strip()

    def __get_address(self, tree):
        temp = tree.xpath(
            "//div[contains(@class,'rich-toggle')]/@data-address"
            )
        if len(temp) > 0:
            temp = temp[0].strip()
            return temp[:temp.find(",")]
        else:
            # try old page match
            temp = tree.xpath(
                "//span[@id='display-address']"
                "/@data-location"
                )
            if len(temp) > 0:
                return temp[0]

    def __get_reviews(self, tree):
        # 2016-04-10
        s = tree.xpath("//meta[@id='_bootstrap-listing']/@content")
        # 2015-10-02
        temp2 = tree.xpath(
            "//div[@class='___iso-state___p3summarybundlejs']"
            "/@data-state"
            )
        reviews = None
        if len(s) > 0:
            j = json.loads(s[0])
            reviews = \
                j["listing"]["review_details_interface"]["review_count"]
        elif len(temp2) == 1:
            summary = json.loads(temp2[0])
            reviews = summary["visibleReviewCount"]
        elif len(temp2) == 0:
            temp = tree.xpath(
                "//div[@id='room']/div[@id='reviews']//h4/text()")
            if len(temp) > 0:
                reviews = temp[0].strip()
                reviews = str(reviews).split('+')[0]
                reviews = str(reviews).split(' ')[0].strip()
            if reviews == "No":
                reviews = 0
        else:
            # try old page match
            temp = tree.xpath(
                "//span[@itemprop='reviewCount']/text()"
                )
            if len(temp) > 0:
                reviews = temp[0]
        if reviews is not None:
            reviews = int(reviews)
        return reviews

    def __get_bedrooms(self, tree):
        temp = tree.xpath(
            "//div[@class='col-md-6']"
            "/div/span[text()[contains(.,'Bedrooms:')]]"
            "/../strong/text()"
            )
        if len(temp) > 0:
            bedrooms = temp[0].strip()
        else:
            temp = tree.xpath(
                "//div[@class='col-md-6']"
                "/div[text()[contains(.,'Bedrooms:')]]"
                "/strong/text()"
                )
            if len(temp) > 0:
                bedrooms = temp[0].strip()
        if bedrooms:
            bedrooms = bedrooms.split('+')[0]
            bedrooms = bedrooms.split(' ')[0]
        bedrooms = float(bedrooms)
    
        return bedrooms

    def __get_bathrooms(self, tree):
        temp = tree.xpath(
            "//div[@class='col-md-6']"
            "/div/span[text()[contains(.,'Bathrooms:')]]"
            "/../strong/text()"
            )
        if len(temp) > 0:
            bathrooms = temp[0].strip()
        else:
            temp = tree.xpath(
                "//div[@class='col-md-6']"
                "/div/span[text()[contains(.,'Bathrooms:')]]"
                "/../strong/text()"
                )
            if len(temp) > 0:
                bathrooms = temp[0].strip()
        if bathrooms:
            bathrooms = bathrooms.split('+')[0]
            bathrooms = bathrooms.split(' ')[0]
        bathrooms = float(bathrooms)
        
        return bathrooms

    def __get_minstay(self, tree):
        # -- minimum stay --
        temp3 = tree.xpath(
            "//div[contains(@class,'col-md-6')"
            "and text()[contains(.,'minimum stay')]]"
            "/strong/text()"
            )
        temp2 = tree.xpath(
            "//div[@id='details-column']"
            "//div[contains(text(),'Minimum Stay:')]"
            "/strong/text()"
            )
        temp1 = tree.xpath(
            "//table[@id='description_details']"
            "//td[text()[contains(.,'Minimum Stay:')]]"
            "/following-sibling::td/descendant::text()"
            )
        if len(temp3) > 0:
            minstay = temp3[0].strip()
        elif len(temp2) > 0:
            minstay = temp2[0].strip()
        elif len(temp1) > 0:
            minstay = temp1[0].strip()
        if minstay is not None:
            minstay = minstay.split('+')[0]
            minstay = minstay.split(' ')[0]
        minstay = int(minstay)
    
        return minstay

    def __get_price(self, tree):
        temp2 = tree.xpath(
            "//meta[@itemprop='price']/@content"
            )
        temp1 = tree.xpath(
            "//div[@id='price_amount']/text()"
            )
        if len(temp2) > 0:
            price = temp2[0]
        elif len(temp1) > 0:
            price = temp1[0][1:]
            non_decimal = re.compile(r'[^\d.]+')
            price = non_decimal.sub('', price)
        # Now find out if it's per night or per month
        # (see if the per_night div is hidden)
        per_month = tree.xpath(
            "//div[@class='js-per-night book-it__payment-period  hide']")
        if per_month:
            price = int(int(price) / 30)
        price = int(price)
        
        return price
        
    def __get_room_info_from_tree(self, survey_id, room_id, tree):
        try:
            # Some of these items do not appear on every page (eg,
            # ratings, bathrooms), and so their absence is marked with
            # logger.info. Others should be present for every room (eg,
            # latitude, room_type, host_id) and so are marked with a
            # warning.  Items coded in <meta
            # property="airbedandbreakfast:*> elements -- country --
            
            query = RoomModel.update(
                price = self.__get_price(tree),
            ).where(
                (RoomModel.room_id == room_id) & (RoomModel.survey_id == survey_id)
            )
            query.execute()

            # NOT FILLING HERE, but maybe should? have to write helper methods:
            # coworker_hosted, extra_host_languages, name,
            #    property_type, currency, rate_type
            
        except (KeyboardInterrupt, SystemExit):
            raise
        except IndexError:
            logger.exception("Web page has unexpected structure.")
            raise
        except UnicodeEncodeError as uee:
            logger.exception("UnicodeEncodeError Exception at " +
                             str(uee.object[uee.start:uee.end]))
            raise
        except AttributeError:
            logger.exception("AttributeError")
            raise
        except TypeError:
            logger.exception("TypeError parsing web page.")
            raise
        except Exception:
            logger.exception("Error parsing web page.")
            raise