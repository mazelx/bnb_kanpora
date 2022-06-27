#!/usr/bin/python3
# ============================================================================
# Tom Slee, 2013--2017.
#
# An ABListing represents and individual Airbnb listing
# ===========================================================================

from bnb_kanpora.http_requests import HTTPRequest
from bnb_kanpora.config import Config
from bnb_kanpora.models import RoomModel, SurveyModel, SearchAreaModel
from bnb_kanpora.db import DBUtils
from bnb_kanpora.utils import GeoBox, SearchResults, SurveyResults

import logging
import re
from lxml import html
import json
import time
import peewee
from playhouse.dataset import DataSet

from bnb_kanpora.views import ABSurveyViewer

logger = logging.getLogger()


class DatabaseController():
    """Control the underlying database

    Attributes:
    ---
        config: Config
            Configuration object
    """
    def __init__(self, config: Config) -> None:
        self.config = config

    ## DB
    def db_check_connection(self) -> bool:
        return DBUtils(self.config).check_connection()

    def create_tables(self) -> None:
        DBUtils(self.config).create_tables()

    def drop_tables(self) -> None:
        DBUtils(self.config).drop_tables()


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
    def  __init__(self, config:Config) -> None:
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
            print(f"Search area created: {search_area}")
            return search_area.search_area_id

        except Exception:
            print("Error adding search area to database")
            raise

    def delete(self, search_area_id:int) -> bool:
        SearchAreaModel.get_by_id(search_area_id).delete_instance() > 0


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

    def __init__(self, config:Config) -> None:
        # Set up logging
        logger.setLevel(config.log_level)

        self.search_node_counter = 0
        self.config = config
        self.request = HTTPRequest(config)
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
            survey:SurveyModel = SurveyModel.get_by_id(survey_id)
            search_area:SearchAreaModel = survey.search_area_id
            survey_results = self.search(search_area.geobox)
            survey_results = self.save_results(survey_results, survey_id)
            return survey_results

    def search(self, geobox:GeoBox, tree_idx:str = '0', survey_results:SurveyResults=SurveyResults()) -> SurveyResults:
        """Search for a geographical bounding box

        Keyword arguments:
        geobox:Geobox -- geographical bounding box
        survey_id:int -- survey id
        tree_idx:str -- Recursive index, root is 0, childs are 0-0, 0-1, 0-2, 0-3, childs of childs are 0-0-1, 0-0-2, ... and so on
        """
        results = self._search_box(geobox)
        
        survey_results.search_results[tree_idx] = results
        logger.info(f"{tree_idx} - {len(results.rooms)} on {results.nb_rooms_expected}")

        # need to split the box to search further (node on tree)
        # TODO get the expected nb rooms from first query and directly split the box
        if len(results.rooms) >= (self.config.SEARCH_MAX_PAGES * self.config.SEARCH_LISTINGS_ON_FULL_PAGE):
            # replace last box with 4-split 
            for idx, child_bbox in enumerate(geobox.get_four_splits(enlarge_pct=0)):
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
                    listing_id = SearchResultsController(self.config).parse_room_from_search_result(room, survey_id)
                    if listing_id:
                        nb_saved += 1  
        survey_results.total_nb_saved = nb_saved
        return survey_results

    def export(self, survey_ids:list[int], folder="export") -> str:
        path = f'{folder}/rooms_{"-".join(survey_ids)}.csv'
        db = DataSet(f'sqlite:///{self.config.database.database}')
        query = (RoomModel
         .select(
            SearchAreaModel.search_area_id, SearchAreaModel.name.alias("search_area_name"), RoomModel)
         .join(SurveyModel, peewee.JOIN.INNER)
         .join(SearchAreaModel,  peewee.JOIN.INNER)
         .where(SurveyModel.survey_id << survey_ids)
        )
        db.freeze(query, format='csv', filename=path)
        return path


class SearchResultsController():
    """Controls a search result
    
    Attributes:
    ---
        config: Config
            Configuration object

    Methods:
    ---
        parse_room_from_search_result(search_result:dict, survey_id:int) -> int
    """
    def __init__(self, config:Config) -> None:
        self.config = config
        """ """
        logger.setLevel(config.log_level)

    def parse_room_from_search_result(self, search_result:dict, survey_id:int) -> int :
        """
        Some fields occasionally extend beyond the varchar(255) limit.
        """

        key_mappings = {
            'room_id' : ['listing','id'],
            'room_type' : ['listing','room_type'],
            'host_id' : ['listing', 'user','id'],
            'address' : ['listing','public_address'],
            'reviews' : ['listing','reviews_count'],
            'overall_satisfaction' : ['listing','star_rating'],
            'accommodates' : ['listing','person_capacity'],
            'bedrooms' : ['listing','bedrooms'],
            'bathrooms' : ['listing','bathrooms'],
            'latitude' : ['listing','lat'],
            'longitude' : ['listing','lng'],
            'coworker_hosted' : ['listing','coworker_hosted'],
            'extra_host_languages' : ['listing','extra_host_languages'],
            'name' : ['listing','name'],
            'license' : ['listing','license'],
            'city' : ['listing','localized_city'],
            'picture_url' : ['listing','picture_url'],
            'neighborhood' : ['listing','neighborhood'],
            'pdp_type' : ['listing','pdp_type'],
            'pdp_url_type' : ['listing','pdp_url_type'],
            'rate' : ['pricing_quote','rate', 'amount'],
            'rate_with_service_fee' : ['pricing_quote','rate_with_service_fee', 'amount'],
            'currency' : ['pricing_quote', 'rate', 'currency'],
            'weekly_price_factor' : ['pricing_quote', 'weekly_price_factor'],
            'monthly_price_factor' : ['pricing_quote', 'monthly_price_factor'],
            'min_nights' : ['listing','min_nights'],
            'max_nights' : ['listing','max_nights]'],
        }

        def map_dict(source, mapping):
            dest = {}
            for k,v in mapping.items():
                if type(v) == str:
                    dest[k] = source.get(v)
                elif type(v) == list:
                    accessor = "source"
                    for i in range(0, len(v)):
                        accessor += f'.get(v[{i}], {{}})'
                    dest[k] = eval(accessor) or None
                else:
                    raise KeyError("Malformed mapping dict for room parsing")
            return dest

        room_dict = map_dict(search_result, key_mappings)
        room_dict['survey_id'] = str(survey_id)

        try:    
            room = RoomModel.create(**room_dict)
            return room.room_id
        except peewee.IntegrityError as e:
            logger.debug(f'Room with {room_dict.get("id")} already saved for this survey')
        except Exception as e:
            logger.info(f'Unknown error : {e}')
        return None


class ABListingExtraController():
    def __init__(self, config:Config) -> None:
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
            response = HTTPRequest
            (self.config).ws_request_with_repeats(room_url)
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