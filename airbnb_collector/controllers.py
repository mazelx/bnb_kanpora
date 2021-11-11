#!/usr/bin/python3
# ============================================================================
# Tom Slee, 2013--2017.
#
# An ABListing represents and individual Airbnb listing
# ===========================================================================

from airbnb_collector.requests import ABRequest
from airbnb_collector.config import ABConfig
from airbnb_collector.models import RoomModel, SurveyModel, SurveyProgressModel, SearchAreaModel, DBUtils
from airbnb_collector.requests import ABRequest

import logging
import re
from lxml import html
import json
import datetime
import time
from bs4 import BeautifulSoup
from peewee import fn

logger = logging.getLogger()

class ABDatabaseController():
    def __init__(self, config) -> None:
        self.config = config

    ## DB
    def db_check_connection(self) -> bool:
        DBUtils.check_connection()

    def create_tables(self) -> None:
        DBUtils.create_tables()

    def drop_tables(self) -> None:
        DBUtils.drop_tables()

class ABSearchAreaController():
    def  __init__(self, config) -> None:
        config.self = config

    ### Search Area
    def add(self, search_area_name:str, bb_e_lng:float, bb_s_lat:float, bb_w_lng:float, bb_n_lat:float) -> int:
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
                bb_n_lat = bb_n_lat,
                bb_s_lat = bb_s_lat,
                bb_e_lng = bb_e_lng,
                bb_w_lng = bb_w_lng
            )
            print(f"Search area created with following id : {search_area}")
            return search_area.search_area_id

        except Exception:
            print("Error adding search area to database")
            raise

    def delete(self, search_area_id:int) -> bool:
        SearchAreaModel.get(search_area_id).delete_instance() > 0


class ABSurveyController():
    """
    Class to represent a generic survey, using one of several methods.
    Specific surveys (eg bounding box, neighbourhood) are implemented in
    subclasses. Right now (May 2018), however, only the bounding box survey
    is working.
    """

    ROOM_TYPES = ["Private room", "Entire home/apt", "Shared room"]

    def __init__(self, config:ABConfig) -> None:
        # Set up logging
        logger.setLevel(config.log_level)

        self.search_node_counter = 0
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
        logfile = f"survey-{self.survey.survey_id}.log"
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

        logger.info("=" * 70)
        logger.info(f"Survey {self.survey.survey_id}, for {self.search_area.name}")
        
        survey = SurveyModel.get_by_id(survey_id)
        survey.survey_date = datetime.datetime.now()
        survey = self.config.SEARCH_BY_BOUNDING_BOX
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
        if self.logged_progress:
            logger.info("Restarting incomplete survey")
        if self.config.SEARCH_DO_LOOP_OVER_ROOM_TYPES:
            for room_type in self.ROOM_TYPES:
                logger.info("-" * 70)
                logger.info("Beginning of search for %s", room_type)
                self._search_quadtree(quadtree_node, room_type)
        else:
            self._search_quadtree(
                                quadtree_node=quadtree_node, 
                                room_type=None)
    
        try:
            self.survey.date = RoomModel.select(fn.Min(RoomModel.last_modified)).where(RoomModel.survey_id == self.survey.survey_id)
            self.survey.status = 1
            self.survey.save()
            return True
        except:
            logger.exception("Survey fini failed")
            return False

    def _search_quadtree(self, quadtree_node:list, room_type:str):
        """
        Recursive function to search for listings inside a rectangle.
        The actual search calls are done in search_node, and
        this method (_search_quadtree) prints output and sets up new
        rectangles, if necessary, for another round of searching.

        To match Airbnb's use of SW and NE corners, quadrants are divided
        like this:

                     [0, 1] (NW)   |   [0, 0] (NE)
                     -----------------------------
                     [1, 1] (SW)   |   [1, 0] (SE)

        The quadrants are searched in the order [0,0], [0,1], [1,0], [1,1]
        """
        zoomable = True
        if self._is_subtree_previously_completed(quadtree_node, room_type):
            logger.info("Resuming survey: subtree previously completed: %s", quadtree_node)
            # This node is part of a tree that has already been searched
            # completely in a previous attempt to run this survey.
            # Go immediately to the next quadrant at the current level,
            # or (if this is a [1, 1] node) go back up the tree one level.
            # For example: if quadtree_node is [0,0] and the logged
            # progress is [1,0] then the subtree for [0,0] is completed. If
            # progress is [0,0][0,1] then the subtree is not completed.
            # TODO: use the same technique as the loop, below
            if not quadtree_node:
                return
            if quadtree_node[-1] == [0, 0]:
                quadtree_node[-1] = [0, 1]
            elif quadtree_node[-1] == [0, 1]:
                quadtree_node[-1] = [1, 0]
            elif quadtree_node[-1] == [1, 0]:
                quadtree_node[-1] = [1, 1]
            elif quadtree_node[-1] == [1, 1]:
                del quadtree_node[-1]
            return

        # The subtree for this node has not been searched completely, so we
        # will continue to explore the tree. But does the current node need
        # to be searched? Only if it is at least as far down the tree as
        # the logged progress.
        # TODO Currently the most recent quadrant is searched again: this
        # is not a big problem.
        searchable_node = (
            self.logged_progress is None
            or len(quadtree_node) >= len(self.logged_progress["quadtree"]))
        if searchable_node:
            # The logged_progress can be set to None, as the survey is now
            # resumed. This should be done only once, but it is repeated.
            # Still, it is cheap.
            self.logged_progress = None
            zoomable = self._search_node(
                quadtree_node, room_type)
        else:
            logger.info("Resuming survey: node previously searched: %s", quadtree_node)

        # Recurse through the tree
        if zoomable:
            # and len(self.logged_progress["quadtree"]) >= len(quadtree_node)):
            # append a node to the quadtree for a new level
            quadtree_node.append([0,0])
            for int_leaf in range(4):
                # Loop over [0,0], [0,1], [1,0], [1,1]
                quadtree_leaf = [int(i)
                                    for i in str(bin(int_leaf))[2:].zfill(2)]
                quadtree_node[-1] = quadtree_leaf
                self._search_quadtree(quadtree_node, room_type)
            # the search of the quadtree below this node is complete:
            # remove the leaf element from the tree and return to go up a level
            if len(quadtree_node) > 0:
                del quadtree_node[-1]
        logger.debug("Returning from _search_quadtree for %s", quadtree_node)

    def _search_node(self, quadtree_node, room_type):
        """
            rectangle is (n_lat, e_lng, s_lat, w_lng)
            returns number of *new* rooms and number of pages tested
        """
        try:
            logger.info("-" * 70)
            rectangle = self._get_rectangle_from_quadtree_node(quadtree_node)
            logger.info(f"Searching rectangle: zoom factor = {len(quadtree_node)}, node = {str(quadtree_node)}")
            logger.debug(f"Rectangle: N={rectangle[0]:+.5f}, E={rectangle[1]:+.5f}, S={rectangle[2]:+.5f}, W={rectangle[3]:+.5f}")
            new_rooms = 0
            # set zoomable to false if the search finishes without returning a
            # full complement of 20 pages, 18 listings per page
            zoomable = True

            # As of October 2018, Airbnb uses items_offset in the URL for each new page,
            # which is the offset in the number of listings, rather than the
            # number of pages. Thanks to domatka78 for identifying the change.
            items_offset = 0
            room_count = 0
            for section_offset in range(0, self.config.SEARCH_MAX_PAGES):
                self.search_node_counter += 1
                # section_offset is the zero-based counter used on the site
                # page number is convenient for logging, etc
                page_number = section_offset + 1
                items_offset += room_count
                room_count = 0

                # TODO: use a class for query parameters
                if self.config.API_KEY:
                    # API (returns JSON)
                    # set up the parameters for the request
                    logger.debug("API key found: using API search at %s",
                                 self.config.URL_API_SEARCH_ROOT)
                    params = {}
                    params["_format"] = "for_explore_search_web"
                    params["_intents"] = "p1"
                    params["adults"] = str(0)
                    params["allow_override[]"] = ""
                    params["auto_ib"] = str(False)
                    params["children"] = str(0)
                    params["client_session_id"] = self.config.CLIENT_SESSION_ID
                    # params["currency"] = "CAD"
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
                    params["items_offset"] = str(18)
                    params["items_per_grid"] = str(18)
                    # params["locale"] = "en-CA"
                    params["key"] = self.config.API_KEY
                    params["luxury_pre_launch"] = str(False)
                    params["metadata_only"] = str(False)
                    # params["query"] = "Lisbon Portugal"
                    params["query_understanding_enabled"] = str(True)
                    params["refinement_paths[]"] = "/homes"
                    if self.config.SEARCH_DO_LOOP_OVER_ROOM_TYPES:
                        params["room_types[]"] = room_type
                    params["search_type"] = "PAGINATION"
                    params["search_by_map"] = str(True)
                    params["section_offset"] = section_offset
                    params["selected_tab_id"] = "home_tab"
                    params["show_groupings"] = str(True)
                    params["supports_for_you_v3"] = str(True)
                    params["timezone_offset"] = "-240"
                    params["ne_lat"] = str(rectangle[0])
                    params["ne_lng"] = str(rectangle[1])
                    params["sw_lat"] = str(rectangle[2])
                    params["sw_lng"] = str(rectangle[3])
                    params["screen_size"] = "medium"
                    params["zoom"] = str(True)
                    # params["version"] = "1.4.8"
                    if items_offset > 0:
                        params["items_offset"]   = str(items_offset)
                        # params["items_offset"]   = str(18*items_offset)
                        params["section_offset"]   = str(8)
                    # make the http request
                    response = ABRequest.ws_request_with_repeats(
                        self.config, self.config.URL_API_SEARCH_ROOT, params)
                    # process the response
                    if response:
                        json_doc = json.loads(response.text)
                    else:
                        # If no response, maybe it's a network problem rather
                        # than a lack of data.To be conservative go to the next page
                        # rather than the next rectangle
                        logger.warning(
                            "No response received from request despite multiple attempts: %s",
                            params)
                        continue
                else:
                    # Web page (returns HTML)
                    logger.debug("No API key found in config file: using web search at %s",
                                 self.config.URL_API_SEARCH_ROOT)
                    logger.warning("These results are probably wrong")
                    logger.warning("See README for how to set an API key")
                    params = {}
                    params["source"] = "filter"
                    params["_format"] = "for_explore_search_web"
                    params["experiences_per_grid"] = str(20)
                    params["items_per_grid"] = str(18)
                    params["guidebooks_per_grid"] = str(20)
                    params["auto_ib"] = str(True)
                    params["fetch_filters"] = str(True)
                    params["has_zero_guest_treatment"] = str(True)
                    params["is_guided_search"] = str(True)
                    params["is_new_cards_experiment"] = str(True)
                    params["luxury_pre_launch"] = str(False)
                    params["query_understanding_enabled"] = str(True)
                    params["show_groupings"] = str(True)
                    params["supports_for_you_v3"] = str(True)
                    params["timezone_offset"] = "-240"
                    params["metadata_only"] = str(False)
                    params["is_standard_search"] = str(True)
                    params["refinement_paths[]"] = "/homes"
                    params["selected_tab_id"] = "home_tab"
                    params["allow_override[]"] = ""
                    params["ne_lat"] = str(rectangle[0])
                    params["ne_lng"] = str(rectangle[1])
                    params["sw_lat"] = str(rectangle[2])
                    params["sw_lng"] = str(rectangle[3])
                    params["search_by_map"] = str(True)
                    params["screen_size"] = "medium"
                    if section_offset > 0:
                        params["section_offset"] = str(section_offset)
                    # make the http request
                    response = ABRequest.ws_request_with_repeats(
                        self.config, self.config.URL_API_SEARCH_ROOT, params)
                    # process the response
                    if not response:
                        # If no response, maybe it's a network problem rather
                        # than a lack of data. To be conservative go to the next page
                        # rather than the next rectangle
                        logger.warning(
                            "No response received from request despite multiple attempts: %s",
                            params)
                        continue
                    soup = BeautifulSoup(response.content.decode("utf-8",
                                                                 "ignore"),
                                         "lxml")
                    html_file = open("test.html", mode="w", encoding="utf-8")
                    html_file.write(soup.prettify())
                    html_file.close()
                    # The returned page includes a script tag that encloses a
                    # comment. The comment in turn includes a complex json
                    # structure as a string, which has the data we need
                    spaspabundlejs_set = soup.find_all("script",
                                                       {"type": "application/json",
                                                        "data-hypernova-key": "spaspabundlejs"})
                    if spaspabundlejs_set:
                        logger.debug("Found spaspabundlejs tag")
                        comment = spaspabundlejs_set[0].contents[0]
                        # strip out the comment tags (everything outside the
                        # outermost curly braces)
                        json_doc = json.loads(comment[comment.find("{"):comment.rfind("}")+1])
                        logger.debug("results-containing json found")
                    else:
                        logger.warning("json results-containing script node "
                                       "(spaspabundlejs) not found in the web page: "
                                       "go to next page")
                        return None


                # Searches for items with a given list of keys (in this case just one: "listing")
                # https://stackoverflow.com/questions/14048948/how-to-find-a-particular-json-value-by-key
                def search_json_keys(key, json_doc):
                    """ Return a list of the values for each occurrence of key
                    in json_doc, at all levels. In particular, "listings"
                    occurs more than once, and we need to get them all."""
                    found = []
                    
                    if isinstance(json_doc, dict):
                    
                        if key in json_doc.keys():
                            found.extend(json_doc[key])
                    
                        elif json_doc.keys():
                            for json_key in json_doc.keys():
                                result_list = search_json_keys(key, json_doc[json_key])
                                if result_list:
                                    found.extend(result_list)
                    
                    elif isinstance(json_doc, list):
                        for item in json_doc:
                            result_list = search_json_keys(key, item)
                            if result_list:
                                found.extend(result_list)
                    return found

                # Get all items with tags "listings". Each json_listings is a
                # list, and each json_listing is a {listing, pricing_quote, verified}
                # dict for the listing in question
                # There may be multiple lists of listings
                listing_dicts = search_json_keys("listings", json_doc)

                room_count = 0
                listings_controlers = ABListingController(self.config)
                for listing_dict in listing_dicts:
                    try:
                        listings_controlers.create_room_from_json(listing_dict, self.survey)
                        room_count += 1
                    except Exception as e:
                        logger.warning(f"Error inserting roomid= {json_doc.get('id')} : {str(e)}")

                # Log page-level results
                logger.info(f"Page {page_number:02d} returned {room_count:02d} listings")

                if room_count < self.config.SEARCH_LISTINGS_ON_FULL_PAGE:
                    # If a full page of listings is not returned by Airbnb,
                    # this branch of the search is complete.
                    logger.info("Final page of listings for this search")
                    zoomable = False
                    break
            # Log node-level results
            if self.config.SEARCH_DO_LOOP_OVER_ROOM_TYPES:
                logger.info("Results: %s pages, %s new %s listings.",
                            page_number, new_rooms, room_type)
            else:
                logger.info("Results: %s pages, %s new rooms",
                            page_number, new_rooms)

            # log progress
            self._log_progress(room_type, quadtree_node)
            return zoomable
        except UnicodeEncodeError:
            logger.error("UnicodeEncodeError: set PYTHONIOENCODING=utf-8")
            # if sys.version_info >= (3,):
            #    logger.info(s.encode('utf8').decode(sys.stdout.encoding))
            # else:
            #    logger.info(s.encode('utf8'))
            # unhandled at the moment
        except Exception:
            logger.exception("Exception in get_search_page_info_rectangle")
            raise

    def _log_progress(self, room_type, quadtree_node):
        try:
            SurveyProgressModel.create(room_type=room_type, quadtree_node=quadtree_node)
            return True
        except Exception as e:
            logger.warning("""Progress not logged: survey not affected, but
                    resume will not be available if survey is truncated.""")
            logger.exception("Exception in log_progress: {e}".format(e=type(e)))
            return False
    
    def _get_logged_progress(self):
        """
        Retrieve from the database the progress logged in previous attempts to
        carry out this survey, to pick up where we left off.
        Returns None if there is no progress logged.
        """
        progress_rows = SurveyProgressModel.select(
            SurveyProgressModel.room_type,
            SurveyProgressModel.quadtree_node
            ).where(SurveyProgressModel.survey_id == self.survey).get_or_none()
        if progress_rows is None:
            logger.debug("No progress logged for survey %s", self.survey.survey_id)
            self.logged_progress = None
        else:
            logged_progress = {}
            logged_progress["room_type"] = progress_rows.room_type
            logged_progress["quadtree"] = progress_rows.quadtree
            logger.info("Resuming survey - retrieved logged progress")
            logger.info("\troom_type=%s", logged_progress["room_type"])
            logger.info("\tquadtree node=%s", logged_progress["quadtree"])
            return logged_progress
    

    def _get_bounding_box(self):
        bounding_box = list(self.search_area.select(
            SearchAreaModel.bb_n_lat,
            SearchAreaModel.bb_e_lng,
            SearchAreaModel.bb_s_lat,
            SearchAreaModel.bb_w_lng
        ).scalar(as_tuple=True))

        # Validate the bounding box
        if None in bounding_box:
            raise Exception("Invalid bounding box: contains 'None'")
        if bounding_box[0] <= bounding_box[2]:
            raise Exception("Invalid bounding box: n_lat must be > s_lat")
        if bounding_box[1] <= bounding_box[3]:
            raise Exception("Invalid bounding box: e_lng must be > w_lng")

        return bounding_box

    def _get_rectangle_from_quadtree_node(self, quadtree_node):
        try:
            rectangle = self.bounding_box[0:4]
            for node in zip(quadtree_node):
                logger.debug("Quadtrees: %s", node)
                [n_lat, e_lng, s_lat, w_lng] = rectangle
                blur = abs(n_lat - s_lat) * self.config.SEARCH_RECTANGLE_EDGE_BLUR
                # find the mindpoints of the rectangle
                mid_lat = (n_lat + s_lat)/2.0
                mid_lng = (e_lng + w_lng)/2.0
                # overlap quadrants to ensure coverage at high zoom levels
                # Airbnb max zoom (18) is about 0.004 on a side.
                rectangle = []
                if node == [0, 0]: # NE
                    rectangle = [round(n_lat + blur, 5),
                                 round(e_lng + blur, 5),
                                 round(mid_lat - blur, 5),
                                 round(mid_lng - blur, 5),]
                elif node == [0, 1]: # NW
                    rectangle = [round(n_lat + blur, 5),
                                 round(mid_lng + blur, 5),
                                 round(mid_lat - blur, 5),
                                 round(w_lng - blur, 5),]
                elif node == [1, 0]: # SE
                    rectangle = [round(mid_lat + blur, 5),
                                 round(e_lng + blur, 5),
                                 round(s_lat - blur, 5),
                                 round(mid_lng - blur, 5),]
                elif node == [1, 1]: # SW
                    rectangle = [round(mid_lat + blur, 5),
                                 round(mid_lng + blur, 5),
                                 round(s_lat - blur, 5),
                                 round(w_lng - blur, 5),]
            logger.info("Rectangle calculated: %s", rectangle)
            return rectangle
        except:
            logger.exception("Exception in _get_rectangle_from_quadtree_node")
            return None

    def _is_subtree_previously_completed(self, quadtree_node, room_type):
        """
        Return True if the child subtree of this node was completed
        in a previous attempt at this survey.
        """
        subtree_previously_completed = False
        if self.logged_progress:
            # Compare the current node to the logged progress node by
            # converting into strings, then comparing the integer value.
            logger.debug("room_type=%s, self.logged_progress['room_type']=%s",
                             room_type, self.logged_progress["room_type"])

            if self.config.SEARCH_DO_LOOP_OVER_ROOM_TYPES == 1:
                if (self.ROOM_TYPES.index(room_type) < self.ROOM_TYPES.index(self.logged_progress["room_type"])):
                    subtree_previously_completed = True
                    return subtree_previously_completed
                if (self.ROOM_TYPES.index(room_type) > self.ROOM_TYPES.index(self.logged_progress["room_type"])):
                    subtree_previously_completed = False
                    return subtree_previously_completed

            common_length = min(len(quadtree_node), len(self.logged_progress["quadtree"]))
            s_this_quadrant = ''.join(str(quadtree_node[i][j])
                                      for j in range(0, 2)
                                      for i in range(0, common_length))
            s_logged_progress = ''.join(
                str(self.logged_progress["quadtree"][i][j])
                for j in range(0, 2)
                for i in range(0, common_length))
            if (s_this_quadrant != ""
                and int(s_this_quadrant) < int(s_logged_progress)):
                subtree_previously_completed = True

        return subtree_previously_completed

class ABListingController():
    """
    # ABListing represents an Airbnb room_id, as captured at a moment in time.
    # room_id, survey_id is the primary key.
    # Occasionally, a survey_id = None will happen, but for retrieving data
    # straight from the web site, and not stored in the database.
    """
    def __init__(self, config:ABConfig) -> None:
        self.config = config
        """ """
        logger.setLevel(config.log_level)

    def create_room_from_json(self, listing_dict:dict, survey_id:int) -> int :
        """
        Some fields occasionally extend beyond the varchar(255) limit.
        """
        room_dict = listing_dict["listing"]
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

        # pricing
        # TODO implement pricings
        #json_pricing = listing_dict["pricing_quote"]
        #room.price = json_pricing["rate"]["amount"] if "rate" in json_pricing else None
        #room.currency = json_pricing["rate"]["currency"] if "rate" in json_pricing else None
        #room.rate_type = json_pricing["rate_type"] if "rate_type" in json_pricing else None
        return room.room_id

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
                room = RoomModel.select().where(RoomModel.survey_id == survey_id).order_by(fn.Random()).limit(1).get()
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