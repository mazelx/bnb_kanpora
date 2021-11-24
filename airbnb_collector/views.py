#!/usr/bin/python3
# ============================================================================
# Tom Slee, 2013--2017.
#
# An ABListing represents and individual Airbnb listing
# ===========================================================================

from models import RoomModel, SurveyModel, SearchAreaModel

class ABDatabaseViewer():
    pass

class ABSearchAreaViewer():
   def print_search_areas():
       for row in SearchAreaModel.select().order_by(SearchAreaModel.search_area_id):
           print(row)

class ABSurveyViewer():
   def print_search_areas():
       for row in SurveyModel.select().order_by(SurveyModel.survey_id):
           print(row)

class ABRoomViewer():
   def print_search_areas():
       for row in RoomModel.select().order_by(RoomModel.survey_id):
           print(row)

