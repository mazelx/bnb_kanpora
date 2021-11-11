from airbnb_collector.controllers import *
from airbnb_collector.config import ABConfig
from airbnb_collector.models import *

import pytest
import json
import peewee

# Tests should be improved to cover edge cases

@pytest.fixture
def config():
    return ABConfig(config_file="test.config", verbose=True)

@pytest.fixture
def json_sample():
    with open('airbnb_collector/test/test_room_sample.json', 'r') as f:
        data = json.load(f)
    return data

@pytest.fixture
def init(config) -> None:
    db = ABDatabaseController(config)
    db.drop_tables()
    db.create_tables()

# search area
@pytest.fixture
def search_area_controller(config):
    return ABSearchAreaController(config)

@pytest.fixture
def search_area(search_area_controller):
    return search_area_controller.add(
        "Mazamet Centre",    
        bb_n_lat = 43.500005,
        bb_e_lng = 2.380265,
        bb_s_lat = 43.486680,
        bb_w_lng = 2.360009
    )

def test_add_search_area(init, search_area:int):
    assert isinstance(search_area, int)
    assert search_area > 0

def test_delete_search_area(init, search_area_controller:ABSearchAreaController, search_area:int):
    search_area_controller.delete(search_area)
    with pytest.raises(peewee.DoesNotExist):
        assert SearchAreaModel.get_by_id(search_area)

# survey
@pytest.fixture
def survey_controller(config):
    return ABSurveyController(config)

@pytest.fixture
def survey(survey_controller:ABSurveyController, search_area:int) -> int:
    return survey_controller.add(search_area)

def test_add_survey(init, survey:int):
    assert isinstance(survey, int)
    assert survey > 0

def test_delete_survey(init, survey_controller:ABSurveyController, survey:int):
    survey_controller.delete(survey)
    with pytest.raises(peewee.DoesNotExist):
        assert SurveyModel.get_by_id(survey)

# listing
@pytest.fixture
def listing_controller(config):
    return ABListingController(config)

@pytest.fixture
def sample_room(listing_controller:ABListingController, json_sample:dict, survey:int):
    room = listing_controller.create_room_from_json(json_sample, survey)
    return (survey, room)

def test_create_from_json(init, sample_room):
    survey, room = sample_room
    room_from_db =  RoomModel.get((RoomModel.survey_id == survey) & (RoomModel.room_id == room))
    assert (room_from_db.survey_id.survey_id, room_from_db.room_id) == (1, 32230973)

# extra listing
@pytest.fixture
def listing_extra_controller(config):
    return ABListingExtraController(config)

#def test_get_extra(init, listing_extra_controller:ABListingExtraController, sample_room):
#    survey, room = sample_room
#    listing_extra_controller.get_extras(survey, room)