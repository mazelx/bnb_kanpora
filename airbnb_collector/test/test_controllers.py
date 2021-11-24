from airbnb_collector.config import ABConfig
from airbnb_collector.utils import GeoBox
from airbnb_collector.controllers import *
import pytest
import json
import peewee

# Tests should be improved to cover edge cases 
# sample box, smaller to bigger
#sample_box = GeoBox(w_lng=5.566198, s_lat=47.425706, e_lng=5.617954, n_lat=47.460710) # gray small
sample_box = GeoBox(w_lng=5.349655,s_lat=47.302050,e_lng=5.828934,n_lat=47.579305) # gray ~50
#sample_box = GeoBox(e_lng=1.800148,s_lat=46.771898, w_lng=1.581617, n_lat=46.916375) # chateauroux ~180
#sample_box = GeoBox(e_lng=-1.525514, s_lat=47.192878, w_lng=-1.581819, n_lat=47.227861) # nantes centre
#sample_box = GeoBox(w_lng=-1.591257,s_lat=43.459428,e_lng=-1.533407,n_lat=43.495925) # btz

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
    db = DatabaseController(config)
    db.drop_tables()
    db.create_tables()

@pytest.fixture
def abrequest(config):
    return ABRequest(config)

@pytest.fixture
def search_area_controller(config):
    return SearchAreaController(config)

@pytest.fixture
def search_area(search_area_controller):
    return search_area_controller.add("Gotham City", sample_box)

def test_add_search_area(init, search_area:int):
    assert isinstance(search_area, int)
    assert search_area > 0

def test_delete_search_area(init, search_area_controller:SearchAreaController, search_area:int):
    search_area_controller.delete(search_area)
    with pytest.raises(peewee.DoesNotExist):
        assert SearchAreaModel.get_by_id(search_area)

# survey
@pytest.fixture
def survey_controller(config):
    return SearchSurveyController(config)

@pytest.fixture
def survey(survey_controller:SearchSurveyController, search_area:int) -> int:
    return survey_controller.add(search_area)

def test_add_survey(init, survey:int):
    assert isinstance(survey, int)
    assert survey > 0

def test_delete_survey(init, survey_controller:SearchSurveyController, survey:int):
    survey_controller.delete(survey)
    with pytest.raises(peewee.DoesNotExist):
        assert SurveyModel.get_by_id(survey)

# room
@pytest.fixture
def result_controller(config):
    return SearchResultsController(config)

@pytest.fixture
def sample_room(result_controller:SearchResultsController, json_sample:dict, survey:int):
    room = result_controller.create_room_from_search_result(json_sample, survey)
    return (survey, room)

def test_create_from_json(init, sample_room):
    survey, room = sample_room
    room_from_db =  RoomModel.get((RoomModel.survey_id == survey) & (RoomModel.room_id == room))
    assert (room_from_db.survey_id.survey_id, room_from_db.room_id) == (1, 32230973)

# extra listing
@pytest.fixture
def listing_extra_controller(config):
    return ABListingExtraController(config)

def test_search(init, survey_controller:SearchSurveyController, survey:int):
    TOLERANCE_MISSING_ROOMS = 0.05
    TOLERANCE_EXCEEDING_ROOMS = -0.1
    survey_results = survey_controller.search(survey)
    missing = survey_results.total_nb_rooms_expected - survey_results.total_nb_rooms
    assert (missing / survey_results.total_nb_rooms_expected) < TOLERANCE_MISSING_ROOMS
    assert (missing / survey_results.total_nb_rooms_expected)  > TOLERANCE_EXCEEDING_ROOMS
