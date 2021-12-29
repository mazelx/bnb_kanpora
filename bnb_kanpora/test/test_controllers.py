from bnb_kanpora.config import Config
from bnb_kanpora.utils import GeoBox
from bnb_kanpora.controllers import *
from bnb_kanpora.db import MODELS
from os import path
import os
import shutil
import pytest
import json
import peewee
import pandas as pd

# Tests should be improved to cover edge cases 
# sample box, smaller to bigger
#sample_box = GeoBox(w_lng=5.566198, s_lat=47.425706, e_lng=5.617954, n_lat=47.460710) # gray small
#sample_box = GeoBox(w_lng=5.349655,s_lat=47.302050,e_lng=5.828934,n_lat=47.579305) # gray ~50 
sample_box = GeoBox(e_lng=1.800148,s_lat=46.771898, w_lng=1.581617, n_lat=46.916375) # chateauroux ~180
# sample_box = GeoBox(e_lng=-1.525514, s_lat=47.192878, w_lng=-1.581819, n_lat=47.227861) # nantes centre
#sample_box = GeoBox(w_lng=-1.563942, s_lat=43.476669, e_lng=-1.541583, n_lat=43.491179) # biarritz micro

@pytest.fixture
def config():
    cfg = Config(config_file="test.config", verbose=True)
    cfg.database.drop_tables(MODELS)
    cfg.database.create_tables(MODELS)
    return cfg

@pytest.fixture
def json_sample():
    with open('bnb_kanpora/test/test_room_sample.json', 'r') as f:
        data = json.load(f)
    return data

@pytest.fixture
def abrequest(config):
    return HTTPRequest(config)

@pytest.fixture
def search_area_controller(config):
    return SearchAreaController(config)

@pytest.fixture
def search_area(search_area_controller):
    return search_area_controller.add("Gotham City", sample_box)

def test_add_search_area(config, search_area:int):
    assert isinstance(search_area, int)
    assert search_area > 0

def test_delete_search_area(config, search_area_controller:SearchAreaController, search_area:int):
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

def test_add_survey(config, survey:int):
    assert isinstance(survey, int)
    assert survey > 0

def test_delete_survey(config, survey_controller:SearchSurveyController, survey:int):
    survey_controller.delete(survey)
    with pytest.raises(peewee.DoesNotExist):
        assert SurveyModel.get_by_id(survey)

# room
@pytest.fixture
def result_controller(config):
    return SearchResultsController(config)

@pytest.fixture
def sample_room(result_controller:SearchResultsController, json_sample:dict, survey:int):
    room = result_controller.parse_room_from_search_result(json_sample, survey)
    return (survey, room)

def test_create_from_json(config, sample_room):
    survey, room = sample_room
    room_from_db =  RoomModel.get((RoomModel.survey_id == survey) & (RoomModel.room_id == room))
    assert (room_from_db.survey_id.survey_id, room_from_db.room_id) == (1, 40279867)

# extra listing
@pytest.fixture
def listing_extra_controller(config):
    return ABListingExtraController(config)

def test_search_and_export(config, survey_controller:SearchSurveyController, survey:int):
    TOLERANCE_MISSING_ROOMS = 0.05
    TOLERANCE_EXCEEDING_ROOMS = -0.1
    EXPORT_FOLDER = "test-export"
    survey_results = survey_controller.run(survey)
    missing = survey_results.total_nb_rooms_expected - survey_results.total_nb_rooms
    assert (missing / survey_results.total_nb_rooms_expected) < TOLERANCE_MISSING_ROOMS
    assert (missing / survey_results.total_nb_rooms_expected)  > TOLERANCE_EXCEEDING_ROOMS
    
    #export
    exported_df_size = 0
    try:
        if path.exists(EXPORT_FOLDER):
            shutil.rmtree(EXPORT_FOLDER)
        os.mkdir(EXPORT_FOLDER)
        export_path = survey_controller.export(survey, folder=EXPORT_FOLDER)
        exported_df_size = len(pd.read_csv(export_path))
    except Exception as e:
        pass
    finally:
        shutil.rmtree(EXPORT_FOLDER)
    assert exported_df_size > 0
    


