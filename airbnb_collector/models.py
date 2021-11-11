from airbnb_collector.config import ABConfig

from peewee import AutoField, BooleanField, CharField, CompositeKey, ForeignKeyField, Model, PostgresqlDatabase, IntegerField, DecimalField, DateTimeField, SmallIntegerField, TextField
from datetime import datetime

# TODO fix object instanciation with config 

cfg = ABConfig()
database =  PostgresqlDatabase(
    cfg.DB_NAME, 
    user=cfg.DB_USER, 
    password=cfg.DB_PASSWORD,
    host=cfg.DB_HOST, 
    port=cfg.DB_PORT
)

class BaseModel(Model):
    class Meta:
        database = database

class SearchAreaModel(BaseModel):
    class Meta:
        table_name = "search_area"
 
    search_area_id = AutoField()
    name = CharField(255, default='UNKNOWN')
    abbreviation  = CharField(255, null=True)
    bb_n_lat = DecimalField(30,6)
    bb_e_lng = DecimalField(30,6)
    bb_s_lat = DecimalField(30,6)
    bb_w_lng = DecimalField(30,6)


class SurveyModel(BaseModel):
    class Meta:
        table_name = "survey"

    survey_id = AutoField()
    survey_date = DateTimeField(default=datetime.now)
    survey_description = CharField(255, null=True)
    comment = CharField(255, null=True)
    survey_method = CharField(20, default="neighborhood")
    status = SmallIntegerField(default=0)
    search_area_id = ForeignKeyField(SearchAreaModel, backref='surveys')


class RoomModel(BaseModel):
    class Meta:
        table_name = "room"
        primary_key = CompositeKey('survey_id', 'room_id')

    room_id = IntegerField()
    host_id = IntegerField()
    name = CharField(255)
    room_type = CharField(100)
    country = CharField(255, null=True)
    city = CharField(100)
    neighborhood = CharField(255, null=True)
    address = CharField(2000)
    reviews = IntegerField(null=True)
    overall_satisfaction = DecimalField(5,2, null=True)
    accommodates = IntegerField(null=True)
    bedrooms = DecimalField(5,2, null=True)
    bathrooms = DecimalField(5,2, null=True)
    price = DecimalField(5,2, null=True)
    deleted = BooleanField(default=False)
    minstay = IntegerField(null=True)
    license = CharField(100, null=True)
    last_modified = DateTimeField(default=datetime.now)
    latitude = DecimalField(30,6)
    longitude = DecimalField(30,6)
    survey_id = ForeignKeyField(SurveyModel, backref='rooms')
    coworker_hosted = IntegerField(null=True)
    extra_host_languages = CharField(100, null=True)
    currency = CharField(20, null=True)
    rate_type = CharField(20, null=True)
    picture_url = CharField(200, null=True)

class SurveyProgressModel(BaseModel):
    class Meta:
        table_name = "survey_progress"

    survey_id = ForeignKeyField(SurveyModel)
    room_type = CharField(100)
    guests = IntegerField(null=True)
    price_min = DecimalField(5,2,null=True)
    price_max = DecimalField(52, null=True)
    quadtree_node = CharField(1000)
    last_modified = DateTimeField(default=datetime.now)

class DBUtils:
    models = [SearchAreaModel, SurveyModel, RoomModel, SurveyProgressModel]

    @staticmethod
    def create_tables():
        with database:
            database.create_tables(DBUtils.models)

    @staticmethod
    def drop_tables():
        with database:
            database.drop_tables(DBUtils.models)

    @staticmethod
    def check_connection():
        try:
            database.connect()
            return True
        except:
            return False
