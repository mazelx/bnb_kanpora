from bnb_kanpora.utils import GeoBox

from peewee import AutoField, BooleanField, CharField, CompositeKey, ForeignKeyField, Model, IntegerField, DecimalField, DateTimeField, SmallIntegerField, TextField, BigIntegerField
from datetime import datetime

class SearchAreaModel(Model):
    class Meta:
        table_name = "search_area"
 
    search_area_id = AutoField()
    name = CharField(255, default='UNKNOWN')
    abbreviation  = CharField(255, null=True)
    bb_n_lat = DecimalField(30,6)
    bb_e_lng = DecimalField(30,6)
    bb_s_lat = DecimalField(30,6)
    bb_w_lng = DecimalField(30,6)

    def __str__(self):
        return f"{self.search_area_id} : {self.name}, http://bboxfinder.com/#{self.bb_s_lat},{self.bb_w_lng},{self.bb_n_lat},{self.bb_e_lng}"

    @property
    def geobox(self):
        return GeoBox(
            n_lat=self.bb_n_lat,
            e_lng=self.bb_e_lng,
            s_lat=self.bb_s_lat,
            w_lng=self.bb_w_lng,
        )


class SurveyModel(Model):
    class Meta:
        table_name = "survey"

    survey_id = AutoField()
    survey_date = DateTimeField(default=datetime.now)
    survey_description = CharField(255, null=True)
    comment = CharField(255, null=True)
    survey_method = CharField(20, default="neighborhood")
    status = SmallIntegerField(default=0)
    search_area_id = ForeignKeyField(SearchAreaModel, backref='surveys')

    def __str__(self):
        return f"Survey: {self.survey_id}: {self.survey_date} - SearchArea:{self.search_area_id} "


class RoomModel(Model):
    class Meta:
        table_name = "room"
        primary_key = CompositeKey('survey_id', 'room_id')
    
    survey_id = ForeignKeyField(SurveyModel, backref='rooms')
    room_id = BigIntegerField()
    host_id = BigIntegerField()
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
    license = CharField(2000, null=True)
    last_modified = DateTimeField(default=datetime.now)
    latitude = DecimalField(30,6)
    longitude = DecimalField(30,6)
    coworker_hosted = IntegerField(null=True)
    extra_host_languages = CharField(100, null=True)
    currency = CharField(20, null=True)
    rate_type = CharField(20, null=True)
    picture_url = CharField(200, null=True)
    pdp_type = CharField(200, null=True)
    pdp_url_type = CharField(200, null=True)
    rate = DecimalField(5,2, null=True)
    rate_with_service_fee = DecimalField(5,2, null=True)
    currency = CharField(5, null=True)

class SurveyProgressModel(Model):
    class Meta:
        table_name = "survey_progress"

    survey_id = ForeignKeyField(SurveyModel)
    room_type = CharField(100)
    guests = IntegerField(null=True)
    price_min = DecimalField(5,2,null=True)
    price_max = DecimalField(52, null=True)
    quadtree_node = CharField(1000)
    last_modified = DateTimeField(default=datetime.now)
