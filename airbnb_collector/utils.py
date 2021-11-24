from dataclasses import dataclass, field
from typing import Dict

@dataclass
class GeoBox():
    e_lng: float = 0.0
    s_lat: float = 0.0
    w_lng: float = 0.0
    n_lat: float = 0.0

    def __str__(self) -> str:
        return f"e_lng:{self.e_lng}, s_lat:{self.s_lat}, w_lng:{self.w_lng}, n_lat:{self.n_lat}"

    def get_two_splits(self) -> tuple:
        return (
            #N
            GeoBox(
                n_lat=self.n_lat,
                e_lng=self.e_lng,
                s_lat=(self.n_lat + self.s_lat) / 2,
                w_lng=self.w_lng
            ),
            GeoBox(
                n_lat=(self.n_lat + self.s_lat) / 2,
                e_lng=self.e_lng,
                s_lat=self.s_lat,
                w_lng=self.w_lng
            )
        )

    def enlarge(self, enlarge_pct:float=0.0) -> 'GeoBox':
            self.n_lat = self.n_lat + abs(self.n_lat - self.s_lat) * enlarge_pct
            self.s_lat = self.s_lat - abs(self.s_lat - self.n_lat) * enlarge_pct
            self.e_lng = self.e_lng + abs(self.e_lng - self.w_lng) * enlarge_pct
            self.w_lng = self.w_lng - abs(self.w_lng - self.e_lng) * enlarge_pct
            return self
            

    def get_four_splits(self, enlarge_pct:float=0.0) -> tuple:
        splits = (
            #NE
            GeoBox(
                n_lat=self.n_lat,
                e_lng=self.e_lng,
                s_lat=(self.n_lat + self.s_lat) / 2,
                w_lng=(self.w_lng + self.e_lng) / 2
            ),
            #NW
            GeoBox(
                n_lat=self.n_lat,
                e_lng=(self.w_lng + self.e_lng) / 2,
                s_lat=(self.n_lat + self.s_lat) / 2,
                w_lng=self.w_lng
            ),
            #SE
            GeoBox(
                n_lat=(self.n_lat + self.s_lat) / 2,
                e_lng=self.e_lng,
                s_lat=self.s_lat,
                w_lng=(self.w_lng + self.e_lng) / 2
            ),
            #SW
            GeoBox(
                n_lat=(self.n_lat + self.s_lat) / 2,
                e_lng=(self.w_lng + self.e_lng) / 2,
                s_lat=self.s_lat,
                w_lng=self.w_lng
            )
        )
        return (box.enlarge(enlarge_pct) for box in splits)

@dataclass
class SearchResults():
    nb_rooms_expected:int = 0
    rooms:list = field(default_factory=list)
    geobox: GeoBox = GeoBox()

    @property
    def nb_rooms(self):
        return len(self.rooms)

@dataclass
class SurveyResults():
    total_nb_saved:int = 0
    search_results: Dict[str, SearchResults] = field(default_factory=dict)

    def get_uniques_search_results(self):
        unique_room_ids = set()
        unique_search_results = {}
        for idx_tree, sr in self.search_results.items():
            unique_sr = sr
            unique_sr.rooms = [room for room in sr.rooms if room['listing']['id'] not in unique_room_ids]
            unique_search_results[idx_tree] = unique_sr
            unique_room_ids.update([room['listing']['id'] for room in sr.rooms])
        self.search_results = unique_search_results
        return self
    
    @property
    def total_nb_rooms_expected(self, idx_tree='0'):
        nb_rooms = self.search_results[idx_tree].nb_rooms_expected
        if nb_rooms == 1001:
            # Airbnb limits nb_rooms to 1001
            for level in range(5):
                try:
                    nb_rooms = 0
                    indices = [k for k in self.search_results if len(k) == (level * 2) + 1]
                    for idx in indices:
                        _nb_rooms = self.search_results[idx].nb_rooms_expected
                        if _nb_rooms < 1001:
                            nb_rooms += _nb_rooms
                        else:
                            raise Exception()
                    return nb_rooms
                except Exception as e:
                    continue
        return nb_rooms

    @property
    def total_nb_rooms(self):
        return sum([sr.nb_rooms for k, sr in self.search_results.items()])

@dataclass
class RoomTypes():
    ENTIRE_APT:str = "Entire home/apt"
    PRIVATE_ROOM:str = "Private room"
    SHARED_ROOM:str = "Shared room"

