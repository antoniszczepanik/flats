# columns required for performing the cleaning
REQUIRED_COLUMNS =[
    'balcony',
    'building_height', 
    'building_material', 
    'building_type',
    'building_year',
    'conviniences',
    'date_added',
    'date_refreshed',
    'desc_len',
    'direct',
    'equipment',
    'flat_state',
    'floor',
    'heating',
    'image_link',
    'lat',
    'lon',
    'market_type',
    'media',
    'offer_id',
    'price',
    'price_m2',
    'promotion_counter',
    'room_n',
    'size',
    'taras',
    'title',
    'url',
    'view_count',
]

# a dictionary with simple dictionary mappings, and None's where
# no mapping is neccessery
CLEANING_MAP = {
    'balcony': 
    {
        np.Nan: 0,
        'Nie': 0,
        'Tak': 1,
    },
    'building_height': None,
    'building_material': custom, 
    'building_type': custom,
    'building_year': None,
    'conviniences': custom,
    'date_added': custom,
    'date_refreshed': custom,
    'desc_len': None,
    'direct': None,
    'equipment': custom,
    'flat_state': custom,
    'floor': custom,
    'heating': custom,
    'image_link': 'remove',
    'lat': None,
    'lon': None,
    'market_type': 
    {
        'wtórny': 1
        'pierwotny': 0
    },
    'media': custom,
    'offer_id': None,
    'price': None,
    'price_m2': None,
    'promotion_counter': None,
    'room_n': mean(),
    'size': None,
    'taras': 
    {
        np.NaN: 0,
        'Tak': 1,
        'Nie': 0,
    },
    'title': 'remove',
    'url': 'remove',
    'view_count': None,
}




class MorizonCleaner(object):
    
    def __init__(self, df):
        self.df = df

    def clean_df(self):
        assert self.df.columns == REQUIRED_COLUMNS
        for column in REQUIRED_COLUMNS:
            cleaning_func = cleaning_map[column]
            if callable(cleaning_func):
                df[column] = df[column].apply(cleaning_func)
            elif type(cleaning_func) == dict:
                df[column] = df[column].map(cleaning_func)
            elif cleaning_func == 'remove':
                df = df.drop(column, axis=1)
            elif cleaning_func is None:
                pass
            else:
                log.error('Cleaning map has values other than expected.')
                raise InvalidCleaningMap
        return self.df



class InvalidCleaningMap(Exception):
    pass