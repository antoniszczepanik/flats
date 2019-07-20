import pandas as pd
import unidecode

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

class MorizonCleaner(object):
    
    def __init__(self, df):
        self.df = df
        self.required_columns = REQUIRED_COLUMNS
        
        # verify columns match
        for column in list(self.df.columns):
            if column not in self.required_columns:
                raise InvalidColumnsError
        self.df = self.df.fillna('no_info')
        
        # a dictionary with dictionary mappings, and None's where
        # no mapping is neccessery
        self.cleaning_map = {
            'balcony': 
            {
                'no_info': 0,
                'Nie': 0,
                'Tak': 1,
            },
            'building_height': None,
            'building_material': self.building_material, 
            'building_type': self.building_type,
            'building_year': None,
            'conviniences': self.conviniences,
#             'date_added': custom,
#             'date_refreshed': custom,
            'desc_len': None,
            'direct': None,
#             'equipment': custom,
#             'flat_state': custom,
#             'floor': custom,
#             'heating': custom,
            'image_link': 'remove',
            'lat': None,
            'lon': None,
            'market_type': 
            {
                'wtórny': 1,
                'pierwotny': 0,
            },
#             'media': custom,
            'offer_id': None,
            'price': None,
            'price_m2': None,
            'promotion_counter': None,
            'room_n': self.fillna_mode,
            'size': None,
            'taras': 
            {
                'no_info': 0,
                'Tak': 1,
                'Nie': 0,
            },
            'title': 'remove',
            'url': 'remove',
            'view_count': None,
        }


    def clean_df(self):
        for column in REQUIRED_COLUMNS:
            try:
                cleaning_func = self.cleaning_map[column]
            except KeyError:
                continue
                
            if callable(cleaning_func):
                cleaning_func(column)
            elif type(cleaning_func) == dict:
                self.df[column] = self.df[column].map(cleaning_func)
            elif cleaning_func == 'remove':
                self.df = self.df.drop(column, axis=1)
            elif cleaning_func is None:
                pass
            else:
                # temoporary, remove later
                pass
                log.error('Cleaning map has values other than expected.')
                raise InvalidCleaningMapError
                
        return self.df
    
    def fillna_mode(self, column_name):
        mode = self.df[column_name].mode()
        self.df[column_name] = self.df[column_name].fillna(mode)
        
    def building_material(self, column_name):
        brick = ['cegla', 'cegła']
        concrete_slab = ['plyta', 'płyta']
        
        def mapping(value):
            value = value.lower()
            if value == 'no_info':
                return 'no_info'
            for b in brick:
                if b in value:
                    return 'brick'
            for c in concrete_slab:
                if c in value:
                    return 'concrete_slab'
            return 'other'
        self.df[column_name] = self.df[column_name].apply(mapping)
        
        
    def building_type(self, column_name):
        block = ['blo', 'wiez', 'szer','seg', 'mieszk',
                'woln', 'inn', 'komp', 'bud', 'plo', 'lat']
        hist = ['kamien', 'will', 'zabyt', 'histo',
                     'styl', 'rezy', 'vil', 'socre', 'pal']
        apart = ['apart', 'nowe budo', 'loft', 'hot','wys', 'biur']
        house = ['do', 'niski', 'wielo', 'bliz']
        
        def mapping(value):
            value = unidecode.unidecode(str(value)).lower()
            for b in block:
                if b in value:
                    return 'block'
            for h in hist:
                if h in value:
                    return 'hist'
            for a in apart:
                if a in value:
                    return 'apart'
            for h in house:
                if h in value:
                    return 'house'
            return 'other'
        self.df[column_name] = self.df[column_name].apply(mapping)
        
    def conviniences(self, column_name):
        # lift case
        df['lift'] = df[column_name].apply(create_lift)
        df['basement'] = df[column_name].apply(create_basement)
        df['parking'] = df[column_name].apply(create_parking)
        df['road_access'] = df[column_name].apply(create_road_access)
        
        
        def check_if_not(word, string):
            """
            Verify if given word followed by '(nie)'
            """
            if f'{word} (nie)' in string:
                return f'no_{word}'
    

        
        

class InvalidCleaningMapError(Exception):
    pass

class InvalidColumnsError(Exception):
    pass


# import pandas as pd
# import unidecode
# pd.set_option('mode.chained_assignment', None)
# from sklearn.metrics import median_absolute_error, r2_score, mean_squared_error, mean_absolute_error

# def clean_raw_flat(
# 	flats_df,
#  	date_limit='2018-01-01',
# 	to_drop =['title', 'url', 'offer_id', 'building_material', 'date_refreshed', 'price', 'desc_len', 'promotion_counter', 'view_count', 'direct', 'date_added'], 
# 	max_nulls_per_row = 4,
# 	popular_coords_to_drop = 20,
# 	center_coords = [52.2304517, 21.0059146],
# 	max_height = 35,
# 	min_year = 1800,
# 	max_floor = 29,
# 	max_room_n = 15,
# 	min_size = 20,
# 	max_size = 300,
# 	max_center_dist = 0.5,
# 	max_price = 25000 
# 	):

# 	"""
# 	This function takes raw data scraped from morizon and cleans it into modelling format.
# 	Several features are dropped, some are trasformed and in fills some missing values.
# 	"""
# 	print(f'Inserted df: {len(flats_df)} rows, {len(flats_df.columns)} columns.')

# 	flats_df_size = len(flats_df)
# 	flats_df = flats_df.dropna(thresh=len(flats_df.columns)-max_nulls_per_row)
# 	print(f'Dropped {flats_df_size - len(flats_df)} rows. They contained more than {max_nulls_per_row} nulls.')
# 	flats_df_size = len(flats_df)	
			

# 	flats_df['taras'].fillna('Nie', inplace=True)
# 	flats_df['taras'] = flats_df['taras'].map({
# 	    'Tak': 1,
# 	    'Nie': 0
# 	})
# 	print('Remmaped taras to binary')
	
# 	flats_df['balcony'].fillna('Nie', inplace=True)
# 	flats_df['balcony'] = flats_df['balcony'].map({
# 	    'Tak': 1,
# 	    'Nie': 0
# 	})
# 	print('Remmaped balcony to binary')
	
# 	flats_df['flat_state'] = flats_df['flat_state'].apply(convert_flat_state)
# 	print('Remmaped flat_state with convert_flat_state funciton.')
# 	flats_df = create_dummies(flats_df, 'flat_state')
# 	print('Created dummies from flat_state column')	

# 	flats_df['building_type'] = flats_df['building_type'].fillna('blok').apply(convert_building_type)
# 	print('Remmaped building_type with convert_buliding_type funciton. Filled missing values with "blok"')
# 	flats_df = create_dummies(flats_df, 'building_type')
# 	print('Created dummies from building_type column')
	
# 	flats_df['building_year'].fillna(flats_df['building_year'].mode(), inplace=True)
# 	print('Filled missing values in building year with mode')
# 	flats_df['building_height'].fillna(flats_df['building_height'].mode(), inplace=True)
# 	print('Filled missing values in building_height with mode')

# 	flats_df['floor'].fillna(1, inplace=True)
# 	flats_df['floor'] = flats_df['floor'].apply(lambda x: int(str(x).replace('parter', '0').split()[0]))
# 	print('Filled missing values in floor with mode. Replace parter with 0.')

# 	flats_df['room_n'].fillna(flats_df['room_n'].mode(), inplace=True)
# 	print('Filled missing values in room_n with mode')

# 	flats_df['market_type'] = flats_df['market_type'].map({
# 	    'wtórny': 1,
# 	    'pierwotny': 0
# 	})	
# 	print('Converted market type to binary')

# 	popular_lats = list(flats_df['lat'].value_counts().index[:popular_coords_to_drop])
# 	flats_df = flats_df[~flats_df['lat'].isin(popular_lats)]
# 	print(f'Removed rows with common lon/lat values in top {popular_coords_to_drop} by popularity. Deleted {flats_df_size - len(flats_df)} rows.')
# 	flats_df_size = len(flats_df)
	
# 	flats_df['center_dist'] = get_dist(flats_df['lon'], flats_df['lat'], center_coords=center_coords)
# 	flats_df.drop(['lon', 'lat'], axis=1, inplace=True)	
# 	print('Calculated distance to the center point. Dropped lon/lat columns.')
	
# 	flats_df = remove_rows_by_column_value(flats_df, 'date_added', date_limit, remove_higher=False)
# 	flats_df = remove_rows_by_column_value(flats_df, 'building_height', max_height) 
# 	flats_df = remove_rows_by_column_value(flats_df, 'building_year', min_year, remove_higher=False) 
# 	flats_df = remove_rows_by_column_value(flats_df, 'floor', max_floor) 
# 	flats_df = remove_rows_by_column_value(flats_df, 'room_n', max_room_n) 
# 	flats_df = remove_rows_by_column_value(flats_df, 'size', max_size) 
# 	flats_df = remove_rows_by_column_value(flats_df, 'size', min_size, remove_higher=False)
# 	flats_df = remove_rows_by_column_value(flats_df, 'center_dist', max_center_dist) 
# 	flats_df = remove_rows_by_column_value(flats_df, 'price_m2', max_price)
# 	flats_df_size = len(flats_df)
# 	flats_df.drop(to_drop, axis=1, inplace=True)
# 	print(f'Dropped columns {to_drop}')


# 	print(f'Output df: {len(flats_df)} rows, {len(flats_df.columns)} columns.')
		
# 	return flats_df 

# def convert_flat_state(state):
# 	do_wprowadzenia = ['do wprowadzenia', 'wysoki', 'dobry', 'do zamieszkania', 'w remoncie', 'idealny', 'klucz']
# 	do_remontu = ['remontu', 'odswiezenia', 'bialego', 'do']
# 	deweloperski = ['deweloperski', 'developerski', 'surowy']
# 	state = unidecode.unidecode(str(state)).lower()
# 	for x in do_wprowadzenia:
# 		if x in state:
# 			return 'do_wprowadzenia'
# 	for x in do_remontu:
# 		if x in state:
# 			return 'do_remontu'
# 	for x in deweloperski:
# 		if x in state:
# 			return 'deweloperski'
			
# 	return 'brak_informacji'

# def convert_building_type(build_type):
#     build_type = unidecode.unidecode(str(build_type)).lower()
#     blok = ['blo', 'wiez', 'szer','seg', 'mieszk', 'woln', 'inn', 'komp', 'bud', 'plo', 'lat'] # 1
#     kamienica = ['kamien', 'will', 'zabyt', 'histo', 'styl', 'rezy', 'vil', 'socre', 'pal'] #3
#     apartament = ['apart', 'nowe budo', 'loft', 'hot','wys', 'biur'] #2
#     dom = ['do', 'niski', 'wielo', 'bliz'] #0
#     for x in blok:
#         if x in build_type:
#             return 'blok'
#     for x in kamienica:
#         if x in build_type:
#             return 'kamienica'
#     for x in apartament:
#         if x in build_type:
#             return 'apartament'
#     for x in dom:
#         if x in build_type:
#             return 'dom'
#     return 'brak_informacji'


# def subset_city(flats_df, city_name='Warszawa'):
# 	flats_df_size = len(flats_df)
# 	flats_df = flats_df[flats_df['title'].str.contains(city_name)]
# 	print(f'There are {len(flats_df)} offers from {city_name}. ({len(flats_df)/flats_df_size}%)')
# 	return flats_df

# def create_dummies(flats_df, column_name):
# 	dummies = pd.get_dummies(flats_df[column_name], prefix=column_name)
# 	flats_df = pd.concat([flats_df, dummies], axis=1, join='inner')
# 	flats_df.drop(column_name, axis=1, inplace=True)
# 	return flats_df

# def remove_rows_by_column_value(flats_df, column_name, max_value, remove_higher=True):
# 	flats_df_size = len(flats_df)
# 	if remove_higher:
# 		flats_df = flats_df[flats_df[column_name] < max_value]
# 	else:
# 		flats_df = flats_df[flats_df[column_name] > max_value]
# 	print(f'Subsetted {column_name} to {max_value}. Removed {flats_df_size-len(flats_df)} rows.')
# 	return flats_df
# def score_rmse_perc(mse, y):
#     if mse < 0:
#         mse = - mse
#     return mse**0.5/(sum(y)/len(y))

# def report(y_pred, y_val):
#     print('Baseline (std)')
#     print(y_val.std())
#     print('Baseline (std/mean)')
#     print(y_val.std()/y_val.mean())
#     print('RMSE')
#     print((mean_squared_error(y_pred, y_val)**0.5))
#     print('RMSE/Mean')
#     print(score_rmse_perc(mean_squared_error(y_pred, y_val), y_val))

# def get_dist(lon_col, lat_col, center_coords=[52.2304517, 21.0059146]):
# 	return ((lat_col - center_coords[0])**2 + (lon_col - center_coords[1])**2)**0.5





