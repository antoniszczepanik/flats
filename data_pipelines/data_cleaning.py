from datetime import datetime

import pandas as pd
import numpy as np
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

FILL_NA_WITH_ZERO = ('parking_spot')

class MorizonCleaner(object):
    
    def __init__(self, df):
        self.df = df
        self.required_columns = REQUIRED_COLUMNS
        
        # verify columns match
        for column in self.required_columns:
            if column not in self.df.columns:
                raise InvalidColumnsError(f'{column} not found in required columns.')
        self.df = self.df.fillna('no_info')
        
        # a dictionary with dictionary mapping functions and None's where
        # no mapping is neccessery
        self.cleaning_map = {
            'balcony': 
            {
                'no_info': 'no_info',
                'Nie': 0,
                'Tak': 1,
            },
            'building_height': None,
            'building_material': self.building_material, 
            'building_type': self.building_type,
            'building_year': None,
            'conviniences': self.conviniences,
            'date_added': self.date_to_int, 
            'date_refreshed': self.date_to_int,
            'desc_len': None,
            'direct': None,
            'equipment': self.equipment,
            'flat_state': self.flat_state,
            'floor': self.floor,
            'heating': self.heating,
            'image_link': 'remove',
            'lat': None,
            'lon': None,
            'market_type': 
            {
                'wtórny': 1,
                'pierwotny': 0,
            },
            'media': self.media,
            'offer_id': None,
            'price': None,
            'price_m2': None,
            'promotion_counter': None,
            'room_n': self.fillna_mode,
            'size': None,
            'taras': 
            {
                'no_info': 'no_info',
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
                continue
            else:
                # temoporary, remove later
                pass
                log.error('Cleaning map has values other than expected.')
                raise InvalidCleaningMapError
                
        # creating new binary columns and remove 'no_info' values
        self.df = self.one_hot_encode_no_info(self.df)
        # replace 'no_info' with mode in given column
        self.df = self.replace_no_info_with_mode(self.df)
        # one hot encode remaining categorical columns
        self.df = self.one_hot_encode_categorical(self.df)
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
        def check_if_not(pol_word, string):
            # Verify if parameter is negated
            string = string.lower()
            if f'{pol_word} (nie)' in string or f'{pol_word} (brak)' in string:
                return 0
            elif pol_word in string:
                return 1
            else:
                return 'no_info'
        
        def create_lift(value):
            value = value.lower()
            if 'brak windy' in value:
                return 0
            elif 'winda' in value:
                return 1
            else:
                return 'no_info'
            
        def create_parking(value):
            value = value.lower()
            pol_park = 'miejsce parkingowe'
            if pol_park in value:
                # find value in parenthesis following polpark
                try:
                    parens = value.split(pol_park + ' (')[1].split(')')[0]
                except IndexError:
                    return 1
                try:
                    parking_n = int(parens)
                except ValueError:
                    try:
                        parking_n = int(parens.split(',')[0])
                    except ValueError:
                        parking_n = 1
                return parking_n
            else:
                return 'no_info'
        
        self.df['lift'] = self.df[column_name].apply(create_lift)
        self.df['basement'] = self.df[column_name].apply(
            lambda v: check_if_not('piwnica', v))
        self.df['telecom'] = self.df[column_name].apply(
            lambda v: check_if_not('domofon', v))
        self.df['driveway'] = self.df[column_name].apply(
            lambda v: check_if_not('podjazd', v))
        self.df['fence'] = self.df[column_name].apply(
            lambda v: check_if_not('ogrodzenie', v))
        self.df['parking_spot'] = self.df[column_name].apply(create_parking)
        self.df = self.df.drop(column_name, axis=1)
        
    def date_to_int(self, column_name):
        dt_2018 = datetime(2018, 1, 1)
        self.df[f'{column_name}_days_from_2018'] = self.df[column_name].apply(
            lambda x: (datetime.strptime(x, "%Y-%m-%d") - dt_2018).days)
        self.df = self.df.drop(column_name, axis=1)
        
    def equipment(self, column_name):
        def furniture(value):
            value = value.lower()
            if 'meble (nie)' in value:
                return 0
            elif 'meble' in value:
                return 1
            else:
                return 'no_info'
            
        def kitchen_furniture(value):
            value = value.lower()
            if 'kuchnia umeblowana' in value:
                return 1
            else:
                return 'no_info'
            
        self.df['furniture'] = self.df[column_name].apply(furniture)
        self.df['kitchen_furniture'] = self.df[column_name].apply(
            kitchen_furniture)
        self.df = self.df.drop(column_name, axis=1)
        
    def flat_state(self, column_name):
        very_good = ['bardzo', 'wysoki', 'po remon', 'podwyzszony',
                     'ideal', 'komfort', 'po gener']
        good = ['dobry', 'normalny', 'do wprowa', 'po czescio', 'czesciowo po',
               'zamieszkania', 'wykon']
        raw = ['dewel', 'devel', 'wykonczenia', 'odswiezenia', 'do czescio',
               'sred', 'do adaptacji', 'drobnego', 'surowy zamkniety', 'surowy']
        to_renovation = ['remontu', 'odnowienia']
        
        def mapping(value):
            value = unidecode.unidecode(str(value)).lower()
            for vg in very_good:
                if vg in value:
                    return 4
            for g in good:
                if g in value:
                    return 3
            for r in raw:
                if r in value:
                    return 2
            for tr in to_renovation:
                if tr in value:
                    return 1
            return 3
        self.df[column_name] = self.df[column_name].apply(mapping)
        
    def floor(self, column_name):
        
        def floor_n(value):
            value = value.split(' / ')
            if len(value) in (1, 2):
                if value[0] == 'parter':
                    return 0
                elif value[0] == 'no_info':
                    return 'no_info'
                else:
                    return int(value[0])
            else:
                raise ValueError(f'"{value}" value is not expected in this column')
                
        def max_floor_n(value):
            value = value.split(' / ')
            if len(value) == 2:
                return int(value[1])
            else:
                return 'no_info'
            
        self.df['foor_n'] = self.df[column_name].apply(max_floor_n)
        self.df[column_name] = self.df[column_name].apply(floor_n)
        
    def heating(self, column_name):
        
        def heating_type(value):
            if value == 'no_info':
                return 'no_info'
            elif 'Ogrzewanie (brak)' in value:
                return 'no_heating'
            elif 'miejsk' in value:
                return 'urban'
            elif value == 'Ogrzewanie':
                return 'urban'
            elif 'elektry' in value:
                return 'electric'
            elif 'gaz' in value:
                return 'gas'
            elif 'piec'  in value or 'węgl' in value:
                return 'coal'
            elif 'komin' in value:
                return 'fireplace'
            elif 'Ogrzewanie' in value:
                return 'other'
            else:
                return 'no_info'
            
        self.df[column_name] = self.df[column_name].apply(heating_type)
        
    def media(self, column_name):
        
        def internet(value):
            value = value.lower()
            if 'internet (brak' in value:
                return 0
            elif 'internet' in value:
                return 1
            else:
                return 'no_info'
            
        def water(value):
            value = value.lower()
            if 'woda' in value:
                return 1
            else:
                return 0
            
        def gas(value):
            value = value.lower()
            if 'gaz (brak' in value:
                return 0
            elif 'gaz' in value:
                return 1
            else:
                return 'no_info'
            
        def electricity(value):
            value = value.lower()
            if 'prąd' in value:
                return 1
            else:
                return 0
            
        def sewers(value):
            value = value.lower()
            if 'kanalizacja' in value:
                return 1
            else:
                return 0
            
        self.df['internet'] = self.df[column_name].apply(internet)
        self.df['water'] = self.df[column_name].apply(water)
        self.df['gas'] = self.df[column_name].apply(gas)
        self.df['electricity'] = self.df[column_name].apply(electricity)
        self.df['sewers'] = self.df[column_name].apply(sewers)
        self.df = self.df.drop(column_name, axis=1)
        
    def one_hot_encode_no_info(self, df):
        for col in df.columns:
            if 'no_info' in list(df[col].values):
                df[col + '_no_info'] = df[col].apply(
                    lambda x: 1 if x == 'no_info' else 0
                )
        return df
            
    def replace_no_info_with_mode(self, df):
        # replace with 0 if binary (no info == lack of feature)
        # else with mode value
        for col in df.columns:
            cols_to_replace = []
            if 'no_info' in list(df[col].values):
                cols_to_replace.append(col)
            df[cols_to_replace] = df[cols_to_replace].replace({'no_info':np.nan})
            for col in cols_to_replace:
                vc = self.value_counts(df, col)
                if vc in (1, 2) or col in FILL_NA_WITH_ZERO:
                    df[col] = df[col].fillna(0)
                else:
                    mode = df[col].mode()[0]
                    df[col] = df[col].fillna(mode)
        return df
    
    def value_counts(self, df, col):
        return len(df[col].value_counts())
    
    def one_hot_encode_categorical(self, df):
        for col in df.columns:
            if self.value_counts(df, col) < 10 and df[col].dtype == 'object':
                df = pd.concat(
                    [df, pd.get_dummies(df[col], prefix=col)], axis=1
                )
        return df
                
        
class InvalidCleaningMapError(Exception):
    pass


class InvalidColumnsError(Exception):
    pass