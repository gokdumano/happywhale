from contextlib import closing
from enum import Enum

import requests, sqlite3, json

db_path   = r'C:\Users\deniz\OneDrive\Masaüstü\happywhale\happywhale\happywhale.db'
Dates     = Enum('Date', ['On', 'Before', 'After', 'Between', 'Preset'], start=0)
Presets   = Enum('Preset', ['AllTime', 'PastYear', 'PastMonth', 'PastWeek'], start=0)
Locations = Enum('Location', ['WholeWorld', 'MapBounds', 'Location', 'WaterGeo'], start=0)

def getAllOceans(con):
    q = con.execute('SELECT id, name FROM oceans')
    results = q.fetchall()    
    if results is None: return None
    return [{ 'oceanId': oceanId, 'oceanName': oceanName } for (oceanId, oceanName) in results]

def getAllSeasByOceanName(oceanName, con):
    oceanId = getOceanId(oceanName, con)
    return getAllSeasByOceanId(oceanId, con)

def getAllSeasByOceanId(oceanId, con):
    q = con.execute('SELECT seaid, name FROM seas WHERE oceanid = ?', (oceanId,))
    results = q.fetchall()    
    if results is None: return None
    return [{ 'seaId': seaId, 'seaName': seaName } for (seaId, seaName) in results]

def getAllSpeciesNames(con):
    q = con.execute('SELECT name FROM species')
    results = q.fetchall()    
    if results is None: return None
    return [specName for (specName, ) in results]

# ----------------------------------------

def getOceanId(oceanName, con):
    q = con.execute('SELECT id FROM oceans WHERE name = ?', (oceanName.lower(),))
    oceanId = q.fetchone()
    if oceanId is None: return None
    return oceanId[0]

def getSeaId(seaName, oceanId, con):
    q = con.execute('SELECT seaid FROM seas WHERE name = ? AND oceanid = ?', (seaName.lower(), oceanId))
    oceanId = q.fetchone()
    if oceanId is None: return None
    return oceanId[0]

def getSpecQName(specName, con):
    if specName is None: return None
    q = con.execute('SELECT qname FROM species WHERE name = ?', (specName.lower(),))
    specDbName = q.fetchone()
    if specDbName is None: return None
    return specDbName[0]
    
# ----------------------------------------

def DateSearch(date, **kwargs):
    match date:
        case Dates.On | Dates.Before | Dates.After:
            startdate = kwargs.get('startdate')
            assert startdate is not None, 'datesearch needs a startdate...'
            datesearch = { 'type': date.value, 'startdate': startdate }
        case Dates.Between:
            startdate = kwargs.get('startdate')
            enddate = kwargs.get('enddate')
            assert None not in (startdate, enddate), 'datesearch needs both startdate and enddate...'
            datesearch = { 'type': date.value, 'startdate': startdate, 'enddate': enddate }
        case Dates.Preset:
            preset = kwargs.get('preset')
            assert preset is not None, 'datesearch needs a preset...'
            datesearch = { 'type': date.value, 'preset': preset.value }
        case _:
            raise ValueError('Cannot create a valid datesearch query...')
    return datesearch

def LocSearch(loc, **kwargs):
    match loc:
        case Locations.WholeWorld:
            locsearch = None
        case Locations.MapBounds:
            mapBounds = kwargs.get('mapBounds')
            assert mapBounds is not None, 'locsearch needs `mapBounds`: List<float, float, float, float>("minLat", "minLng", "maxLat", "maxLng")...'
            minLat, minLng, maxLat, maxLng = mapBounds
            locsearch = {
                'type': 'mapbounds',
                'mapBounds': {
                    'southWest': { 'lat': minLat, 'lng': minLng },
                    'northEast': { 'lat': maxLat, 'lng': maxLng }
                    }
                }
        case Locations.Location:
            location = kwargs.get('location')
            assert location is not None, 'locsearch needs a `location`: String...'
            locsearch = {
                "type": "location",
                "location": location
                }
        case Locations.WaterGeo:
            oceanName = kwargs.get('oceanName')
            seaName   = kwargs.get('seaName')
            
            oceanId   = None
            seaId     = None
            
            assert oceanName is not None, 'locsearch needs an `oceanName`: String...'
            with closing(sqlite3.connect(db_path)) as con:
                oceanId = getOceanId(oceanName.lower(), con)
                if seaName is not None:
                    seaId = getSeaId(seaName.lower(), oceanId, con)

                locsearch = {
                    'type': 'watergeo',
                    'watergeo': {
                        'oceanid': oceanId,
                        'seaid': seaId
                        }
                    }
        case _:
            raise ValueError('Cannot create a valid locsearch query...')
    return locsearch

def Encounters(date, loc, specName=None, showConnections=False, **kwargs):
    with closing(sqlite3.connect(db_path)) as con:
        data = {
            'encounter': {
                'datesearch': DateSearch(date, **kwargs),
                'locsearch': LocSearch(loc, **kwargs),
                'species': getSpecQName(specName, con)
                },
            'showConnections': showConnections
            }
        return data

def Individual(specName, showConnections=False):
    with closing(sqlite3.connect(db_path)) as con:
        data = {
            'individual': {
                'species': getSpecQName(specName, con)
                },
            'showConnections': showConnections
            }
        return data

url = 'https://critterspot.happywhale.com/v1/cs/admin/encounter/search'
data = Encounters(
    Dates.Between,
    Locations.WholeWorld,
    startdate="1970-01-01",
    enddate="1980-01-01"
    )

r = requests.post(url, json=data)
    
