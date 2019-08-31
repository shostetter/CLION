from collections import defaultdict

__author__ = 'SHostetter'
# set up base globals
DB_HOST =  # database host
DB_NAME = # Database
WORKING_SCHEMA = 'working'  # 'working'
FINAL_SCHEMA = 'public'
ARCHIVE_SCHEMA = 'archive'
LION = 'lion'
NODE = 'node'
RPL = 'tbl_rpl'
RPL_TXT = 'RPL.txt'
VERSION = '18d'  # '15b'
PRECINCTS = 'districts_police_precincts'
BOROUGHS = 'districts_boroughs'
HIGHWAYS = True
SRID = 2263

FOLDER = # working folder

# global dictionaries
def st_name_factory():
    return [set(), 0]
nodeStreetNames = defaultdict(st_name_factory)  # {node: [{street names}, masterid}
nodeIsIntersection = {}  # {node: True or False}
nodeNextSteps = defaultdict(lambda: defaultdict(set))  # {node: {street: fromNode, toNode}}
segmentBlocks = {}  # {segmentID: fromMaster, toMaster}
nodeMaster = {}  # {node: masterid}
masterNodes = defaultdict(list)  # {masterid: [nodeid, nodeid, ...]}
clusterIntersections = defaultdict(st_name_factory)  # {sorted-street-names: [set([nodes]), masterID]}
mfts = []
coordFromMaster = {}  # {master: [x,y]
# minor datastores - can be deleted after use?
streetSet = []
mft1Dict = defaultdict(list)  # mft: [segmentid, segmentid]
altGraph = defaultdict(list)
