from collections import defaultdict

__author__ = 'SHostetter'
# set up base globals
DOWLOAD_PATH = r'C:\Users\SHostetter\Desktop'
DB_HOST = 'DOTDEVRHPGSQL01'  # database host
DB_NAME = 'CRASHDATA'   # Database
WORKING_SCHEMA = 'public'  # 'working'
FINAL_SCHEMA = 'public'
ARCHIVE_SCHEMA = 'archive'
LION = 'lion'
NODE = 'node'
RPL = 'tbl_rpl'
RPL_TXT = 'RPL.txt'
VERSION = '19d'
PRECINCTS = 'nypp'#''districts_police_precincts'
BOROUGHS = 'districts_boroughs'
HIGHWAYS = True
SRID = 2263

FOLDER = r'C:\Users\SHostetter\Desktop\GIT\CLION\DATA\{}'.format(VERSION.upper())


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
