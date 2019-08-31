import RIS_Tools as db2
from tqdm import tqdm
import params
import os
import RPL_importer as RPLi
import math
import itertools
from collections import defaultdict
from datetime import datetime
import getpass

#
# This is the refactor of CLION intended to simplify the workflow.
#
#     --- THIS IS A WORK IN PROGRESS  ---
#
# -----------------------------------------------------------------
# STEPS:
#     1. Setup database
#       a. Archive historic data if exists
#       b. Reset / create new LION files in database
#       c. Add version number and timestamp (sub version)
#       d. Add new columns for clion
#     2. Define street network to use (centerline)
#       a. Update manually fixed locations - fixes for LION errors
#       b. Exclude highways, ped overpasses, non-street features, Rikers Island
#       c. define ramps in lion
#     3. Define Intersections
#       a. Lookup table of street names to nodes inlcudes highway ramps
#           (where they intersect with non-highway street features)
#       b. identify double segments -> generates false intersections
# TODO: ____________________ why is this before the master nodes section ______________
#     4. Build street network graph
#       a. street name dictionary
#       b. node is intersection
#       c. graph streets
#     5. Build simplified network - segments
#       a. build full blocks - intersection to intersection (removes unecssary segmentation) part 1
#       b. build simple intersections (masterIDs first grouping)
#       c. build master intersections - merge masterids of similar street name sets
#       d. add masterid to clustered intersections (dictionary)
#     6. Build simplified network - nodes
#       a. Revise simplified network - nodes
#           i. identify nodes with the same masterid but are far apart
#           ii. check if the distant nodes are in different precincts
#           iii. split nodes based on pct
#       b. build masterIDs from nodes that are near eachother
#       c. group nodes into masters where meet in small triangle
#       d. write masterids to database
#     7. Generate stable masterids
# TODO: ____________________ move street network build after step 7 ? ______________
#     8. Rebuild street network with new masterid info
#     9. Update roadbeds
#       a. update roadbed segments
#       b. update roadbed nodes
#     10. Generate corridors
#       a. Dissolve on street name and geom
#       b. Create ID for corridors
#     11 Make master geom lookup tables
#     12. Make views
#     13. Cleanup and index


# -----------------------------------------------------------------

# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
# Step 1: Setup database
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
def archive(dbo, lion=params.LION, node=params.NODE, schema=params.WORKING_SCHEMA ,
            archive_schema=params.ARCHIVE_SCHEMA):
    print 'Archiving existing version...\n'
    # get last version
    v = dbo.query("select distinct version||'.'||created::date from {s}.{l}".format(
        l=lion,
        s=schema))
    dbo.query("""drop table if exists {archs}."{ver}_{l}";
                create table {archs}."{ver}_{l}" as 
                select * from {s}.{l}
                """.format(archs=archive_schema, s=schema, l=lion, ver=v.data[0][0]))
    dbo.query("""
            comment on table {archs}."{ver}_{l}" is 'Created by {u} on {d}'
            """.format(archs=archive_schema, s=schema, l=lion, ver=v.data[0][0],
                       u=getpass.getuser(), d=datetime.now().strftime('%Y-%m-%d %H:%M')))
    dbo.query("""drop table if exists {archs}."{ver}_{n}";
                    create table {archs}."{ver}_{n}" as 
                    select * from {s}.{n}
                    """.format(archs=archive_schema, s=schema, n=node, ver=v.data[0][0]))
    dbo.query("""
                comment on table {archs}."{ver}_{n}" is 'Created by {u} on {d}'
                """.format(archs=archive_schema, s=schema, n=node, ver=v.data[0][0],
                           u=getpass.getuser(), d=datetime.now().strftime('%Y-%m-%d %H:%M')))


@db2.timeDec
def setup_database(dbo, lion=params.LION, node=params.NODE, version=params.VERSION, rpl=params.RPL_TXT,
                   schema=params.WORKING_SCHEMA, folder=params.FOLDER):
    # clear tables if exist
    tables = [lion, node, rpl[:-4]]
    for table in tables:
        dbo.query("DROP TABLE IF EXISTS {s}.{t} CASCADE;".format(s=schema, t=table))
    # import lion shapefiles
    for shp in [('lion.shp', params.LION), ('node.shp', params.NODE)]:
        shapefile = os.path.join(params.FOLDER, shp[0])
        db2.import_shp_to_pg(shapefile, dbo, schema)
        # rename if needed
        if shapefile != shp[1]:
            dbo.query('ALTER TABLE {}."{}" RENAME TO {}'.format(schema, shapefile.lower()[:-4], shp[1]))
            dbo.query('ALTER TABLE {}."{}" RENAME wkb_geometry  TO {}'.format(
                schema, shp[1], 'geom')
            )
        print 'Imported {}'.format(os.path.join(params.FOLDER, shp[0]))
    # fix node geom field (GDAL imports as MultiPoint
    # Revised process for PostGIS 2.X +
    dbo.query("""
                ALTER TABLE {s}.{t} 
                ALTER COLUMN geom TYPE geometry(Point,2263) USING ST_GeometryN(geom, 1);
            """.format(s=schema, t=node))
    # Replaced with Above - this method needed for PostGIS 1.X
    # fix node geom field (GDAL imports as MultiPoint)
    # dbo.query("""
    #         alter table {s}.{t} add column geo geometry(Point,2263);
    #         update {s}.{t} set geo = (st_dump(geom)).geom;
    #         alter table {s}.{t} drop column geom;
    #         alter table {s}.{t} rename geo to geom;
    #         """.format(s=schema, t=node))
    # add RPL table
    RPLi.run(dbo, folder, rpl)
    # add version
    add_version(dbo, schema, lion, node, version, rpl)
    # add master id columns
    add_clion_columns(dbo, schema, lion, node)


def add_version(dbo, schema=params.WORKING_SCHEMA, lion=params.LION, node=params.NODE,
                version=params.VERSION, rpl=params.RPL_TXT):
    # update lion, node and rpl with version number
    tables = [lion, node, 'tbl_'+rpl[:-4]]
    for table in tables:
        print 'Updating {}...'.format(table)
        dbo.query("ALTER TABLE {s}.{t} ADD version varchar(5)".format(s=schema, t=table))
        dbo.query("ALTER TABLE {s}.{t} ADD created timestamp".format(s=schema, t=table))
        dbo.query("UPDATE {s}.{t} set version = '{v}', created = now()".format(s=schema, t=table, v=version))


def add_clion_columns(dbo, schema=params.WORKING_SCHEMA, lion=params.LION, node=params.NODE):
    print 'Adding master ID fields to tables...\n'
    dbo.query("""alter table {0}.{1}
                 add column mft int,
                 add column masteridfrom int,
                 add column masteridto int, 
                 add column exclude bool default True, 
                 add column manual_fix bool default False,
                 add column ramp bool default False
                 """.format(
        schema, lion))
    dbo.query("""alter table {0}.{1} 
                 add column masterid int,
                 add column is_int bool default False,
                 add column manual_fix bool default False
                """.format(schema, node))
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
# Step 2: Define street network to use (centerline)
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


def manual_fix_segments(dbo, to_fix, schema=params.WORKING_SCHEMA, lion=params.LION):
    # to_dos = [(where field, where value, update field, update value)]
    for row in to_fix:
        wf, wv, uf, uv = row
        print 'Updating {s} field {f} to {v}'.format(f=row[1], v=row[2], s=row[0])
        dbo.query("update {sch}.{l} set {u_f} = {u_v}, manual_fix = True where {w_f} = {w_v}".format(
            sch=schema, l=lion, u_f=uf, u_v=uv, w_f=wf, w_v=wv
        ))
        dbo.conn.commit()


@db2.timeDec
def define_usable_street_network(dbo, schema=params.WORKING_SCHEMA, lion=params.LION):
    # take out highways
    dbo.query("""
                update {s}.{t} set exclude = False
                where rb_layer in ('G', 'B') and featuretyp in ('0', '6', 'C') 
                and (nonped = 'D' or nonped is null)
                and trafdir != 'P' and rw_type!='7' -- take out ped streets (not including step streets)
                and street != 'UNNAMED STREET' 
                and street != 'DRIVEWAY' 
                and street not like '%{po1}%' and street not like '%{po2}%'  and street not like '%{po3}%'
                """.format(s=schema, t=lion, po1='PED OVPS', po2='PEDESTRIAN OVERPASS', po3='PEDESTRIAN UNDERPASS'))
    # add special cases to include list
    # update the manual fix and nonped to make the remaining code work with these edge cases
    dbo.query("""
                   update {s}.{t} set exclude = False, nonped = null, manual_fix = True
                   where rb_layer in ('G', 'B') and featuretyp in ('0', '6', 'C') 
                   and (nonped = 'V' and street in ({street_list}) )
                   and trafdir != 'P' and rw_type!='7' -- take out ped streets (not including step streets)
                   """.format(s=schema, t=lion, street_list="'PELHAM PARKWAY', 'ROCKAWAY FREEWAY', 'EAST FORDHAM ROAD'")
              )


def define_ramps(dbo, schema=params.WORKING_SCHEMA, lion=params.LION):
    # update lion street segments with ramp boolen value
    # this will be used to define intersections
    print 'updating ramp flag'
    dbo.query("""
                UPDATE {s}.{l} set ramp = True
                where featuretyp in ('0', '6', 'C') and rb_layer in ('G', 'B') -- centerline streets
                and nonped = 'V' and (rw_type = '9' or segmenttyp = 'E' or segmenttyp = 'F') -- ramps
    """.format(s=schema, l=lion))
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
# Step 3: Define Intersections
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


def build_generic_node_levels(dbo, schema, node_table, lion_table, rpl_table):
    # builds a lookup table of nodes with the node level from the RB view
    # generic view doesn't have node level
    # this is only used for ramps (some road have split levels where we want to keep as intersection),
    # but ramps passing over a street at a different level should be excluded

    dbo.query("""
                drop table if exists {s}.rb_to_generic_node_levels;
                create table {s}.rb_to_generic_node_levels as
                
                select nodeid, count(*) as levels--, geom
                from (
                    select distinct n.nodeid, l.nodelevelt--, n.geom
                    from {s}.{n} as n
                    -- join node to rpl on g nodes (to)
                    join {s}.{r} as t on n.nodeid=t.g_tond
                    -- join lion on rb nodes (to) to get level
                    join {s}.{l} l on l.nodeidto::int = t.r_tond
                    where l.featuretyp in ('0', '6', 'C') 
                    ------------------------------------fails for ints with ramps and devided rds without other x-street
                    and l.rb_layer !='G'
                    --------------------------------------------------------------------------------------------------
                    union 
                    
                    select distinct n.nodeid, l.nodelevelf--, n.geom
                    from {s}.{n} as n
                    -- join node to rpl on g nodes (to)
                    join {s}.{r} as t on n.nodeid=t.g_frnd
                    -- join lion on rb nodes (to) to get level
                    join {s}.{l} l on l.nodeidfrom::int = t.r_frnd
                    where l.featuretyp in ('0', '6', 'C') 
                    ------------------------------------fails for ints with ramps and devided rds without other x-street
                    and l.rb_layer !='G'
                    --------------------------------------------------------------------------------------------------
                ) as nl group by nodeid--, geom;
            """.format(s=schema, n=node_table, l=lion_table, r=rpl_table))
    dbo.query("""
                drop table if exists {s}.generic_node_levels;

                create table {s}.generic_node_levels as
                
                select nodeid, count(*) as levels--, geom
                from (
                    select nodeidfrom::int as nodeid, nodelevelf
                    from {s}.{l} l
                    left outer join {s}.rb_to_generic_node_levels as nl on nl.nodeid = l.nodeidfrom::int
                    where l.featuretyp in ('0', '6', 'C') and nl.nodeid is null
                    union
                    select nodeidto::int as nodeid, nodelevelt
                    from {s}.{l} l
                    left outer join {s}.rb_to_generic_node_levels as nl on nl.nodeid = l.nodeidto::int
                    where l.featuretyp in ('0', '6', 'C') and nl.nodeid is null
                ) as gnl group by nodeid;
               """.format(s=schema, l=lion_table, r=rpl_table))
    dbo.query("""
                    drop table if exists {s}.node_levels;

                    create table {s}.node_levels as
                    
                    select nodeid, levels from {s}.generic_node_levels
                    union
                    select nodeid, levels from {s}.rb_to_generic_node_levels
                    
                   """.format(s=schema))

    # override node level=1 for cases listed as virtual intersection
    # this is needed because streets where rb_layer='B' aren't in rpl pointer file so the level count may be wrong
    dbo.query("""
                update {s}.node_levels l
                set levels=2
                from {s}.{n} as n
                where l.nodeid=n.nodeid and n.vintersect='VirtualIntersection'
    """.format(s=schema, n=node_table))


@db2.timeDec
def build_street_name_table(dbo, schema=params.WORKING_SCHEMA, lion=params.LION, node=params.NODE):
    # Make a street name -> node lookup table
    # if ramp is involved use 'Ramp' as street name
    #   this should avoid including two ramps intersection as an intersection
    dbo.query("""drop table if exists {0}.node_stnameFT;
                create table {0}.node_stnameFT as (
                    select left(node, length(node)-1)::int as node, -- remove nodelevel from nodeid
                    /*case when ramp = True then 'Ramp' else street end as*/--removed because it was over clusering
                     street, ramp, 0 as master
                    from (
                        select nodeidfrom||nodelevelf as node, street, ramp
                        from {0}.{1} where exclude = False or ramp = True
                        group by  nodeidfrom||nodelevelf, street, ramp
                     
                        union
                     
                        select nodeidto||nodelevelt as node, street, ramp
                        from {0}.{1} where exclude = False or ramp = True
                        group by nodeidto||nodelevelt, street, ramp
                    ) as included 
                );""".format(schema, lion))
    # update node intersection flag where there are more than 1 unique street names
    print 'Updating intersection flag...'
    # update standard intersections
    dbo.query("""
            update {s}.{n} set is_int = True
            from (select node, count(distinct street) 
                    from {s}.node_stnameFT 
                    where ramp = False
                    group by node having count(distinct street) >1
                ) as i
            where {s}.{n}.nodeid = i.node
    """.format(s=schema, n=node))
    # update where ramp intersects with a a street
    dbo.query("""
    -- standard
    update {s}.node set is_int = True
    from (
        select street.* 
        from (select distinct node, street from {s}.node_stnameFT where ramp = False) street
        join (select distinct node, 'ramp' from {s}.node_stnameFT where ramp = True) ramp
        on street.node = ramp.node
        /*left outer*/ join {s}.node_levels as levels 
        on street.node = levels.nodeid
        where levels.levels = 1 --or levels.levels is null
    ) as i
    where node.nodeid = i.node;
    """.format(s=schema))
    define_double_segments(dbo, schema, lion, node)
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
# Step 5: Build simplified network
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


def define_double_segments(dbo, schema=params.WORKING_SCHEMA, lion=params.LION, node=params.NODE):
    # identify if there are more than 1 segment with the same nodeid from/to
    # not including ramps, generates too many false positives
    dbo.query("""
                drop table if exists {s}.doubles;
                create table {s}.doubles as 
                select nf::int, nt::int, count(street)
                from (
                    select  nodeidfrom as nf, nodeidto as nt, street
                    from {s}.{l}
                    where exclude = False and ramp = False
                    group by  nodeidfrom, nodeidto, street
                    union
                    select  nodeidto as nf, nodeidfrom as nt, street
                    from {s}.{l}
                    where exclude = False and ramp = False 
                    group by  nodeidfrom, nodeidto, street
                ) as t
                group by nf, nt
                having count(street) > 1
    """.format(s=schema, l=lion))
    # remove these nodes is intersection flag
    dbo.query("""
                update {s}.{n} set is_int = False 
                from {s}.doubles d
                where {s}.{n}.nodeid = d.nf
                or {s}.{n}.nodeid = d.nt
                """.format(s=schema, n=node)
              )
    # update the intersection flag for these nodes if they have 3 street names
    print 'Updating intersection flag for doubles...'
    dbo.query("""
            update {s}.{n} set is_int = True
            from (select node, count(distinct street) 
                    from {s}.node_stnameFT n
                    join {s}.doubles d on (n.node = d.nf or n.node = d.nt)
                    group by node having count(distinct street) > 2
                ) as i
            where {s}.{n}.nodeid = i.node
    """.format(s=schema, n=node))
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
# Step 4: Build street network graph
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


@db2.timeDec
def node_names(dbo, node_street_names, node_is_intersection, schema=params.WORKING_SCHEMA, node=params.NODE):
    def de_suffix(name):
        # remove some precision in street names to create better sets
        d = {' WEST': ' DIR',
             ' EAST': ' DIR',
             ' SOUTH': ' DIR',
             ' NORTH': ' DIR',
             ' EXIT': ' DIR',
             ' ENTRANCE': ' DIR',
             ' APPROACH': ' DIR',
             ' NORTHBOUND': ' DIR',
             ' NB': ' DIR',
             ' SOUTHBOUND': ' DIR',
             ' SB': ' DIR',
             ' EASTBOUND': ' DIR',
             ' EB': ' DIR',
             ' WESTBOUND': ' DIR',
             ' WB': ' DIR'}
        for i in d:
            if name not in {'WEST STREET', 'SOUTH STREET', 'NORTH STREET', 'EAST STREET',
                            'WEST AVENUE', 'SOUTH AVENUE', 'NORTH AVENUE', 'EAST AVENUE',
                            'WEST BOULEVARD', 'SOUTH BOULEVARD', 'NORTH BOULEVARD', 'EAST BOULEVARD',
                            'WEST LOOP', 'SOUTH LOOP', 'NORTH LOOP', 'EAST LOOP',
                            'WEST DRIVE', 'SOUTH DRIVE', 'NORTH DRIVE', 'EAST DRIVE',
                            'WEST ROAD', 'SOUTH ROAD', 'NORTH ROAD', 'EAST ROAD',
                            'JUNIPER BOULEVARD NORTH', 'JUNIPER BOULEVARD SOUTH',
                            'PROSPECT PARK WEST', 'AVENUE N', 'AVENUE S', 'AVENUE E',
                            'AVENUE W'}:
                if name:
                    name = name.replace(i, d[i])
        return name
    # build the dictionary of the street names for each node
    q = dbo.query("""
                        select n.nodeid, n.is_int, s.street 
                        from {s}.{n} n 
                        join {s}.node_stnameFT s
                        on n.nodeid = s.node
                    """.format(s=schema, n=node))
    for row in tqdm(q.data):
        node, isint, street = row
        node = int(node)
        # normalize the street name
        street = de_suffix(street)
        # update the intersection diction with is_int flag
        node_is_intersection[node] = isint
        # add street name(s) to dict
        node_street_names[node][0].add(street)
        # if node not in node_street_names.keys():
        #     node_street_names[node] = [{street}, 0]  # [set([street]), masterid]
        # else:
        #     node_street_names[node][0].add(street)
    return node_street_names, node_is_intersection


@db2.timeDec
def graph(dbo, node_next_steps=params.nodeNextSteps, schema=params.WORKING_SCHEMA, lion=params.LION):
    # builds non-directional graph of included street network
    q = dbo.query("select street, segmentid, nodeidfrom, nodeidto from {0}.{1} where exclude = False".format(
        schema, lion))
    for row in tqdm(q.data):
        street, segmentid, nodeidfrom, nodeidto = row
        node_next_steps[int(nodeidfrom)][street].add(int(nodeidto))
        node_next_steps[int(nodeidto)][street].add(int(nodeidfrom))
    return node_next_steps


@db2.timeDec
def search(node_street_names, street_set, node_is_intersection, node_next_steps):
    """gets intersection to intersection segments or collection of segments"""
    # get blocks for every intersection node
    for startNode in node_street_names.keys():
        if node_is_intersection[startNode]:
            # blocks formed for each available street name
            for street in node_next_steps[startNode].keys():
                # run DFS block maker for each street of the node
                queue = list(node_next_steps[startNode][street])
                while len(queue) > 0:
                    to_node = queue.pop()
                    street_set.append(
                        go_to_end(
                            street, [startNode], {to_node}, node_is_intersection, node_next_steps
                        )
                    )
    return node_street_names, street_set


def go_to_end(street, done, todo, node_is_intersection, node_next_steps):
    # forms blocks using DFS
        # 1(i) - 2 - 3 -4(i) - 5
        # done = [1], todo_set = {2}
            # [1-2], {1,3}
                # [1,2,3], {2,4}
                    # [1,2,3,4]
        # [5], {4,6}...
    # anything left in the queue?
    if len(todo) == 0:
        # you're done exit
        return done
    else:
        # get new starting point
        start = todo.pop()
        # add it to the visited nodes
        done.append(start)
        # make sure it is not an ending point
        if not node_is_intersection[start]:
            # see where we can go from here
            for i in node_next_steps[start][street]:
                # make sure we haven't done it yet
                if i not in done:
                    # add it to the queue
                    todo.add(i)
        return go_to_end(street, done, todo, node_is_intersection, node_next_steps)


@db2.timeDec
def generate_blocks_from_masterids(dbo,
                                   alt_graph=params.altGraph,
                                   street_set=params.streetSet,
                                   mft_1_dict=params.mft1Dict,
                                   folder=params.FOLDER,
                                   lion=params.LION,
                                   schema=params.WORKING_SCHEMA):
    """gets simplest master segments based on from to nodes, intersections only"""
    sql = """select segmentid, nodeidfrom, nodeidto
            from {0}.{1} where exclude is false
            group by segmentid, nodeidfrom, nodeidto""".format(schema, lion)

    data = dbo.query(sql)
    # build local dict of seg node ids from-to to build mft
    for row in tqdm(data[0]):
        # build up simple graph
        # node : [[other node , segid], [other node , segid]]
        segment, fromid, toid = row
        alt_graph[int(fromid)].append([int(toid), segment])
        alt_graph[int(toid)].append([int(fromid), segment])
    print 'done graphing\n'
    mft = 0
    to_write_out = []
    # go through all nodes in each block and get all of the segmentids that have
    # from and to nodes both in the block list
    for block in tqdm(street_set):  # streetSet is a set of nodes that together make up a block
        if block:
            mft += 1
            for node in block:
                for other_node, seg in alt_graph[node]:
                    if other_node in block:  # both ends of the street are in the block
                        mft_1_dict[mft].append(seg)
                        to_write_out.append([mft, seg])
    print 'Writing out csv\n'
    db2.write(os.path.join(folder, "mft.csv"), to_write_out, ['mft', 'segment'])
    dbo.query(
        'drop table if exists {0}.tempMaster; CREATE TABLE {0}.tempMaster (mft varchar(10), seg varchar(10))'.format(
            schema
            )
    )
    print 'Adding outputs to DB\n'
    db2.add_data_to_pg(dbo, 'tempMaster', schema, ',', os.path.join(folder, "mft.csv"))
    print 'Updating lion with mfts\n'
    dbo.query("""update {0}.{1} as l
                       set mft = t.mft::int
                       -- added min(mft) sice blockes are defined in both directions
                       -- and mft attribution was random
                       from (select seg, min(mft) as mft from {0}.tempmaster group by seg) as t
                       where l.segmentid =t.seg""".format(schema, lion))
    dbo.query("drop table {}.tempmaster".format(schema))
    return alt_graph


def street_name_key(street_set):
    street_set = list(street_set)
    street_set.sort()
    street_set = str(street_set)
    return street_set[1:-1]


@db2.timeDec
def intersection_cluster_dict(node_street_names, cluster_intersections, node_is_intersection):
    # get street names by intersetion
    for node in tqdm(node_street_names.keys()):
        # only consider intersections
        if node_is_intersection[node]:
            # order the street names
            street_key = street_name_key(node_street_names[node][0])
            # add node to the street name set dict
            cluster_intersections[street_key][0].add(node)
    return cluster_intersections


def merge_clusters(cluster_intersections, key1, key2):
    cluster = dict(cluster_intersections)
    # #clusterIntersections = {sorted-street-names: [set([nodes]), masterID]}
    # merge nodes from key2 into key 1
    cluster[key1][0] = cluster[key1][0].union(cluster[key2][0])
    # copy full cluster to second key (includes all nodes and master)
    cluster[key2] = cluster[key1]
    return cluster


def sub_get_doubles(dbo, schema):
    data = dbo.query("""select nf from {0}.doubles
                        union select nt from {0}.doubles
                    """.format(schema))
    d = set()
    for i in data.data:
        d.add(i[0])
    return d


@db2.timeDec
def subset_merge_with_superset(dbo, schema, cluster_intersections, node_street_names):
    # sample problem
        # set(['BROADWAY', 'WEST 225 STREET', 'BROADWAY BRIDGE'])
        # set(['BROADWAY', 'BROADWAY BRIDGE'])
    # for each street set
    # check if it is a subset of another set of street names
    # if yes  merge into single master - will be split later if too far and in diff precinct
    # TODO: need to create a better solution for doubles;
    # TODO contd. for now, skip any node that has a double block condition (ex. 12 Ave / Joe Dimaggio)

    def sub_get_name_set_from_str(str_set, doublenodes):
        sample_node = [i for i in cluster_intersections[str_set][0] if i not in doublenodes]
        if sample_node:
            # remove ramp from set for testing, otherwise all of the ramps+service road intersections get merged
            names = set(node_street_names[sample_node[0]][0])
            # if 'Ramp' in names:
            #     names.remove('Ramp')
            return names
    # double block conditions to skip
    double_nodes = sub_get_doubles(dbo, schema)
    # list of sets to test
    street_set_list = [sub_get_name_set_from_str(str_street_set, double_nodes)
                       for str_street_set in cluster_intersections.keys()]
    for str_street_set in tqdm(cluster_intersections.keys()):
        # ex. "'QUEENS BOULEVARD', 'VAN DAM STREET'"
        # get node to get street names as set
        street_name_set = sub_get_name_set_from_str(str_street_set, double_nodes)
        # check for super set
        for super_set in street_set_list:
            if all([super_set, street_name_set]) and \
                            'Ramp' not in super_set and \
                            len(street_name_set) > 1 and \
                            street_name_set != super_set and \
                    street_name_set.issubset(super_set):
                # print 'Merging {} with, {}'.format(str_street_set, street_name_key(super_set))
                # merge clusters
                # clusterIntersections[street_name_key(super)]
                cluster_intersections = merge_clusters(
                    cluster_intersections,
                    str_street_set,
                    street_name_key(super_set))
    return cluster_intersections


@db2.timeDec
def master_intersection_first_pass(cluster_intersections, node_master, master_node):
    master = 0
    for street_set in cluster_intersections.keys():
        master += 1
        # add masterid to each set of street names
        cluster_intersections[street_set][-1] = master
        # update node: master dict and master : node dict
        # This may be overkill, but it allows for 3 way lookups by street names, nodeid, or masterid
        for node in cluster_intersections[street_set][0]:
            node_master[node] = master
            master_node[master].append(node)
    return cluster_intersections, node_master, master_node
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
# Step 6: Revise simplified network
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


def distance(x1, y1, x2, y2):
    # returns euclidian distance
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)


def get_nodes_pct(dbo, schema):
    node_pct_dict = defaultdict(list)
    data = dbo.query("select distinct nodeid, precinct from {}.c_intersection_name".format(schema))
    for row in data.data:
        node_pct_dict[row[0]].append(row[1])
    del data
    return node_pct_dict


# =====================================================================================================================

def get_all_database_needs_for_distance_check(dbo, schema, node_table, precinct_table):
    # get the precinct for each node
    # data = dbo.query("""
    #                 select nodeid::int as nodeid, precinct
    #                         from {0}.{1} as n
    #                         join {0}.{2} as l on l.nodeidfrom::int=nodeid::int
    #                         join {0}.{2} as ll on ll.nodeidfrom::int=nodeid::int
    #                         join {3} as p on st_within(n.geom, p.geom)
    #                         where l.street !=ll.street
    #                     union
    #                         select nodeid::int as nodeid, precinct
    #                         from {0}.{1} as n
    #                         join {0}.{2} as l on l.nodeidto::int=nodeid::int
    #                         join {0}.{2} as ll on ll.nodeidto::int=nodeid::int
    #                         join {3} as p on st_within(n.geom, p.geom)
    #                         where l.street !=ll.street
    #                     union
    #                         select nodeid::int as nodeid, precinct
    #                         from {0}.{1} as n
    #                         join {0}.{2} as l on l.nodeidfrom::int=nodeid::int
    #                         join {0}.{2} as ll on ll.nodeidto::int=nodeid::int
    #                         join {3} as p on st_within(n.geom, p.geom)
    #                         where l.street !=ll.street
    #                 """.format(schema, node_table, lion_table, precinct_table))
    data = dbo.query("""select nodeid::int as nodeid, precinct
                            from {0}.{1} as n join {2} as p 
                            on st_dwithin(n.geom, p.geom, 10)
                    """.format(schema, node_table, precinct_table))
    pct_lookup = dict()
    for row in data[0]:
        node, pct = row
        pct_lookup[node] = pct
    del data
    # get the neighboring pcts for each pct
    data = dbo.query("""select p1.precinct, p2.precinct
                           from {0} as p1
                           join {0} as p2
                           on st_intersects(p1.geom, p2.geom)
                           """.format(precinct_table))
    pct_neighbors = defaultdict(set)
    for row in tqdm(data[0]):
        p1, p2 = row
        pct_neighbors[p1].add(p2)
    del data
    data = dbo.query("select nodeid, st_x(geom), st_y(geom) from {sch}.{nt}".format(sch=schema, nt=node_table))
    node_coords = dict()
    for row in tqdm(data[0]):
        nd, x, y = row
        node_coords[nd] = (x, y)
    del data, row
    return pct_lookup, pct_neighbors, node_coords


def find_masters_with_distant_nodes(master_nodes, node_is_int, node_coords):
    tolerance = 1000  # used to be 300.........
    problems = list()  # list of masterIDs to be split
    for master in master_nodes.keys():
        node_list = master_nodes[master]
        for pair in itertools.product(list(node_list), repeat=2):
            n1, n2 = pair
            if node_is_int[n1] and node_is_int[n2]:
                (x1, y1), (x2, y2) = node_coords[n1], node_coords[n2]
                if distance(x1, y1, x2, y2) > tolerance:
                    problems.append(master)
    return problems


@db2.timeDec
def update_problem_groups(problems, pct_lookup, node_master, master_node, street_names):
    problems = set(problems)
    for problem_master_id in tqdm(problems):
        print 'Updating {} ({})'.format(problem_master_id, master_node[problem_master_id])
        # group nodes in master by pct
        temp_pct_dict = defaultdict(list)
        for node in master_node[problem_master_id]:
            temp_pct_dict[pct_lookup[node]].append(node)
        # 1st clean up existing data
        del master_node[problem_master_id]
        # update masterids for each pct
        for p in temp_pct_dict.keys():
            # get next availible masterid
            mx = max(master_node.keys()) + 1
            for node in temp_pct_dict[p]:
                # update masters
                node_master[node] = mx
                street_names[node][1] = mx
                master_node[mx].append(node)
    print 'CLUSTER INTERSECTION DICT NO LONGER ACCURATE'
    return node_master, master_node, street_names


def merge_masters(nodes_to_merge, node_master, master_node):
    masters_to_merge = [node_master[n] for n in nodes_to_merge]
    new_master = masters_to_merge[0]
    # get all of the nodes that belong to the masters of the nodes to merge
    # need to make sure to bring sibling nodes along in the merger
    all_nodes_to_merge = list()
    for n in [master_node[m] for m in masters_to_merge]:
        all_nodes_to_merge += n
    for n in all_nodes_to_merge:
        node_master[n] = new_master
    for m in masters_to_merge:
        if m != new_master:
            master_node[new_master] = master_node[new_master] + master_node[m]
            del master_node[m]
    return node_master, master_node


@db2.timeDec
def near_by_simple(dbo, schema, node_table, node_master, master_node, search_distance):
    # TODO: this is very slow and needs to be refactored
    # get all nodes within seach distance
    for node in tqdm(node_master.keys()):
        near_nodes = dbo.query("""
                select near.nodeid 
                from {s}.{n} as near, 
                (
                    select geom 
                    from {s}.{n}
                    where nodeid = {nd}
                ) as src
                where st_dwithin(near.geom, src.geom, {sd})
                and near.is_int = True
                and near.nodeid != {nd}
        """.format(s=schema, n=node_table, nd=node, sd=search_distance))
        node_master, master_node = merge_masters([int(n[0]) for n in near_nodes.data]+[node], node_master, master_node)
    return node_master, master_node


@db2.timeDec
def triangle(node_coords, next_steps, node_master, master_node, node_is_int, triangle_dist=150):
    # for every intersection
    #     get all nodes 1 step away
    #     check the distance between origin node and 1 step nodes
    #     if less than predefined distance group
    triangles_list = list()
    for node in tqdm(node_master.keys()):
        x, y = node_coords[node]
        one_hop_nodes = dict()
        for s in next_steps[node]:  # get steets steaming from node
            for nd in params.nodeNextSteps[node][s]:  # get the next nodes down each street
                if node_is_int[nd]:  # ignore non-intersection nodes
                    x1, y1, = node_coords[nd]
                    d = distance(x, y, x1, y1)
                    if d < triangle_dist:  # check distance
                        one_hop_nodes[nd] = d
        if len(one_hop_nodes) == 2:
            # check if each of the 1 hop nodes will also connect to eachother in 1 hop
            n1, n2 = one_hop_nodes.keys()
            for street_name in next_steps[n1].keys():
                if n2 in next_steps[n1][street_name]:
                    new_tri = set(one_hop_nodes.keys())
                    new_tri.add(node)
                    if new_tri not in triangles_list:
                        triangles_list.append(new_tri)
                        merge_masters(new_tri, node_master, master_node)
    return triangles_list, node_master, master_node


@db2.timeDec
def update_db_nodes(dbo, schema, node_table, folder, node_master):
    # prep dict to write to csv
    data_to_write = [[node, node_master[node]] for node in node_master]
    # write out data
    db2.write(os.path.join(folder, "nodeMaster.csv"), data_to_write)  # , ['nodeid', 'masterid'])
    # create temp_table
    dbo.query('''drop table if exists {0}.nodeMaster; CREATE TABLE {0}.nodeMaster 
                (nodeid numeric(10,0), masterid numeric(10,0))
            '''.format(schema))
    db2.add_data_to_pg(dbo, 'nodeMaster', schema, ',', os.path.join(folder, "nodeMaster.csv"))
    dbo.query("""update {0}.{1} as n
                set masterid = nm.masterid
                from {0}.nodemaster as nm
                where n.nodeid = nm.nodeid
            """.format(schema, node_table))
    dbo.query('drop table {}.nodemaster'.format(schema))
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
# Step 7: Generate stable master ids
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


@db2.timeDec
def stabilize_masters(dbo, node_table, schema):
    dbo.query("alter table {s}.{n} add newid int;".format(s=schema, n=node_table))
    dbo.query("""drop table if exists {s}.new_masters;
                create table {s}.new_masters as
                select masterid, max(nodeid) newid from {s}.{n} group by masterid;""".format(s=schema, n=node_table))
    dbo.query("""
                update {s}.{n} as n
                set newid = nm.newid
                from {s}.new_masters as nm
                where n.masterid = nm.masterid;""".format(s=schema, n=node_table))
    dbo.query("update {s}.{n} set masterid = newid;".format(s=schema, n=node_table))
    dbo.query("alter table {s}.{n} drop column newid;".format(s=schema, n=node_table))
    dbo.query("drop table if exists {s}.new_masters;".format(s=schema))
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
#     8. Rebuild street network with new masterid info
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


@db2.timeDec
def remap_from_to_masters(dbo, lion_table, node_table, schema):
    print 'Updating Master From/To segments'
    # update the from nodes for simple streets
    # update the to nodes for simple streets

    # make the min master as from and the max master as to
    dbo.query("""drop table if exists {0}.tempFrom; 
                create table {0}.tempFrom as (
                    select p1.mft, mn, mx from (
                        select mft, min(masterid) as mn from (
                            select mft, masterid 
                            from {0}.{1} as l
                            join {0}.{2} as n
                            on nodeidfrom::int = nodeid
                            where mft is not null and masterid is not null
                        union
                            select mft, masterid 
                            from {0}.{1} as l
                            join {0}.{2} as n
                            on nodeidto::int = nodeid
                            where mft is not null and masterid is not null
                        ) as mn group by mft
                    ) as p1 join (
                        select mft, max(masterid) as mx from (
                            select mft, masterid 
                            from {0}.{1} as l
                            join {0}.{2} as n
                            on nodeidfrom::int = nodeid
                            where mft is not null and masterid is not null
                        union
                            select mft, masterid 
                            from {0}.{1} as l
                            join {0}.{2} as n
                            on nodeidto::int = nodeid
                            where mft is not null and masterid is not null
                        ) as mx group by mft
                    ) as p2 on p1.mft = p2.mft
                );""".format(schema, lion_table, node_table))
    # update lion with mfrom and to values
    dbo.query("""update {s}.{l}  as l
                    set masteridfrom = mn, masteridto= mx
                    from {s}.tempFrom
                    where l.mft = tempFrom.mft 
            """.format(s=schema, l=lion_table))
    dbo.query("drop table {0}.tempFrom".format(schema))
    stabilize_mfts(dbo, lion_table, schema)
    remap_blocks(dbo, lion_table, node_table, schema)


@db2.timeDec
def stabilize_mfts(dbo, lion_table, schema):
    dbo.query("alter table {s}.{n} add newid int;".format(s=schema, n=lion_table))
    dbo.query("""drop table if exists {s}.new_mft;
                create table {s}.new_mft as
                select masteridfrom, masteridto, max(segmentid ) as newid
                from {s}.{n}
                where masteridfrom is not null and masteridto is not null
                group by masteridfrom, masteridto
                """.format(s=schema, n=lion_table))
    dbo.query("""
                update {s}.{n} as n
                set newid = nm.newid::int
                from {s}.new_mft as nm
                where n.masteridfrom = nm.masteridfrom and 
                n.masteridto = nm.masteridto;""".format(s=schema, n=lion_table))
    dbo.query("update {s}.{n} set mft = newid;".format(s=schema, n=lion_table))
    dbo.query("alter table {s}.{n} drop column newid;".format(s=schema, n=lion_table))
    dbo.query("drop table if exists {s}.new_mft;".format(s=schema))


def remap_blocks(dbo, lion_table, node_table, schema):
    dbo.query("""
        drop table if exists {s}.temp_masters;
        create table {s}.temp_masters as 
        select mft, n.masterid 
        from {s}.{l} l join {s}.{n} n on l.nodeidfrom::int=n.nodeid 
        where masterid is not null
        union 
        select mft, n.masterid 
        from {s}.{l} l join {s}.{n} n on l.nodeidto::int=n.nodeid 
        where masterid is not null;""".format(l=lion_table, n=node_table, s=schema))
    dbo.query("""delete from {s}.temp_masters where mft in (
                select mft from {s}.temp_masters group by mft having count(*) <3
            )""".format(s=schema))  # -------- only fix forks where there is an issue (was removing needed masters)
    dbo.query("""        
        drop table if exists {s}.temp_masters_segs;
        create table {s}.temp_masters_segs as 
        select segmentid, 
        case when s.masteridfrom is not null then s.masteridfrom 
            when min(tm.masterid) != s.masteridto then min(tm.masterid)
            else max(tm.masterid) end as masteridfrom,
        case when s.masteridto is not null then s.masteridto 
            when min(tm.masterid) != s.masteridfrom then min(tm.masterid)
            else max(tm.masterid) end as masteridto
        from
        (
            select segmentid, mft, nodeidfrom, nodeidto, n.masterid as masteridfrom, n2.masterid as masteridto
            from {s}.{l} l join {s}.{n} n on l.nodeidfrom::int=n.nodeid join {s}.{n} n2 on l.nodeidto::int=n2.nodeid
        ) as s join {s}.temp_masters as tm on s.mft = tm.mft
        group by segmentid, s.masteridfrom, s.masteridto;""".format(l=lion_table, n=node_table, s=schema))
    dbo.query("""        
        update {s}.{l} l
        set masteridfrom = t.masteridfrom, masteridto=t.masteridto
        from {s}.temp_masters_segs t 
        where l.segmentid = t.segmentid; 

        drop table if exists {s}.temp_masters_segs;
        drop table if exists {s}.temp_masters;
        """.format(l=lion_table, s=schema))
    print 'Fixed forking masters from/to issue\n'
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
#     9. Update roadbeds
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


@db2.timeDec
def update_roadbeds(dbo, schema, lion_table, tbl_rpl):
    # get all associated roadbed segments and update with centerline's master info
    dbo.query("""
                drop table if exists {s}.rb_mft;
                create table {s}.rb_mft as 
                select l.mft, l.masteridfrom, l.masteridto, l.segmentid, r.segmentidr
                from {s}.{l} as l
                join {s}.{r} as r on l.segmentid::int = r.segmentidg;
            """.format(s=schema, l=lion_table, r=tbl_rpl))
    dbo.query("""
                update {s}.{l} as l
                set mft = r.mft, masteridfrom = r.masteridfrom, masteridto=r.masteridto
                from rb_mft as r
                where l.segmentid::int = r.segmentidr
            """.format(s=schema, l=lion_table))


@db2.timeDec
def update_roadbed_nodes(dbo, schema, node_table, tbl_rpl):
    # set centerline intersection flag
    dbo.query("""
                alter table {s}.{n} add is_cntrln_int bool default False; 
                update {s}.{n} set is_cntrln_int = is_int;
            """.format(s=schema, r=tbl_rpl, n=node_table))

    # node -> rpl(rb) -> node(gen) connects rb node to gen node
    dbo.query("""
                drop table if exists {s}.rb_masterids;
                create table {s}.rb_masterids as 
                select n.nodeid, nn.masterid
                from {s}.{n} as n
                -- join node to rpl on rb nodes (to)
                join {s}.{r} as t on n.nodeid=t.r_tond
                -- join node to rpl on centerline nodes (to) 
                join {s}.{n} as nn on g_tond = nn.nodeid
                where n.masterid is null and nn.masterid is not null
                union 
                select n.nodeid, nn.masterid
                from {s}.{n} as n
                -- join {s}.{n} to rpl on rb nodes (from)
                join {s}.{r} as t on n.nodeid=t.r_frnd
                -- join node to rpl on centerline nodes (from) 
                join {s}.{n} as nn on g_frnd = nn.nodeid
                where n.masterid is null and nn.masterid is not null
                -- this might be over kill, but just in case join in opposite direction 
                union
                select n.nodeid, nn.masterid
                from {s}.{n} as n
                -- join node to rpl on rb nodes (to)
                join {s}.{r} as t on n.nodeid=t.g_tond
                -- join node to rpl on centerline nodes (to) 
                join {s}.{n} as nn on r_tond = nn.nodeid
                where n.masterid is null and nn.masterid is not null
                union 
                select n.nodeid, nn.masterid
                from {s}.{n} as n
                -- join node to rpl on rb nodes (from)
                join {s}.{r} as t on n.nodeid=t.g_frnd
                -- join node to rpl on centerline nodes (from) 
                join {s}.{n} as nn on r_frnd = nn.nodeid
                where n.masterid is null and nn.masterid is not null;
                """.format(s=schema, r=tbl_rpl, n=node_table))
    # update node table rb nodes with masters from centerline
    dbo.query("""
                -- update node table
                update {s}.{n} as n
                set masterid = r.masterid
                from {s}.rb_masterids as r
                where n.nodeid=r.nodeid
                and n.masterid is null;
             """.format(s=schema, r=tbl_rpl, n=node_table))
    # housekeeping
    dbo.query("""
                update {s}.{n} set is_int = true where masterid is not null; 
                -- cleanup 
                drop table {s}.rb_masterids;
            """.format(s=schema, r=tbl_rpl, n=node_table))

    # second run - TODO: this needs to be re-thought a little
    # jumps levels out and may over select in some cases where the rpl is a little funky
    dbo.query("""
                    drop table if exists {s}.rb_masterids;
                    create table {s}.rb_masterids as 
                    select n.nodeid, nn.masterid
                    from {s}.{n} as n
                    -- join node to rpl on rb nodes (to)
                    join {s}.{r} as t on n.nodeid=t.r_tond
                    -- join node to rpl on centerline nodes (to) 
                    join {s}.{n} as nn on g_tond = nn.nodeid
                    where n.masterid is null and nn.masterid is not null
                    union 
                    select n.nodeid, nn.masterid
                    from {s}.{n} as n
                    -- join {s}.{n} to rpl on rb nodes (from)
                    join {s}.{r} as t on n.nodeid=t.r_frnd
                    -- join node to rpl on centerline nodes (from) 
                    join {s}.{n} as nn on g_frnd = nn.nodeid
                    where n.masterid is null and nn.masterid is not null
                    -- this might be over kill, but just in case join in opposite direction 
                    union
                    select n.nodeid, nn.masterid
                    from {s}.{n} as n
                    -- join node to rpl on rb nodes (to)
                    join {s}.{r} as t on n.nodeid=t.g_tond
                    -- join node to rpl on centerline nodes (to) 
                    join {s}.{n} as nn on r_tond = nn.nodeid
                    where n.masterid is null and nn.masterid is not null
                    union 
                    select n.nodeid, nn.masterid
                    from {s}.{n} as n
                    -- join node to rpl on rb nodes (from)
                    join {s}.{r} as t on n.nodeid=t.g_frnd
                    -- join node to rpl on centerline nodes (from) 
                    join {s}.{n} as nn on r_frnd = nn.nodeid
                    where n.masterid is null and nn.masterid is not null;
                    """.format(s=schema, r=tbl_rpl, n=node_table))
    # update node table rb nodes with masters from centerline
    dbo.query("""
                    -- update node table
                    update {s}.{n} as n
                    set masterid = r.masterid
                    from {s}.rb_masterids as r
                    where n.nodeid=r.nodeid
                    and n.masterid is null;
                 """.format(s=schema, r=tbl_rpl, n=node_table))
    # housekeeping
    dbo.query("""
                    -- fix intersection flag
                    update {s}.{n} set is_int = true where masterid is not null; 
                    -- cleanup 
                    drop table {s}.rb_masterids;
                """.format(s=schema, r=tbl_rpl, n=node_table))

# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
#     10.  Generate corridors
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
# add street names
@db2.timeDec
def corridor_names(dbo):
    dbo.query("""-- p street is the street name without directional prefix
                alter table {s}.{l} drop column if exists corridor_street;
                alter table {s}.{l} add corridor_street text;
                update {s}.{l} set corridor_street = case 
                    -- edge case streets where the direction is the name
                    when street in ('SOUTH STREET', 'WEST STREET','NORTH STREET', 'EAST STREET',
                        'SOUTH AVENUE', 'WEST AVENUE','NORTH AVENUE', 'EAST AVENUE',
                        'SOUTH BOULEVARD', 'WEST BOULEVARD','NORTH BOULEVARD', 'EAST BOULEVARD',
                        'SOUTH LOOP', 'WEST LOOP','NORTH LOOP', 'EAST LOOP',
                        'SOUTH ROAD', 'WEST ROAD','NORTH ROAD', 'EAST ROAD',
                        'SOUTH DRIVE', 'WEST DRIVE','NORTH DRIVE', 'EAST DRIVE',
                        'WEST END AVENUE', 'EAST END AVENUE', 'SOUTH END AVENUE', 'NORTH END AVENUE',
                        'WEST END DRIVE', 'EAST END DRIVE', 'SOUTH END DRIVE', 'NORTH END DRIVE',
                        'EAST BROADWAY', 'WEST BROADWAY'
                    ) then street
                        -- remove direction from names
                        when street like 'EAST %%' then replace(street, 'EAST ', '')
                        when street like 'WEST %%' then replace(street, 'WEST ', '')
                        when street like 'SOUTH %%' then replace(street, 'SOUTH ', '')
                        when street like 'NORTH %%' then replace(street, 'NORTH ', '')
                        else street
                    end   
    """.format(s=params.WORKING_SCHEMA, l=params.LION))


# Dissolve on continuity and street name
@db2.timeDec
def dissolve_corridors(dbo):
    dbo.query("""
                alter table {s}.{l} drop column if exists cid; 
                alter table {s}.{l} add cid text; 

                drop table if exists {s}._corridor_;
                create table {s}._corridor_ as (
                select corridor_street, boro,  ST_CollectionExtract(unnest(ST_ClusterIntersecting(geom)),2) as geom
                from (
                        select corridor_street, greatest(lboro, rboro) as boro
                        ,ST_LineMerge(ST_LineMerge(((ST_Dump(st_linemerge(st_union(geom)))).geom))) as geom
                        from {s}.{l} where exclude = False -- 18D uses boolean 
                        group by corridor_street, greatest(lboro, rboro)
                    ) as p
                group by corridor_street, boro
                );
            """.format(s=params.WORKING_SCHEMA, l=params.LION))


# generate temp ID based on dissolve length
@db2.timeDec
def corridor_id(dbo):
    dbo.query("""
                drop table if exists {s}._corridor3_;
                create table {s}._corridor3_ as
                select corridor_street, boro, geom, 
                count(*) OVER (PARTITION BY boro, corridor_street ORDER BY st_length(geom) desc) AS cid
                from {s}._corridor_ 
                order by corridor_street, cid;
            """.format(s=params.WORKING_SCHEMA))
    # rename temp table
    dbo.query("""
                drop table if exists {s}._corridor_;
                alter table {s}._corridor3_ rename to _corridor_;
                update {s}._corridor_ set geom = st_setsrid(geom, 2263);
            """.format(s=params.WORKING_SCHEMA))
    # create ID number
    dbo.query("""                
                -- cid is boro - P street name - the order # by length of corridor desc 
                drop table if exists {s}._corridor2_;
                create table {s}._corridor2_ as (
                    select corridor_street, boro, cid,
                        (st_dump(geom)).path[1] as path,
                        (st_dump(geom)).geom as geom
                    from (
                        select boro, corridor_street, boro::varchar(1)||'-'||corridor_street||'-'||id::varchar(10) as cid, 
                        st_union(st_buffer(geom, 10)) as geom
                        from (
                            SELECT {s}._corridor_.boro, {s}._corridor_.corridor_street, len,
                            count(*) OVER (PARTITION BY {s}._corridor_.boro, {s}._corridor_.corridor_street ORDER BY len desc) AS id, 
                            {s}._corridor_.geom
                            FROM  {s}._corridor_ join (
                                select corridor_street, boro, cid, sum(st_length(geom)) as len
                                from {s}._corridor_
                                group by corridor_street, boro, cid
                            ) t on {s}._corridor_.boro=t.boro and {s}._corridor_.corridor_street=t.corridor_street and {s}._corridor_.cid=t.cid
                            ORDER BY boro, corridor_street, len desc
                        ) as d group by boro, corridor_street, boro::varchar(1)||'-'||corridor_street||'-'||id::varchar(10)
                        ) as buf
                    );

                """.format(s=params.WORKING_SCHEMA, l=params.LION))


# Add ID to LION
@db2.timeDec
def add_corridor_to_lion(dbo):
    dbo.query("""
                update {s}.{l} as l
                    set cid =b.cid
                    from {s}._corridor2_ as b
                    where st_within(l.geom, b.geom)
                    and l.corridor_street=b.corridor_street
                    and greatest(lboro, rboro)=b.boro
                    and exclude = False; -- 18D uses boolean;

                drop table {s}._corridor_;
                drop table {s}._corridor2_;
    """.format(s=params.WORKING_SCHEMA, l=params.LION))

# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
#     11.  Make master geom lookup tables
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


@db2.timeDec
def make_master_node_lookup(dbo, schema, node_table, version):
    # This creates a lookup table for display coordinates for nodes
    dbo.query("""
                drop table if exists {s}.temp_best_node;
                create table {s}.temp_best_node as 
                select min(nodeid) as node, dist, masterid
                from (
                    select ad.*
                    from (
                        select n.masterid, min(st_distance(n.geom, cent_geom)) as mdist
                        from {s}.{n} n
                        join (
                            select masterid, st_centroid(st_union(geom)) as cent_geom
                            from {s}.{n} 
                            where masterid != 0
                            group by masterid
                        ) cent on n.masterid = cent.masterid
                        where n.masterid != 0
                        group by n.masterid
                    ) as md
                    join (
                        select n.nodeid, n.masterid, st_distance(n.geom, cent_geom) as dist, n.geom
                        from {s}.{n} n
                        join (
                            select masterid, st_centroid(st_union(geom)) as cent_geom
                            from {s}.{n} 
                            where masterid != 0
                            group by masterid
                        ) cent on n.masterid = cent.masterid
                        where n.masterid != 0
                    ) as ad on md.masterid = ad.masterid and md.mdist = ad.dist
                )  d group by masterid, dist;
            """.format(s=schema, n=node_table))
    dbo.query("""
                drop table if exists {s}.master_node_geo_lookup;
                create table {s}.master_node_geo_lookup as
                select n.nodeid, display.masterid, 
                st_distance(n.geom, display.geom) as dist_to_display, display.geom
                from {s}.{n} as n
                join 
                (
                    select n.nodeid, t.masterid, n.geom
                    from {s}.{n} as n join {s}.temp_best_node as t 
                    on n.nodeid = t.node
                ) as display on n.masterid = display.masterid;
                drop table if exists {s}.temp_best_node;
                grant all on {s}.master_node_geo_lookup to public;
           """.format(s=schema, n=node_table))
    dbo.query("""
                Comment on table {s}.master_node_geo_lookup is 
                'CLION display coordinates for nodes with masterids (Version {v} - Run: {d}'
            """.format(s=schema, v=version, d=datetime.now().strftime('%Y-%m-%d')))


@db2.timeDec
def make_master_segment_lookup(dbo, schema, lion_table, version):
    # This creates a lookup table for display coordinates for segments
    dbo.query("""
                drop table if exists {s}.temp_display_seg;
                create table {s}.temp_display_seg as 
                SELECT l.mft, l.segmentid, st_distance(l.geom , c.cent_geom) as dist, l.geom
                from lion l  
                join (
                    select mft, st_centroid(st_union(geom)) as cent_geom
                    from {s}.{l} where rb_layer in ('G', 'B') and mft > 0 group by mft
                ) as c on l.mft=c.mft
                where l.mft > 0 and rb_layer in ('G', 'B');
            """.format(s=schema, l=lion_table))
    dbo.query("""
                drop table if exists {s}.temp_master_seg_geo_lookup;
                create table {s}.temp_master_seg_geo_lookup as 
                with cte as (
                    select mft,min(dist) as mindist
                    from {s}.temp_display_seg
                    group by mft
                ) select distinct t.* from cte 
                join {s}.temp_display_seg t on cte.mft=t.mft and cte.mindist=t.dist;
           """.format(s=schema))
    dbo.query("""       
                drop table if exists {s}.master_seg_geo_lookup;
                create table {s}.master_seg_geo_lookup as 
                select distinct l.street, l.segmentid, l.mft, t.segmentid as display_segment, 
                st_distance(l.geom, t.geom) as dist, t.geom as geom
                from {s}.{l} l 
                join (
                    select t.*
                    from {s}.temp_master_seg_geo_lookup t join (
                        select mft, min(segmentid) as seg from {s}.temp_master_seg_geo_lookup group by mft 
                    ) m on t.segmentid = m.seg 
                ) t on l.mft = t.mft;
                drop table if exists {s}.temp_display_seg;
                drop table if exists {s}.temp_master_seg_geo_lookup;
                grant all on {s}.master_seg_geo_lookup to public;
            """.format(s=schema, l=lion_table))
    dbo.query("""
                Comment on table {s}.master_seg_geo_lookup is 
                'CLION display coordinates for segments with mfts (Version {v} - Run: {d}'
            """.format(s=schema, v=version, d=datetime.now().strftime('%Y-%m-%d')))
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
#     12. Make views
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


def street_name_view(dbo, schema, lion_table):
    dbo.query("""
                drop view if exists {s}.v_street_names;
                create view {s}.v_street_names as
                select node, min(street) as s1, max(street) as s2, array_agg(street) as als
                from (
                    select nodeidfrom::int as node, street
                    from {s}.{l}
                    union
                    select nodeidto::int as node, street
                    from {s}.{l}
                ) as d group by node;
            """.format(s=schema, l=lion_table))


def ramp_intersection_views(dbo, schema, node_table, lion_table):
    dbo.query("""
                    drop view if exists {s}.v_ramp_intersections;
                    create view {s}.v_ramp_intersections as
                    select n.nodeid, n.masterid, n.is_int, n.manual_fix, n.is_cntrln_int, true as is_ramp_int, n.geom
                    from {s}.{n} n
                    join (
                        select r.* from
                        (
                        select nodeidfrom::int as node from {s}.{l} where ramp = true
                        union select nodeidto::int from {s}.{l} where ramp = true
                        ) as r
                        join 
                        (
                        select nodeidfrom::int as node from {s}.{l} where ramp = false
                        union select nodeidto::int from {s}.{l} where ramp = false
                        ) as nr
                        on r.node = nr.node
                    ) as ramp_ints
                    on n.nodeid = ramp_ints.node
                """.format(s=schema, l=lion_table, n=node_table))

# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
#     13.  Cleanup and index
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


def add_indexes(dbo,  node_table, lion_table, schema=params.WORKING_SCHEMA):
    index_list = ["drop index if exists {s}.nd_IDX;".format(s=schema),
                  "drop index if exists {s}.master_IDX;".format(s=schema),
                  "drop index if exists {s}.seg_IDX;".format(s=schema),
                  "drop index if exists {s}.mft_IDX;".format(s=schema),
                  "drop index if exists {s}.nf_IDX;".format(s=schema),
                  "drop index if exists {s}.nt_IDX;".format(s=schema),
                  "drop index if exists {s}.mf_IDX;".format(s=schema),
                  "drop index if exists {s}.mt_IDX;".format(s=schema),
                  "CREATE INDEX nd_IDX ON {s}.{n} (nodeid);".format(s=schema, n=node_table),
                  "CREATE INDEX master_IDX ON {s}.{n} (masterid);".format(s=schema, n=node_table),
                  "CREATE INDEX seg_IDX ON {s}.{l} (segmentid);".format(s=schema, l=lion_table),
                  "CREATE INDEX mft_IDX ON {s}.{l} (mft);".format(s=schema, l=lion_table),
                  "CREATE INDEX nf_IDX ON {s}.{l} (nodeidfrom);".format(s=schema, l=lion_table),
                  "CREATE INDEX nt_IDX ON {s}.{l} (nodeidto);".format(s=schema, l=lion_table),
                  "CREATE INDEX mf_IDX ON {s}.{l} (masteridfrom);".format(s=schema, l=lion_table),
                  "CREATE INDEX mt_IDX ON {s}.{l} (masteridto);".format(s=schema, l=lion_table)
                  ]
    print 'Indexing...\n'
    for idx in tqdm(index_list):
        dbo.query(idx)


# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
#     *** DONE ***
# |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||


@db2.timeDec
def run():
    db = db2.PostgresDb(params.DB_HOST, params.DB_NAME, quiet=True)
    #     1. Setup database
    if raw_input('Archive (Y/N) ?\n').upper() == 'Y':
        archive(db,
                params.LION,
                params.NODE,
                params.WORKING_SCHEMA,
                params.ARCHIVE_SCHEMA)
    fixes = [
        # KNOWN ERRORS IN LION TO FIX | INPUTS:
        # (field to select on, value to select on, field to update, value to update)
        # ('segmentid', "'0172607'", 'trafdir', "'A'"),  # was P
        # ('segmentid', "'0106838'", 'trafdir', "'A'"),  # was P
        ('segmentid', "'0164415'", 'trafdir', "'A'"),  # was P
        ('segmentid', "'0297670'", 'trafdir', "'A'"),  # was P
        ('segmentid', "'0276350'", 'trafdir', "'A'"),  # was P
        ('segmentid', "'0164344'", 'trafdir', "'A'"),  # was P
        ('segmentid', "'0276509'", 'trafdir', "'A'"),  # was P
        # ('segmentid', "'0176867'", 'street', "'59 AVENUE'"),  # was WOODHAVEN BOULEVARD
        # ('segmentid', "'0176866'", 'street', "'59 AVENUE'"),  # was WOODHAVEN BOULEVARD
        ('segmentid', "'0262058'", 'nonped', 'null'),  # was V
        ('segmentid', "'0159297'", 'nonped', 'null'),  # was V
        ('segmentid', "'0145754'", 'nonped', 'null'),  # was V
        ('segmentid', "'0145755'", 'nonped', 'null'),  # was V
        ('segmentid', "'0161277'", 'nonped', 'null'),  # was V
        ('segmentid', "'0139548'", 'nonped', 'null'),  # was V
        ('segmentid', "'0270493'", 'nonped', "'V'"),  # was NULL
        # ('street', "'QUEENS MIDTOWN TUNNEL APPROACH'", 'rw_type', "'9'"),  # mis-catagorized as non-ramps
        # ('street', "'QUEENS MIDTOWN TUNNEL EXIT'", 'rw_type', "'9'"),  # mis-catagorized as non-ramps
        ('segmentid', "'0038411'", 'rw_type', "'9'"),  # need to treat it as a ramp
        ('segmentid', "'0038398'", 'rw_type', "'9'"),  # need to treat it as a ramp
        ('segmentid', "'0174839'", 'street', "'45 AVENUE'"),  # was 70 STREET
        ('segmentid', "'0174841'", 'street', "'45 AVENUE'"),  # was 70 STREET
        ('segmentid', "'0174840'", 'street', "'45 AVENUE'"),  # was 70 STREET
        ('segmentid', "'0123347'", 'nodeidfrom', "'0076624'"),  # was 00000-1
        ('segmentid', "'0123347'", 'nodeidto', "'0076623'"),  # was 00000-1

        ('segmentid', "'0176861'", 'nodelevelt', "'M'"),  # was *
        ('segmentid', "'0176861'", 'nodelevelf', "'M'"),  # was *
        ('segmentid', "'0176861'", 'rb_layer', "'B'"),  # was G
        ('segmentid', "'0176861'", 'rw_type', "'9'"),  # need to treat it as a ramp
        ('segmentid', "'0122207'", 'nodelevelt', "'M'"),  # was *
        ('segmentid', "'0122207'", 'nodelevelf', "'M'"),  # was *
        ('segmentid', "'0122207'", 'rb_layer', "'B'"),  # was G
        ('segmentid', "'0122207'", 'rw_type', "'9'"),  # need to treat it as a ramp
        ('segmentid', "'0175063'", 'nodelevelt', "'M'"),  # was *
        ('segmentid', "'0175063'", 'nodelevelf', "'M'"),  # was *
        ('segmentid', "'0175063'", 'rb_layer', "'B'"),  # was G
        ('segmentid', "'0175063'", 'rw_type', "'9'")  # need to treat it as a ramp
    ]

    print '\n'*25
    setup_database(db)  # 221 sec
    #     2. Define street network to use (centerline)
    manual_fix_segments(db, fixes, params.WORKING_SCHEMA, params.LION)
    define_usable_street_network(db, params.WORKING_SCHEMA, params.LION)
    define_ramps(db, params.WORKING_SCHEMA, params.LION)
    #     3. Define Intersections
    build_generic_node_levels(db, params.WORKING_SCHEMA, params.NODE, params.LION, params.RPL)
    build_street_name_table(db, params.WORKING_SCHEMA, params.LION, params.NODE)
    params.nodeStreetNames, params.nodeIsIntersection = node_names(
        db,
        params.nodeStreetNames,
        params.nodeIsIntersection,
        params.WORKING_SCHEMA,
        params.NODE)
    #     4. Build street network graph
    params.nodeNextSteps = graph(
        db,
        params.nodeNextSteps,
        params.WORKING_SCHEMA,
        params.LION)
    params.nodeStreetNames, params.streetSet = search(
        params.nodeStreetNames,
        params.streetSet,
        params.nodeIsIntersection,
        params.nodeNextSteps)
    params.altGraph = generate_blocks_from_masterids(
        db,
        params.altGraph,
        params.streetSet,
        params.mft1Dict,
        params.FOLDER,
        params.LION,
        params.WORKING_SCHEMA)
    params.clusterIntersections = intersection_cluster_dict(
        params.nodeStreetNames,
        params.clusterIntersections,
        params.nodeIsIntersection)
    params.clusterIntersections = subset_merge_with_superset(  # 14 min!?!
        db,
        params.WORKING_SCHEMA,
        params.clusterIntersections,
        params.nodeStreetNames)
    params.clusterIntersections, params.nodeMaster, params.masterNodes = master_intersection_first_pass(
        params.clusterIntersections,
        params.nodeMaster,
        params.masterNodes)
    #     6. Build simplified network - nodes
    pct_lookup, pct_neighbors, node_coords = get_all_database_needs_for_distance_check(
        db,
        params.WORKING_SCHEMA,
        params.NODE,
        params.PRECINCTS)
    problem_masters = find_masters_with_distant_nodes(
        params.masterNodes,
        params.nodeIsIntersection,
        node_coords)
    params.nodeMaster, params.masterNodes, params.nodeStreetNames = update_problem_groups(
        problem_masters,
        pct_lookup,
        params.nodeMaster,
        params.masterNodes,
        params.nodeStreetNames)
    params.nodeMaster, params.masterNodes = near_by_simple(
        db,
        params.WORKING_SCHEMA,
        params.NODE,
        params.nodeMaster,
        params.masterNodes,
        75)
    tri, params.nodeMaster, params.masterNodes = triangle(
        node_coords,
        params.nodeNextSteps,
        params.nodeMaster,
        params.masterNodes,
        params.nodeIsIntersection,
        150)
    update_db_nodes(db,
                    params.WORKING_SCHEMA,
                    params.NODE,
                    params.FOLDER,
                    params.nodeMaster)
    #     7. Generate stable master ids
    stabilize_masters(db,
                      params.NODE,
                      params.WORKING_SCHEMA)
    #     8. Rebuild street network with new masterid info
    remap_from_to_masters(
        db,
        params.LION,
        params.NODE,
        params.WORKING_SCHEMA)
    #     9. Update roadbeds
    update_roadbeds(
        db,
        params.WORKING_SCHEMA,
        params.LION,
        params.RPL)
    update_roadbed_nodes(
        db,
        params.WORKING_SCHEMA,
        params.NODE,
        params.RPL)
    #     10. Generate corridors
    corridor_names(db)
    dissolve_corridors(db)
    corridor_id(db)
    add_corridor_to_lion(db)
    db.dbClose()
    del db


def index_and_permissions():
    print '\nIndex and permissions...\n'
    # split here, because db was hanging somewhere in the make lookup when run in full, run in pieces was fine
    db = db2.PostgresDb(params.DB_HOST, params.DB_NAME, quiet=True)
    #     11.  Make master geom lookup tables
    make_master_node_lookup(
        db,
        params.WORKING_SCHEMA,
        params.NODE,
        params.VERSION)
    make_master_segment_lookup(
        db,
        params.WORKING_SCHEMA,
        params.LION,
        params.VERSION)
    #     12.  Make views
    street_name_view(
        db,
        params.WORKING_SCHEMA,
        params.LION)
    ramp_intersection_views(
        db,
        params.WORKING_SCHEMA,
        params.NODE,
        params.LION)

#     13. Cleanup and index
    add_indexes(db,
                params.NODE,
                params.LION,
                params.WORKING_SCHEMA)
    tables = [
        params.LION,
        params.NODE,
        'master_seg_geo_lookup',
        'master_node_geo_lookup',
        'node_stnameft'
    ]
    for table in tables:
        db.query("grant all on {s}.{t} to public;".format(
            s=params.WORKING_SCHEMA,
            t=table
        ))
        db.query("""
                       comment on table {s}.{t} is 'Created by {u} on {d}'
                       """.format(s=params.WORKING_SCHEMA,
                                  t=table,
                                  u=getpass.getuser(),
                                  d=datetime.now().strftime('%Y-%m-%d %H:%M')))

    print '\n\n'
    print '#' * 50
    print '\n{s}DONE\n'.format(s=' '*23)
    print '#' * 50


if __name__ == '__main__':
    run()
    index_and_permissions()
