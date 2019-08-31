import os
from tqdm import tqdm
import params


def read_file(file_path):
    with open(file_path, 'r') as f:
        read_data = f.read()
    return read_data


def split_to_rows(raw_data):
    d = raw_data.split('\n')
    output = []
    for row in d:
        output.append(row.split(','))
    return output

def split_to_columns(data_rows):
    data = []
    # ['RPL_ID', 'SegmentIDG', 'SegmentIDR', 'RPC', 'NCI',
    #         'NodeLevelF', 'NodeLevelT', 'R_FrNd', 'G_FrNd', 'R_ToNd', 'G_ToNd']
    idn = 0
    for row in data_rows:
        if row != ['']:
            # print row
            idn += 1
            g_seg = row[0][:7]
            g_seg_typ = row[0][7]
            r_seg = row[0][8:15]
            rpc = row[0][16]
            nci = row[0][18]
            fnode_level_code = row[0][22]
            tnode_level_code = row[0][26]
            f_node_rb_seg = row[0][28:35]
            f_node_g_seg = row[0][36:43]
            t_node_rb_seg = row[0][44:51]
            t_node_g_seg = row[0][52:59]
            data.append([idn, g_seg, r_seg, rpc, nci, fnode_level_code, tnode_level_code,
                         f_node_rb_seg, f_node_g_seg, t_node_rb_seg, t_node_g_seg])
    return data



def add_to_db(db, data, rpl):
    rpl_table = 'tbl_'+rpl[:-4].lower()
    cur = db.conn.cursor()  # use cursor rather than full method to avoid noisy print statements
    # make sure table exists and is clean
    cur.execute("""DROP TABLE if exists {s}.{t};
                    CREATE TABLE {s}.{t}
                    (
                      rpl_id bigint, segmentidg bigint, segmentidr bigint,
                      rpc character varying(1), nci character varying(1),
                      nodelevelf character varying(1), nodelevelt character varying(1),
                      r_frnd bigint, g_frnd bigint, r_tond bigint, g_tond bigint
                    );
                """.format(s=params.WORKING_SCHEMA, t=rpl_table))
    db.conn.commit()
    del cur
    print 'Adding RPL data'
    for row in tqdm(data):
        if row[0] != 'RPL_ID':
            add_row(db, row)


def add_row(db, row):
    cur = db.conn.cursor()
    if row[0]:
        cur.execute("""INSERT INTO {s}.tbl_rpl (rpl_id, segmentidg, segmentidr, rpc, nci,nodelevelf,
                        nodelevelt, r_frnd, g_frnd, r_tond, g_tond)
                        VALUES ({v});
                    """.format(s=params.WORKING_SCHEMA, v=str(row)[1:-1]))
        db.conn.commit()
        del cur
        #print 'Added %s' % row[0]


def run(db, folder, rpl):
    r_data = read_file(os.path.join(folder, rpl))
    dta = split_to_rows(r_data)
    split_data = split_to_columns(dta)
    add_to_db(db, split_data, rpl)


