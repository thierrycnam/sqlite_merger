import sys
import sqlite3

def copy_db(from_, to, list_tables):
    """
    copy database to another

    :param from: data to copy from. expeditor.
    :param to: data to copy to. destination.
    """
    query_detach = """DETACH sourcefile;"""
    query_attach = """ATTACH '{}' AS sourcefile;""".format(from_)
    list_tables_out = get_list_tables(to)

    print("from_: {}".format(from_), file=sys.stderr)
    print("to: {}".format(to), file=sys.stderr)
    print("list_tables in {}: {}".format(to, list_tables_out), file=sys.stderr)
    print("query_attach: {}".format(query_attach), file=sys.stderr)
    conn = sqlite3.connect(to)
    c = conn.cursor()
    c.execute(query_attach)

    for table in list_tables:
        query_insert = """INSERT INTO {} SELECT * FROM sourcefile.{};""".format(table, table)
        if table in list_tables_out:
            print("table '{}' in {}. it wont be created".format(table, to),
                file=sys.stderr)
        else:
            print("table '{}' not in {}.".format(table, to),
                file=sys.stderr)
            query_create_to = get_query_create(table, from_)
            print(query_create_to, file=sys.stderr)
            c.execute(query_create_to)
            print("TABLE '{}' created in table '{}'".format(table, to),
                file=sys.stderr)
        print("query_insert: {}".format(query_insert), file=sys.stderr)
        c.execute(query_insert)
        conn.commit()
    print("query_detach: {}".format(query_detach), file=sys.stderr)
    c.execute(query_detach)

    conn.close()
    print("Copy ended!", file=sys.stderr)

def get_query_create(table, infile):
    """
    Returns the query that created a table in the database

    :param table: the name of the table of infile
    :param infile: the database file
    :returns query: the query that created the table in infile
    """
    query_create = """
        SELECT sql
        FROM sqlite_master
        WHERE name ='{}';""".format(table)
    print(query_create, file=sys.stderr)
    conn = sqlite3.connect(infile)
    c = conn.cursor()
    c.execute(query_create)
    print("QUERY that created {} in {}".format(table, infile), file=sys.stderr)
    query_ = c.fetchall()
    conn.close()

    query_ = query_
    return query_[0][0]

def get_list_tables(infile):
    """
    Returns list of tables of a database file

    :param infile: input database
    :returns: list of tables
    """
    query_list_tables = """
        SELECT
            name
        FROM
            sqlite_master
        WHERE
            type ='table' AND
            name NOT LIKE 'sqlite_%';"""
    conn = sqlite3.connect(infile)
    c = conn.cursor()
    print(query_list_tables, file=sys.stderr)
    c.execute(query_list_tables)
    list_tables = c.fetchall()
    list_tables = [i[0] for i in list_tables]
    conn.close()
    return list_tables

def merge_databases(tables, infiles, outfile=None):
    """
    Merge databases

    :param tables: list of tables from user
    :param infiles: input files to merge
    :param outfile: output file

    :return: -
    """
    if tables:
        list_tables = tables
    else:
        list_tables = get_list_tables(infiles[0])

    if not outfile:
        print("NO OUTFILE GIVEN", file=sys.stderr)
        sys.exit(2)
    else:
        print("output file : {}".format(outfile), file=sys.stderr)
    print(list_tables, file=sys.stderr)
    if len(list_tables) ==  0:
        print("No tables detected. Empty db", file=sys.stderr)
        sys.exit(2)
    for infile in infiles:
        copy_db(infile, outfile, list_tables)

def parser_handler(args):
    """
    Parser handler

    :param args: arguments

    :returns: -
    """
    infiles = args.infiles
    outfile = args.outfile
    tables = args.tables

    if tables:
        print("tables provided: {}".format(tables), file=sys.stderr)
    else:
        print("No tables provided!", file=sys.stderr)

    if outfile:
        print("outfile given: {}".format(outfile), file=sys.stderr)
        print("{} will be merged in {}".format(",".join(infiles),
            outfile))
    merge_databases(tables, infiles, outfile)


def get_argument_parser():
    """
    Creates an argument parser.

    Returns:
        A parser object from argparse
    """
    import argparse
    parser = argparse.ArgumentParser('Merge several databases into one.')

    # Create the parser for the "parse" command
    parser.add_argument('-i', '--infiles', required=True, nargs="+",
        help='Input SQLite files to merge into a single database.')
    parser.add_argument('-o', '--outfile', required=True, default=None,
        help='Output file.')
    parser.add_argument('-t', '--tables', required=False,
        default=None, nargs='*',
        help='List of tables to copy. If not provided all the tables will '
        'be copied')
    parser.set_defaults(func=parser_handler)
    return parser

def main():
    parser = get_argument_parser()
    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
