##!/usr/bin/env python3

import mmap
import random
import timeit
from pathlib import Path

import click
import numpy as np
import terminaltables
from joblib import Parallel, delayed

g_max_reads = 4096
g_randomize_reads = False
g_max_read_time_secs = 60


def do_random_with_read(read_byte):
    zx = read_byte * 2
    zx = zx + 4
    return zx


def random_read_file(path):
    global g_max_reads
    global g_max_read_time_secs
    gbg_data = 0
    with open(path, 'rb', buffering=0) as f:
        nbytes = f.seek(0, 2)
        nactual = f.tell()
        start_time = timeit.default_timer()
        with mmap.mmap(f.fileno(), nactual, prot=mmap.PROT_READ) as f_map:
            # read_locs = np.arange(0,len(f_map))
            # np.random.shuffle(read_locs)
            num_reads = 0
            max_reads = nbytes if g_max_reads < 0 else g_max_reads
            while num_reads < max_reads:
                loc = random.randint(0, len(f_map) - 1)
                d = f_map[loc]
                gd = do_random_with_read(d)
                gbg_data += gd
                num_reads += d

                cur_time = timeit.default_timer()
                if cur_time - start_time > g_max_read_time_secs:
                    num_reads = max_reads

    return [path, gbg_data, nbytes]


def read_all_paths(path_list, njobs, list_files=True):
    total_bytes = 0
    total_gbg_data = 0
    data_totals_tabulate = []
    if len(path_list) > 1 and njobs > 1:
        with Parallel(n_jobs=njobs, verbose=10) as parallel:
            gbg_data = np.array(parallel(delayed(random_read_file)(p) for p in path_list))
            if list_files:
                data_totals_tabulate = gbg_data
            gbg_data_szs = gbg_data[:, 1:]
            total_gbg_data, total_bytes = np.sum(gbg_data_szs, axis=0)
    else:
        for p in path_list:
            fn, gbg, tb = random_read_file(p)
            if list_files:
                data_totals_tabulate.append([fn, gbg, tb])
            total_bytes += tb
            total_gbg_data += gbg

    return total_gbg_data, total_bytes, data_totals_tabulate


def sizeof_fmt(num, suffix='B'):
    """
    sizeof_fmt - code gathered from somewhere to give human-readable sizes of bytes.
    :param num:
    :param suffix:
    :return:
    """
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def read_folder(current_folder, glob_pattern):
    v = []

    with click.progressbar(current_folder.glob(glob_pattern)) as bar:
        for p in bar:
            if not p.is_dir() and p.stat().st_size > 100:
                v.append(p)
    return v


def print_total_bytes(large_data, do_table=True, style='ascii'):
    """

    :param large_data:
    :param do_table:
    :param style: can be ascii, markdown, or single. Sets the table style
    :return:
    """
    gbg_data = large_data[0:2] if len(large_data) > 2 else large_data
    # click.secho(f"Dbg: {gbg_data}",fg="blue")
    total_bytes = sizeof_fmt(gbg_data[1])
    gbg_bytes = sizeof_fmt(gbg_data[0])
    click.secho(f"Read {total_bytes}", fg="green")
    click.secho(f"GBG Res: {gbg_bytes}", fg="green")
    if do_table:
        headers = ['Filename', 'Bytes Read', 'Filesize']
        data = [headers]
        lgd = large_data[2]
        lgd[:, 0] = np.apply_along_axis(lambda x: x[0].name, 1, lgd)
        data = [headers]
        [data.append(v) for v in lgd]

        if style == 'markdown':
            table = terminaltables.GithubFlavoredMarkdownTable(data)
        elif style == 'ascii':
            table = terminaltables.AsciiTable(data)
        else:
            table = terminaltables.SingleTable(data)
        click.echo_via_pager(f"{table.table}")


@click.group()
@click.option("--njobs", default=16, help="number of processes to run")
@click.option('--max-reads', '-M', default=4096,
              help="Max bytes to read from each file. If negative will be set to the size of the file")
@click.option('--random-max-reads', '-R', default=False, is_flag=True,
              help="Randomize max read sizes (Not implemented yet)")
@click.option('--read-timeout', '-T', default=60.0,
              help="Max seconds to spend reading from a file (an override to max_reads)")
@click.option('--table', default=False, is_flag=True, help="print out table of files touched")
@click.option('--table-fmt', default='single', help="Table format style (single, ascii, markdown)")
@click.pass_context
def cli(ctx, njobs, max_reads, random_max_reads, read_timeout, table, table_fmt):
    global g_max_read_time_secs
    global g_max_reads
    global g_randomize_reads
    g_max_read_time_secs = read_timeout
    g_max_reads = max_reads
    g_randomize_reads = random_max_reads

    if g_randomize_reads:
        # TODO: Add random per-file read size
        click.secho("Read size is not implemented yet", fg="yellow")
        exit(1)

    ctx.ensure_object(dict)
    ctx.obj['njobs'] = njobs
    ctx.obj['do_table'] = table
    ctx.obj['table_fmt'] = table_fmt

@click.command()
@click.argument('paths', nargs=-1, type=click.Path(exists=True, file_okay=True))
@click.pass_context
def read(ctx, paths):
    """
    read - Reads one or more specified files. Does not glob through the subfolder
    :param paths: A list of file paths to read
    """
    ctx.ensure_object(dict)
    njobs = ctx.obj['njobs']
    if len(paths) > 10:
        msg = f"Reading {len(paths)} files"
    else:
        m2 = "\n".join([str(x) for x in paths])
        msg = f"Reading the following files:\n {m2}"
    click.secho(msg, fg="green")
    file_list = []
    for p in paths:
        pth = Path(p)
        if not pth.is_dir():
            file_list.append(pth)
    dta = read_all_paths(file_list, njobs)
    total_size = sizeof_fmt(dta[0])
    click.secho(f"Data Read Debug:\n {total_size}", fg="blue")
    print_total_bytes(dta)


@click.command()
@click.option("--start-loc", type=click.Path(dir_okay=True, file_okay=False), help="Folder to refresh")
@click.option("--glob-pattern", default="**/*", help="Glob pattern for file discovery")
@click.pass_context
def deep_read(ctx, start_loc, glob_pattern):
    """
    deep_read - Reads all files starting in the 'start_loc' folder.
    Will read random chunks of bytes from the files, do a simple math operation on the bytes,
    then continute to the next chunk / file.

    Good for refreshing your l2_arc cache
    :param ctx:
    :param start_loc:
    :param glob_pattern
    """
    ctx.ensure_object(dict)
    njobs = ctx.obj['njobs']
    do_table = ctx.obj['do_table']
    table_fmt = ctx.obj['table_fmt']
    click.secho(f"Reading files in {start_loc}...", fg="green")

    start_loc = Path(start_loc)
    paths = read_folder(start_loc, glob_pattern)

    click.secho(f"Reading in {len(paths)} files...", fg="green")

    dta = read_all_paths(paths, njobs, do_table)
    print_total_bytes(dta, do_table, table_fmt)


cli.add_command(read)
cli.add_command(deep_read)

if __name__ == '__main__':
    cli()
