import click
import os
from pathlib import Path
from joblib import Parallel, delayed
import random
import mmap
import numpy as np
from pathlib import Path


g_max_reads = 4096
g_randomize_reads = False

def do_random_with_read(read_byte):
    zx = read_byte * 2
    zx = zx + 4
    return zx



def random_read_file(path):
    global g_max_reads
    gbg_data = 0
    with open(path,'rb',buffering=0) as f:
        nbytes = f.seek(0,2)
        nactual = f.tell()
        with mmap.mmap(f.fileno(),nactual,prot=mmap.PROT_READ) as f_map:
            #read_locs = np.arange(0,len(f_map))
            #np.random.shuffle(read_locs)
            num_reads = 0
            if g_max_reads < 0:
                max_reads = nbytes
            else:
                max_reads = g_max_reads

            while num_reads < max_reads:
                loc = random.randint(0,len(f_map)-1)
                d = f_map[loc]
                gd = do_random_with_read(d)
                gbg_data += gd
                num_reads += d 
    return [gbg_data,nbytes]

def read_all_paths(path_list,njobs):
    total_bytes = 0
    total_gbg_data = 0
    if len(path_list) > 1 and njobs > 1:
        with Parallel(n_jobs=njobs, verbose=10) as parallel:
            
            gbg_data = parallel(delayed(random_read_file)(p) for p in path_list)
            print(gbg_data)

            total_gbg_data,total_bytes = np.sum(gbg_data,axis=0)
    else:
        for p in path_list:
            gbg,tb = random_read_file(p)
            total_bytes += tb
            total_gbg_data += gbg

    return total_gbg_data,total_bytes

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

@click.group()
@click.option("--njobs",default=16,help="number of processes to run")
@click.option('--max_reads','-M', default=4096, help="Max bytes to read from each file. If negative will be set to the size of the file")
@click.option('--random_max_reads','-R',default=False,is_flag=True, help="Randomize max reads")
@click.pass_context
def cli(ctx, njobs,max_reads,random_max_reads):
    global g_max_reads = max_reads
    global g_randomize_reads = random_max_reads
    if g_randomize_reads:
        click.secho("Read size is not implemented yet", fg="yellow")
        click.exit(1)

    ctx.ensure_object(dict)
    ctx.obj['njobs'] = njobs



def print_total_bytes(gbg_data):
    #click.secho(f"Dbg: {gbg_data}",fg="blue")
    total_bytes = sizeof_fmt(gbg_data[1])
    gbg_bytes = sizeof_fmt(gbg_data[0]) 
    click.secho(f"Read {total_bytes}",fg="green")
    click.secho(f"GBG Res: {gbg_bytes}",fg="green")


@click.command()
@click.argument('paths', nargs=-1, type=click.Path(exists=True,file_okay=True))
@click.pass_context
def read(ctx,paths):
    ctx.ensure_object(dict)
    njobs=ctx.obj['njobs']
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
    dta = read_all_paths(file_list,njobs)
    total_size = sizeof_fmt(dta[0])
    click.secho(f"Data Read Debug:\n {total_size}",fg="blue")
    print_total_bytes(dta)
        

def read_folder(current_folder,njobs=16):
    v = []
    #v = [x for x in current_folder.glob('**/*') if not x.is_dir()]
    with click.progressbar(current_folder.glob('**/*')) as bar:
            for p in bar:
                if not p.is_dir() and p.stat().st_size > 100:
                    v.append(p)
    return v

@click.command()
@click.option("--start_loc", type=click.Path(dir_okay=True,file_okay=False), help="Folder to refresh")
@click.pass_context
def deep_read(ctx,start_loc):
    ctx.ensure_object(dict)
    njobs=ctx.obj['njobs']
    click.secho(f"Reading files in {start_loc}...",fg="green")
    start_loc = Path(start_loc)
    paths = read_folder(start_loc, njobs)
    click.secho(f"Reading in {len(paths)} files...",fg="green")
    dta = read_all_paths(paths,njobs)
    print_total_bytes(dta)




cli.add_command(read)
cli.add_command(deep_read)

if __name__ == '__main__':
    cli()
