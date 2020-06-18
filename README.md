# Refresh L2_ARC

A python script to refresh the l2arc on a zfs file system.

I wanted a way to force-feed my l2arc, so I wrote this small script. Using joblib as a parallel backend,
it will recursively glob through a specified folder, and read random chunks of bytes into memory, do a small
arbitrary operation on them (in-memory) and continue.

This does not make any file writes - however, if you are using the filesystem option that tracks last accessed time
it will update that (as far as I know this is unavoidable)


I use the libraries Click, joblib, and numpy. I did experiment with using
Numba, but that seems like overkill since the overhead will be file reads


