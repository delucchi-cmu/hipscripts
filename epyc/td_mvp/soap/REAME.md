## ps1_otmo_to_detection

    Planning: 100%|██████████| 1/1 [01:55<00:00, 115.74s/it]
    Counting : 100%|██████████| 150806/150806 [7:46:54<00:00,  5.38it/s]
    Finishing: 100%|██████████| 4/4 [06:35<00:00, 98.98s/it]

and most of the sources were matched up! less than 1% of sources remained
in the `unmatched_sources` file.

So for the first pass of the pipeline, using only the nearest neighbor pixels,
taking only 8 hours to run, I'm pretty happy with that coverage.

Unfortunately, source partitions with unmatched sources are 80% of partitions.
So to expand the search, that would mean adding more object tiles to search
for *every* source tile. But we quit after everything is matched, so that 
would only add time to the planning stage in the worst case.

I dunno. We'll see how things go with other catalogs.

- AXS ZTF source to AXS ZTF object
- AXS ZTF source to PanStarrs
- Zubercal to PanStarrs

## AXS ZTF source to AXS ZTF object

    Planning: 100%|██████████| 1/1 [03:22<00:00, 202.37s/it]
    Counting : 100%|██████████| 311037/311037 [5:02:41<00:00, 17.13it/s]
    Finishing: 100%|██████████| 4/4 [14:57<00:00, 224.36s/it]

    real    342m8.452s
    user    4054m35.025s
    sys     2217m27.016s

With no unmatched sources file written (because duh everything definitely
matches).