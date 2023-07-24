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

## AXS ZTF source to PanStarrs

    Planning: 100%|██████████| 1/1 [03:40<00:00, 220.75s/it]
    Counting : 100%|██████████| 311037/311037 [4:50:25<00:00, 17.85it/s]
    Finishing: 100%|██████████| 4/4 [14:41<00:00, 220.45s/it]

    real    329m26.276s
    user    4058m6.049s
    sys     2094m6.291s

Oh wow. .01% unmatched (71985277/570737814736). That's pretty good.
And only 571 source partitions contain unmatched sources.

## Zubercal to PanStarrs

Ok. Last one for a little while.

    Planning: 100%|██████████| 1/1 [00:56<00:00, 56.84s/it]
    Counting : 100%|██████████| 70853/70853 [6:16:20<00:00,  3.14it/s]
    Finishing: 100%|██████████| 4/4 [03:47<00:00, 57.00s/it]

    real    386m4.066s
    user    4317m26.892s
    sys     3783m58.487s

.06% unmatched. I'm pretty happy with this.

Still curious what this might look like with extra look-arounds for some
pixels, but I'm pretty sure this is good enough for the timedomain MVP.

- PanStarrs object to detection [/data3/epyc/data3/hipscat/catalogs/ps1/ps1_otmo_to_detection]
- AXS ZTF source to AXS ZTF object [/data3/epyc/data3/hipscat/catalogs/ztf_axs/ztf_object_to_source]
- AXS ZTF source to PanStarrs [/data3/epyc/data3/hipscat/catalogs/ztf_axs/ps1_to_source]
- Zubercal to PanStarrs [/data3/epyc/data3/hipscat/catalogs/ps1/ps1_to_zubercal]

## AGN to SDSS.

Ok. That's going to take longer.