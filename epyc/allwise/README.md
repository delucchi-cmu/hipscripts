## Running full allwise

This is from a resumed run, so I lost the mapping/splitting lines, 
and like 20k of the reducing lines too.

Binning  : 100%|██████████| 1/1 [00:14<00:00, 15.00s/it]
Reducing : 100%|██████████| 830/830 [27:40<00:00,  2.00s/it]
Finishing: 100%|██████████| 6/6 [00:39<00:00,  6.52s/it]

## Running neowise stats only

real    3524m16.522s
user    34283m20.342s
sys     2090m15.450s

Mapping  : 100%|██████████| 72/72 [58:38:35<00:00, 2932.16s/it]
Binning  : 100%|██████████| 1/1 [05:26<00:00, 326.84s/it]
Finishing: 100%|██████████| 6/6 [00:01<00:00,  3.21it/s]

## Running full neowise

$ time python3 run_neowise.py &> neowise_full.log

    real    8326m23.745s
    user    77316m55.417s
    sys     5867m17.342s

    Mapping  : 100%|██████████| 72/72 [58:55:18<00:00, 2946.09s/it]

    Binning  :   0%|          | 0/1 [00:00<?, ?it/s]
    Binning  : 100%|██████████| 1/1 [01:07<00:00, 67.28s/it]
    Binning  : 100%|██████████| 1/1 [01:07<00:00, 67.28s/it]

    Splitting: 100%|██████████| 72/72 [72:50:03<00:00, 3641.71s/it]

    Reducing : 100%|█████████▉| 20007/20010 [6:54:09<00:08,  2.99s/it]
    Reducing : 100%|█████████▉| 20008/20010 [6:54:18<00:09,  4.73s/it]2023-07-02 01:36:55,755 - distributed.worker - WARNING - Compute Failed
    Key:       9_2118998
    Function:  reduce_pixel_shards
    args:      ()
    kwargs:    {'cache_path': '/data3/epyc/data3/hipscat/tmp/neowise_yr8/intermediate', 'destination_pixel_order': 9, 'destination_pixel_number': 2118998, 'destination_pixel_size': 529914, 'output_path': '/data3/epyc/data3/hipscat/catalogs/neowise_yr8', 'ra_column': 'RA', 'dec_column': 'DEC', 'id_column': 'SOURCE_ID', 'add_hipscat_index': True, 'use_schema_file': None}
    Exception: "ArrowNotImplementedError('Unsupported cast from string to null using function cast_null')"


    Reducing : 100%|█████████▉| 20009/20010 [6:54:26<00:05,  5.71s/it]
    Reducing : 100%|██████████| 20010/20010 [6:54:51<00:00, 11.53s/it]
    Reducing : 100%|██████████| 20010/20010 [6:54:51<00:00,  1.24s/it]

adding in the schema file generated somewhere else. resuming.


    Binning  :   0%|          | 0/1 [00:00<?, ?it/s]
    Binning  : 100%|██████████| 1/1 [01:15<00:00, 75.16s/it]
    Binning  : 100%|██████████| 1/1 [01:15<00:00, 75.16s/it]

    Reducing :   0%|          | 0/10895 [00:00<?, ?it/s]

so about half the files failed to write. they're writing now, though!

Further attempt:

    real    477m36.557s
    user    3605m41.809s
    sys     1446m0.624s

And a breakdown:

    Binning  : 100%|██████████| 1/1 [01:15<00:00, 75.16s/it]
    Reducing : 100%|██████████| 10895/10895 [7:46:07<00:00,  2.57s/it]
    Finishing: 100%|██████████| 6/6 [08:03<00:00, 80.65s/it]

And it's stinking done now.