# October 16

## TIC

**catalog**

    (hatsenv) bash-4.2$ time python import_tic.py &> tic2.log

    real    28m32.465s
    user    244m23.651s
    sys     64m3.237s

and the tqdm times:

    Converting parquet:   0%|          | 1/3768 [00:00<41:21,  1.52it/s]
    Converting parquet: 100%|██████████| 3768/3768 [26:57<00:00,  2.33it/s]
    Finishing :   0%|          | 0/4 [00:00<?, ?it/s]
    Finishing :  25%|██▌       | 1/4 [00:48<02:25, 48.34s/it]
    Finishing :  75%|███████▌  | 3/4 [00:48<00:12, 12.65s/it]
    Finishing : 100%|██████████| 4/4 [00:49<00:00,  8.50s/it]
    Finishing : 100%|██████████| 4/4 [00:49<00:00, 12.33s/it]

**margin**

A LOT faster.

    real    2m2.102s
    user    8m19.941s
    sys     1m14.784s

and the tqdm times:

    Converting parquet:   0%|          | 1/3768 [00:01<1:26:47,  1.38s/it]
    Converting parquet: 100%|██████████| 3768/3768 [01:01<00:00, 61.15it/s] 
    Finishing :   0%|          | 0/4 [00:00<?, ?it/s]
    Finishing :  25%|██▌       | 1/4 [00:13<00:41, 13.70s/it]
    Finishing :  75%|███████▌  | 3/4 [00:14<00:03,  3.68s/it]
    Finishing : 100%|██████████| 4/4 [00:14<00:00,  3.51s/it]

## GAIA

**gaia**

    Converting parquet:   0%|          | 1/3933 [00:01<1:14:31,  1.14s/it]
    Converting parquet: 100%|██████████| 3933/3933 [21:26<00:00,  3.06it/s]

    Finishing :   0%|          | 0/4 [00:00<?, ?it/s]
    Finishing :  25%|██▌       | 1/4 [01:34<04:44, 94.86s/it]
    Finishing :  75%|███████▌  | 3/4 [01:35<00:24, 24.67s/it]
    Finishing : 100%|██████████| 4/4 [01:35<00:00, 16.21s/it]
    Finishing : 100%|██████████| 4/4 [01:35<00:00, 23.82s/it]

**gaia_10arcs**

Toooo fast....

    Converting parquet:   0%|          | 1/3933 [00:01<1:19:45,  1.22s/it]
    Converting parquet: 100%|██████████| 3933/3933 [00:29<00:00, 131.96it/s]
    Finishing :   0%|          | 0/4 [00:00<?, ?it/s]

then like a million error messages like:

    Index must either be string or integer
      worker address: tcp://127.0.0.1:40884
    Index must either be string or integer

then:

    FileNotFoundError: /data3/epyc/data3/hats/catalogs/gaia_dr3/gaia_10arcs/dataset

I think because the catalog_info doesn't have radec columns. Adding now.

Yes. That went better:

    Converting parquet: 100%|██████████| 3933/3933 [00:44<00:00, 88.87it/s]

    Finishing :   0%|          | 0/4 [00:00<?, ?it/s]
    Finishing :  25%|██▌       | 1/4 [00:22<01:06, 22.27s/it]
    Finishing :  75%|███████▌  | 3/4 [00:22<00:05,  5.85s/it]
    Finishing : 100%|██████████| 4/4 [00:22<00:00,  5.62s/it]

**epoch_photometry**

Well beans. This one ALSO doesn't have radec columns. Intentionally. There 
was A LOT of massaging that went into this dataset.

## ZTF

**starting subcatalog ztf_dr14**

    Converting parquet:   0%|          | 0/2352 [00:00<?, ?it/s]
    Converting parquet:   0%|          | 1/2352 [00:00<26:48,  1.46it/s]
    Converting parquet: 100%|██████████| 2352/2352 [02:09<00:00, 18.10it/s]

    Finishing :   0%|          | 0/4 [00:00<?, ?it/s]
    Finishing :  25%|██▌       | 1/4 [00:03<00:11,  3.85s/it]
    Finishing :  75%|███████▌  | 3/4 [00:03<00:01,  1.04s/it]
    Finishing : 100%|██████████| 4/4 [00:04<00:00,  1.06s/it]

**starting subcatalog ztf_dr14_10arcs**

    Converting parquet:   0%|          | 0/2352 [00:00<?, ?it/s]
    Converting parquet: 100%|██████████| 2352/2352 [00:25<00:00, 92.01it/s] 

    Finishing :   0%|          | 0/4 [00:00<?, ?it/s]
    Finishing :  25%|██▌       | 1/4 [00:02<00:06,  2.24s/it]
    Finishing : 100%|██████████| 4/4 [00:02<00:00,  1.71it/s]

**starting subcatalog ztf_zource**

This could take 5-15 hours.

## Other catalogs

starting subcatalog des_dr2

    Converting parquet: 100%|██████████| 1582/1582 [34:19<00:00,  1.19s/it]
    Converting parquet: 100%|██████████| 1582/1582 [34:19<00:00,  1.30s/it]

starting subcatalog alerce_nested

    Converting parquet: 100%|██████████| 113/113 [01:13<00:00,  1.53it/s]

starting subcatalog erosita

FileNotFoundError: [Errno 2] No such file or directory: '/astro/store/epyc3/projects3/max_hipscat/erosita/catalog_info.json'
