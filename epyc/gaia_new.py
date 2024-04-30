"""Main method to enable command line execution"""

import partitioner.dask_runner as dr
from partitioner.arguments import PartitionArguments

# def download_gaia(client=None):
#     c = hc.Catalog('gaia_real', location='/epyc/projects3/mmd11_hipscat/')
#     c.hips_import(file_source='/epyc/data/gaia_edr3_csv/', fmt='csv.gz', ra_kw='ra', dec_kw='dec',
#         id_kw='source_id', debug=False, verbose=True, threshold=1_000_000, client=client)

if __name__ == "__main__":
    args = PartitionArguments()
    args.from_params(
        catalog_name="gaia_real",
        input_path="/epyc/data/gaia_edr3_csv/",
        input_file_list=["/epyc/data/gaia_edr3_csv/GaiaSource_786097-786431.csv.gz"],
        input_format="csv.gz",
        ra_column="ra",
        dec_column="dec",
        id_column="source_id",
        pixel_threshold=1_000_000,
        dask_tmp="/epyc/projects3/mmd11/tmp/",
        highest_healpix_order=10,
        output_path="/epyc/projects3/mmd11_hipscat/",
    )
    dr.run(args)
