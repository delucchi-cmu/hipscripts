
HATS v0.6.2  (2025-07-26)
==========================================

Bugfixes
-------------------------

- prevent jproperties from escaping colons in values (`#523 <https://github.com/astronomy-commons/hats/pull/523>`__)
- Better type-handling in parquet statistics (`#532 <https://github.com/astronomy-commons/hats/pull/532>`__)
- Add block sizes to UPath (`#536 <https://github.com/astronomy-commons/hats/pull/536>`__)
- Support margin filtering with larger Margin thresholds (`#535 <https://github.com/astronomy-commons/hats/pull/535>`__)

Improved Documentation
-------------------------

- Update getting_started.rst (`#530 <https://github.com/astronomy-commons/hats/pull/530>`__)


HATS v0.6.1  (2025-07-16)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

- Do not swallow file system errors when reading catalogs. (`#518 <https://github.com/astronomy-commons/hats/pull/518>`__)
- Check if catalogs are complete on validation (`#521 <https://github.com/astronomy-commons/hats/pull/521>`__)
- Check if pixel file exists needs filesystem (`#522 <https://github.com/astronomy-commons/hats/pull/522>`__)

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Update testing-windows.yml (`#517 <https://github.com/astronomy-commons/hats/pull/517>`__)
- raise error when there are no remaining pixels to merge or plot (`#526 <https://github.com/astronomy-commons/hats/pull/526>`__)
- raise error when plot_pixels is called on an empty region (`#525 <https://github.com/astronomy-commons/hats/pull/525>`__)
- Update directory scheme documentation (`#527 <https://github.com/astronomy-commons/hats/pull/527>`__)
- Generate provenance properties (`#529 <https://github.com/astronomy-commons/hats/pull/529>`__)

New Contributors
-------------------------

* @jaladh-singhal made their first contribution in https://github.com/astronomy-commons/hats/pull/527


HATS v0.6  (2025-06-11)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- rename plot pixels function header (`#514 <https://github.com/astronomy-commons/hats/pull/514>`__)
- Allow alternate skymaps (and unknown properties) (`#511 <https://github.com/astronomy-commons/hats/pull/511>`__)
- Skip pyarrow version 19.0.0 (`#516 <https://github.com/astronomy-commons/hats/pull/516>`__)
TODO - organize
* @kesiavino made their first contribution in https://github.com/astronomy-commons/hats/pull/514


HATS v0.5.3  (2025-06-02)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Generate data thumbnail (`#503 <https://github.com/astronomy-commons/hats/pull/503>`__)
- Change default for `create_thumbnail` flag (`#504 <https://github.com/astronomy-commons/hats/pull/504>`__)
- Patch colorbar label in `plot_density()` (`#506 <https://github.com/astronomy-commons/hats/pull/506>`__)
- Add more imports to inits. (`#507 <https://github.com/astronomy-commons/hats/pull/507>`__)
- Write additional hats.properties file. (`#512 <https://github.com/astronomy-commons/hats/pull/512>`__)


HATS v0.5.2  (2025-05-07)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Update moc fov to determine order based on FOV size (`#491 <https://github.com/astronomy-commons/hats/pull/491>`__)
- Catalog collection (`#490 <https://github.com/astronomy-commons/hats/pull/490>`__)
- Add original schema property to dataset (`#492 <https://github.com/astronomy-commons/hats/pull/492>`__)
- Update to PPT v2.0.6 (`#493 <https://github.com/astronomy-commons/hats/pull/493>`__)
- Change empty catalog max order behaviour (`#495 <https://github.com/astronomy-commons/hats/pull/495>`__)
- add int64 dtype conversion to pixel tree (`#494 <https://github.com/astronomy-commons/hats/pull/494>`__)
- Handle list and dict input for properties creation. (`#496 <https://github.com/astronomy-commons/hats/pull/496>`__)
- Add github button to docs (`#498 <https://github.com/astronomy-commons/hats/pull/498>`__)
- Update `read_parquet_file_to_pandas` to use nested pandas I/O (`#499 <https://github.com/astronomy-commons/hats/pull/499>`__)
- Update PPT and add lowest supported versions (`#502 <https://github.com/astronomy-commons/hats/pull/502>`__)


HATS v0.5.1  (2025-04-17)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Granular, per-pixel statistics method. (`#477 <https://github.com/astronomy-commons/hats/pull/477>`__)
- Better type hints for new methods/args. (`#478 <https://github.com/astronomy-commons/hats/pull/478>`__)
- Rename argument -> multi_index (`#479 <https://github.com/astronomy-commons/hats/pull/479>`__)
- Add `extra_dict` method to `TableProperties` (`#484 <https://github.com/astronomy-commons/hats/pull/484>`__)
- Allow single input for compute_spatial_index (`#486 <https://github.com/astronomy-commons/hats/pull/486>`__)
- Remove almanac (`#488 <https://github.com/astronomy-commons/hats/pull/488>`__)
- Pydantic class for collection properties (`#485 <https://github.com/astronomy-commons/hats/pull/485>`__)
- Unpin matplotlib (`#489 <https://github.com/astronomy-commons/hats/pull/489>`__)


HATS v0.5.0  (2025-03-19)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Enable anonymous S3 access by default (`#466 <https://github.com/astronomy-commons/hats/pull/466>`__)
- Re-generate test data, and update expectations. (`#467 <https://github.com/astronomy-commons/hats/pull/467>`__)
- Support Npix partitions with a different file suffix or that are directories (`#458 <https://github.com/astronomy-commons/hats/pull/458>`__)
- Always provide partitioning=None and filesystem (`#469 <https://github.com/astronomy-commons/hats/pull/469>`__)
- Filtered catalog should retain path. Add friendlier check for in-memo… (`#470 <https://github.com/astronomy-commons/hats/pull/470>`__)
- Remove utilities to write pixel-only data to parquet metadata files. (`#471 <https://github.com/astronomy-commons/hats/pull/471>`__)
- Remove reading partition info pixels from Norder/Npix (`#474 <https://github.com/astronomy-commons/hats/pull/474>`__)
- Expand column statistics to limit by pixels (`#472 <https://github.com/astronomy-commons/hats/pull/472>`__)
- Move collection of hive column names to shared library. (`#475 <https://github.com/astronomy-commons/hats/pull/475>`__)


HATS v0.4.7  (2025-03-04)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Suppress NaN warnings with context manager. (`#453 <https://github.com/astronomy-commons/hats/pull/453>`__)
- Change non-anchoring links to "anonymous" links. (`#454 <https://github.com/astronomy-commons/hats/pull/454>`__)
- Add example for anonymous S3 catalog reads (`#459 <https://github.com/astronomy-commons/hats/pull/459>`__)
- Be safer around none values in metadata statistics. (`#460 <https://github.com/astronomy-commons/hats/pull/460>`__)
- Don't pass additional kwargs to file open. (`#465 <https://github.com/astronomy-commons/hats/pull/465>`__)


HATS v0.4.6  (2025-01-23)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Remove typing imports for List, Tuple, Union (`#441 <https://github.com/astronomy-commons/hats/pull/441>`__)
- Update to PPT 2.0.5 - fixes slack notifications (`#443 <https://github.com/astronomy-commons/hats/pull/443>`__)
- Documentation improvements. (`#445 <https://github.com/astronomy-commons/hats/pull/445>`__)
- Ensure use of float64 when calling radec2pix (`#447 <https://github.com/astronomy-commons/hats/pull/447>`__)
- Use a naive sparse histogram. (`#446 <https://github.com/astronomy-commons/hats/pull/446>`__)
- Add testing for python 3.13 (`#449 <https://github.com/astronomy-commons/hats/pull/449>`__)
TODO - organize
* @gitosaurus made their first contribution in https://github.com/astronomy-commons/hats/pull/447


HATS v0.4.5  (2024-12-06)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Update PPT to 2.0.4 (`#438 <https://github.com/astronomy-commons/hats/pull/438>`__)
- Make point_map.fits plotting more friendly. (`#439 <https://github.com/astronomy-commons/hats/pull/439>`__)
- Try windows test workflow (`#440 <https://github.com/astronomy-commons/hats/pull/440>`__)
- Move window of supported python versions. (`#442 <https://github.com/astronomy-commons/hats/pull/442>`__)


HATS v0.4.4  (2024-11-26)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Add version property (`#418 <https://github.com/astronomy-commons/hats/pull/418>`__)
- Migrate polygon search to use mocpy utilities (`#415 <https://github.com/astronomy-commons/hats/pull/415>`__)
- Capture compression and open binary if present. (`#419 <https://github.com/astronomy-commons/hats/pull/419>`__)
- Vectorize polygon validation (`#431 <https://github.com/astronomy-commons/hats/pull/431>`__)
- Create new catalog type: map (`#429 <https://github.com/astronomy-commons/hats/pull/429>`__)
- Remove margin fine filtering, and healpy dependency. (`#434 <https://github.com/astronomy-commons/hats/pull/434>`__)


HATS v0.4.3  (2024-11-07)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Update cone search notebook (`#405 <https://github.com/astronomy-commons/hats/pull/405>`__)
- Improve catalog validation and column statistics (`#404 <https://github.com/astronomy-commons/hats/pull/404>`__)
- Write point map with cdshealpix skymap (`#409 <https://github.com/astronomy-commons/hats/pull/409>`__)
- add moc plotting method (`#414 <https://github.com/astronomy-commons/hats/pull/414>`__)
- Correct pixel boundaries when plotting pixels at orders lower than 3 show (`#413 <https://github.com/astronomy-commons/hats/pull/413>`__)


HATS v0.4.2  (2024-10-29)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Introduce aggregate_column_statistics (`#387 <https://github.com/astronomy-commons/hats/pull/387>`__)
- Merge development branch (`#389 <https://github.com/astronomy-commons/hats/pull/389>`__)
- Remove fishy file (`#390 <https://github.com/astronomy-commons/hats/pull/390>`__)
- Unpin astropy version (`#391 <https://github.com/astronomy-commons/hats/pull/391>`__)
- Update copier (`#388 <https://github.com/astronomy-commons/hats/pull/388>`__)
- Convenience method to estimate mindist for a given order. (`#392 <https://github.com/astronomy-commons/hats/pull/392>`__)
- Add custom healpix plotting method (`#374 <https://github.com/astronomy-commons/hats/pull/374>`__)
- Allow custom plot title (`#396 <https://github.com/astronomy-commons/hats/pull/396>`__)
- Use numba jit compilation instead of precompilation (`#395 <https://github.com/astronomy-commons/hats/pull/395>`__)
- Simplify catalog reading (`#394 <https://github.com/astronomy-commons/hats/pull/394>`__)
- Minor plotting fixes (`#403 <https://github.com/astronomy-commons/hats/pull/403>`__)


HATS v0.4.1  (2024-10-17)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Documentation sweep (`#381 <https://github.com/astronomy-commons/hats/pull/381>`__)
- Fix broken unittests (`#383 <https://github.com/astronomy-commons/hats/pull/383>`__)
- Add a getting started page (`#382 <https://github.com/astronomy-commons/hats/pull/382>`__)
- Pin astropy temporarily (`#384 <https://github.com/astronomy-commons/hats/pull/384>`__)
TODO - organize
* @jeremykubica made their first contribution in https://github.com/astronomy-commons/hats/pull/383


HATS v0.4.0  (2024-10-16)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Replace FilePointer with universal pathlib (`#336 <https://github.com/astronomy-commons/hats/pull/336>`__)
- Limit kwargs passed to file.open. (`#341 <https://github.com/astronomy-commons/hats/pull/341>`__)
- Provide better type hints for path-like arguments. (`#342 <https://github.com/astronomy-commons/hats/pull/342>`__)
- Remove unused methods in pixel margin module (`#343 <https://github.com/astronomy-commons/hats/pull/343>`__)
- Hats renaming (`#379 <https://github.com/astronomy-commons/hats/pull/379>`__)
TODO - organize
* Initial renaming (https://github.com/astronomy-commons/hats/pull/352)
* Create HIPS-style properties file (https://github.com/astronomy-commons/hats/pull/358)
* Add default column to properties (https://github.com/astronomy-commons/hats/pull/359)
* Improve reading/writing of fits file. (https://github.com/astronomy-commons/hats/pull/361)
* Set total_rows to non-none value. (https://github.com/astronomy-commons/hats/pull/362)
* Insert dataset dir and use general ra/dec column names. (https://github.com/astronomy-commons/hats/pull/377)


HATS v0.3.9  (2024-08-27)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Pass compression kwargs through file system open. (`#334 <https://github.com/astronomy-commons/hipscat/pull/334>`__)
- Expand strict catalog validation. (`#326 <https://github.com/astronomy-commons/hipscat/pull/326>`__)
- Update LINCC logo with 2024 version (`#335 <https://github.com/astronomy-commons/hipscat/pull/335>`__)


HATS v0.3.8  (2024-08-05)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Store arrow schema when reading catalogs (`#310 <https://github.com/astronomy-commons/hipscat/pull/310>`__)
- Clean up unused methods. Prepare for file_system argument. (`#315 <https://github.com/astronomy-commons/hipscat/pull/315>`__)
- Better mypy hints (`#316 <https://github.com/astronomy-commons/hipscat/pull/316>`__)
- Add utility healpix functions to convert between order, average pixel size and minimum distance between edges (`#318 <https://github.com/astronomy-commons/hipscat/pull/318>`__)


HATS v0.3.7  (2024-07-22)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Return the total number of rows written in metadata. (`#306 <https://github.com/astronomy-commons/hipscat/pull/306>`__)
- Add option to drop empty sibling pixels from final partitioning (`#304 <https://github.com/astronomy-commons/hipscat/pull/304>`__)


HATS v0.3.6  (2024-07-15)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Pin numpy (`#294 <https://github.com/astronomy-commons/hipscat/pull/294>`__)
- Insert healpix shim over (most) healpy operations (`#297 <https://github.com/astronomy-commons/hipscat/pull/297>`__)
- handle_pandas_storage_options (`#296 <https://github.com/astronomy-commons/hipscat/pull/296>`__)


HATS v0.3.5  (2024-06-14)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Enable python 3.12 in CI (`#283 <https://github.com/astronomy-commons/hipscat/pull/283>`__)
- Derive `MarginCacheCatalogInfo` from `CatalogInfo` (`#285 <https://github.com/astronomy-commons/hipscat/pull/285>`__)
- Use pathlib for test path construction. (`#289 <https://github.com/astronomy-commons/hipscat/pull/289>`__)
- Support list of paths for parquet dataset. (`#288 <https://github.com/astronomy-commons/hipscat/pull/288>`__)
- Fix smoke test, uses pathlib (`#290 <https://github.com/astronomy-commons/hipscat/pull/290>`__)
- added support to url params on pixel_catalog_files (`#291 <https://github.com/astronomy-commons/hipscat/pull/291>`__)
- Use mypy-friendly dict type. (`#292 <https://github.com/astronomy-commons/hipscat/pull/292>`__)


HATS v0.3.4  (2024-05-31)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Add catalog filter_by_moc method and replace spatial filters to use it (`#276 <https://github.com/astronomy-commons/hipscat/pull/276>`__)
- Fix box filter moc generation types (`#278 <https://github.com/astronomy-commons/hipscat/pull/278>`__)
- Support more types in json encoding. (`#279 <https://github.com/astronomy-commons/hipscat/pull/279>`__)
- Use nest ordering for point_map.fits file. (`#273 <https://github.com/astronomy-commons/hipscat/pull/273>`__)


HATS v0.3.3  (2024-05-21)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Add dtypes to `HipscatEncoder` (`#274 <https://github.com/astronomy-commons/hipscat/pull/274>`__)
- Pin matplotlib (`#275 <https://github.com/astronomy-commons/hipscat/pull/275>`__)
TODO - organize
* @troyraen made their first contribution in https://github.com/astronomy-commons/hipscat/pull/274


HATS v0.3.2  (2024-05-15)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Refactor catalog alignment moc none check (`#269 <https://github.com/astronomy-commons/hipscat/pull/269>`__)
- Fix infinite loop in outer alignment edge case (`#270 <https://github.com/astronomy-commons/hipscat/pull/270>`__)


HATS v0.3.1  (2024-05-08)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Update pin to newer fsspec versions. (`#265 <https://github.com/astronomy-commons/hipscat/pull/265>`__)
- Add moc as a parameter to HealpixDataset and subclasses (`#263 <https://github.com/astronomy-commons/hipscat/pull/263>`__)
- Remove redundant pixel tree builder class (`#266 <https://github.com/astronomy-commons/hipscat/pull/266>`__)
- Suppress color bar for constant order plot (`#267 <https://github.com/astronomy-commons/hipscat/pull/267>`__)
- Add MOC filter and alignment methods (`#268 <https://github.com/astronomy-commons/hipscat/pull/268>`__)


HATS v0.3.0  (2024-04-22)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Interval Pixel Tree (`#249 <https://github.com/astronomy-commons/hipscat/pull/249>`__)
- Fix `from_healpix` typing (`#256 <https://github.com/astronomy-commons/hipscat/pull/256>`__)
- Change readthedocs links to stable. (`#255 <https://github.com/astronomy-commons/hipscat/pull/255>`__)
- Prepend file protocol to pointer (`#257 <https://github.com/astronomy-commons/hipscat/pull/257>`__)
- Add paths method for getting multiple paths (`#258 <https://github.com/astronomy-commons/hipscat/pull/258>`__)
- Optimize Inner PixelTree Alignment (`#259 <https://github.com/astronomy-commons/hipscat/pull/259>`__)
- Update acknowledgment text (again) (`#260 <https://github.com/astronomy-commons/hipscat/pull/260>`__)


HATS v0.2.10  (2024-04-05)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Use loader to read catalog from almanac entry (`#246 <https://github.com/astronomy-commons/hipscat/pull/246>`__)
- Bump PPT to v2.0.1 (`#250 <https://github.com/astronomy-commons/hipscat/pull/250>`__)
- Readme: update acknowledgements (`#251 <https://github.com/astronomy-commons/hipscat/pull/251>`__)
- Set description for pyproject.toml (`#252 <https://github.com/astronomy-commons/hipscat/pull/252>`__)
TODO - organize
* @hombit made their first contribution in https://github.com/astronomy-commons/hipscat/pull/250


HATS v0.2.9  (2024-03-21)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Change Sphinx theme to book theme (`#239 <https://github.com/astronomy-commons/hipscat/pull/239>`__)
- Use storage options in catalog validation. (`#241 <https://github.com/astronomy-commons/hipscat/pull/241>`__)
- refactor get_projection_method function (`#243 <https://github.com/astronomy-commons/hipscat/pull/243>`__)
- Add storage options to more catalog calls (`#244 <https://github.com/astronomy-commons/hipscat/pull/244>`__)


HATS v0.2.8  (2024-03-14)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Improve testing of visualization. Add kwargs. (`#233 <https://github.com/astronomy-commons/hipscat/pull/233>`__)
- Update README.md (`#234 <https://github.com/astronomy-commons/hipscat/pull/234>`__)
- Match parquet path docstrings to behavior. (`#235 <https://github.com/astronomy-commons/hipscat/pull/235>`__)
- Fix base catalog info types (`#236 <https://github.com/astronomy-commons/hipscat/pull/236>`__)


HATS v0.2.7  (2024-03-08)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Remove slow right ascension wrapping call (`#224 <https://github.com/astronomy-commons/hipscat/pull/224>`__)
- Refactor `filter_from_pixel_list` from `Catalog` to `HealpixDataset` (`#226 <https://github.com/astronomy-commons/hipscat/pull/226>`__)
- Discrete values for legend in plot_pixels (`#228 <https://github.com/astronomy-commons/hipscat/pull/228>`__)
- Notebook for generating test data (`#231 <https://github.com/astronomy-commons/hipscat/pull/231>`__)
TODO - organize
* @nevencaplar made their first contribution in https://github.com/astronomy-commons/hipscat/pull/228
* @dependabot made their first contribution in https://github.com/astronomy-commons/hipscat/pull/229


HATS v0.2.6  (2024-02-26)
==========================================

TODO - categorize


Features
-------------------------

Bugfixes
-------------------------

Deprecations and Removals
-------------------------

Misc
-------------------------

Improved Documentation
-------------------------

- Update to PPT 1.5.3 (`#217 <https://github.com/astronomy-commons/hipscat/pull/217>`__)
- Dataset factory method (`#216 <https://github.com/astronomy-commons/hipscat/pull/216>`__)
- Support pathlib in json encoding (`#219 <https://github.com/astronomy-commons/hipscat/pull/219>`__)
- Pin higher numba version. (`#220 <https://github.com/astronomy-commons/hipscat/pull/220>`__)
- Change argument to radius_arcsec (`#221 <https://github.com/astronomy-commons/hipscat/pull/221>`__)
- Fix formatting in docstrings. (`#222 <https://github.com/astronomy-commons/hipscat/pull/222>`__)

