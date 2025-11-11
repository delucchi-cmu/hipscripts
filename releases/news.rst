
LSDB v0.6.3  (2025-07-27)
==========================================

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

TODO - CATEGORIZE

- Remove soft launch warnings (`#963 <https://github.com/astronomy-commons/lsdb/pull/963>`__)
- Update pyproject.toml (`#965 <https://github.com/astronomy-commons/lsdb/pull/965>`__)


LSDB v0.6.2  (2025-07-26)
==========================================

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

TODO - CATEGORIZE

- Add DP1 paper to citations (`#923 <https://github.com/astronomy-commons/lsdb/pull/923>`__)
- Add `write_catalog` method (`#913 <https://github.com/astronomy-commons/lsdb/pull/913>`__)
- Add error message when non-catalog used as xmatch target in Catalog.crossmatch (`#920 <https://github.com/astronomy-commons/lsdb/pull/920>`__)
- Add option to input a list of values to id_search (`#919 <https://github.com/astronomy-commons/lsdb/pull/919>`__)
- add scaling workflows tutorial (`#925 <https://github.com/astronomy-commons/lsdb/pull/925>`__)
- Update release tracker with nested-pandas version for conda (`#918 <https://github.com/astronomy-commons/lsdb/pull/918>`__)
- Docs: DP1 NERSC instructions (`#931 <https://github.com/astronomy-commons/lsdb/pull/931>`__)
- Docs DP1: fix section numbers (`#928 <https://github.com/astronomy-commons/lsdb/pull/928>`__)
- scaling_workflows.ipynb: fix a typo (`#933 <https://github.com/astronomy-commons/lsdb/pull/933>`__)
- Move region search methods. Remove skymap methods. (`#960 <https://github.com/astronomy-commons/lsdb/pull/960>`__)
- Add test for corrected margin filtering in lsdb.read_hats (`#962 <https://github.com/astronomy-commons/lsdb/pull/962>`__)


LSDB v0.6.1  (2025-07-16)
==========================================

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

TODO - CATEGORIZE

- Update hats-builder property (`#826 <https://github.com/astronomy-commons/lsdb/pull/826>`__)
- Crossmatching tutorial notebook (`#817 <https://github.com/astronomy-commons/lsdb/pull/817>`__)
- include ra and dec columns (`#828 <https://github.com/astronomy-commons/lsdb/pull/828>`__)
- Fix potential issue with meta in count_nested (`#832 <https://github.com/astronomy-commons/lsdb/pull/832>`__)
- Write tutorial on Time Series (`#834 <https://github.com/astronomy-commons/lsdb/pull/834>`__)
- NestedFrame tutorial (`#838 <https://github.com/astronomy-commons/lsdb/pull/838>`__)
- Re-generate test data; organize into collections. (`#833 <https://github.com/astronomy-commons/lsdb/pull/833>`__)
- Recall read hats on filter call on first loaded catalog (`#835 <https://github.com/astronomy-commons/lsdb/pull/835>`__)
- First draft of how to access Rubin data (`#840 <https://github.com/astronomy-commons/lsdb/pull/840>`__)
- Smoother on-ramp when getting started (`#842 <https://github.com/astronomy-commons/lsdb/pull/842>`__)
- Get reStructured Text cells to render. (`#843 <https://github.com/astronomy-commons/lsdb/pull/843>`__)
- Use open_catalog in documentation (`#844 <https://github.com/astronomy-commons/lsdb/pull/844>`__)
- Can use count_nested directly as of nested-pandas==0.4.6 (`#839 <https://github.com/astronomy-commons/lsdb/pull/839>`__)
- Fix path in benchmark module. (`#852 <https://github.com/astronomy-commons/lsdb/pull/852>`__)
- Add `append_columns` to `Catalog.reduce` signature (`#848 <https://github.com/astronomy-commons/lsdb/pull/848>`__)
- Mimic reference API docs of nested-pandas (`#853 <https://github.com/astronomy-commons/lsdb/pull/853>`__)
- Enable grouped TOC in the navbar (`#851 <https://github.com/astronomy-commons/lsdb/pull/851>`__)
- Add "Using Rubin Data" Tutorial (`#845 <https://github.com/astronomy-commons/lsdb/pull/845>`__)
- Tidy up the getting-started page (`#850 <https://github.com/astronomy-commons/lsdb/pull/850>`__)
- Fix TOC links (`#856 <https://github.com/astronomy-commons/lsdb/pull/856>`__)
- Docstring cleanups (`#859 <https://github.com/astronomy-commons/lsdb/pull/859>`__)
- link using_rubin_data nb; Rename Rubin Section (`#860 <https://github.com/astronomy-commons/lsdb/pull/860>`__)
- update catalog object notebook (`#855 <https://github.com/astronomy-commons/lsdb/pull/855>`__)
- Add small notebook on how to load Rubin DP1 photo-z (`#861 <https://github.com/astronomy-commons/lsdb/pull/861>`__)
- port lc viz notebook (`#862 <https://github.com/astronomy-commons/lsdb/pull/862>`__)
- fix bad toc conflict resolution (`#864 <https://github.com/astronomy-commons/lsdb/pull/864>`__)
- Improve screenshot, add download example (`#858 <https://github.com/astronomy-commons/lsdb/pull/858>`__)
- update catalog section (`#865 <https://github.com/astronomy-commons/lsdb/pull/865>`__)
- DP1 outputs: using_rubin_data + visualizing nbs (`#868 <https://github.com/astronomy-commons/lsdb/pull/868>`__)
- Execute Rubin tutorials with true DP1 paths (`#869 <https://github.com/astronomy-commons/lsdb/pull/869>`__)
- add Catalog.rename() (`#857 <https://github.com/astronomy-commons/lsdb/pull/857>`__)
- Tutorial NB: DP1 vs VSX (`#870 <https://github.com/astronomy-commons/lsdb/pull/870>`__)
- fix outdated parquet backend references (`#872 <https://github.com/astronomy-commons/lsdb/pull/872>`__)
- RTD Stable->Latest in readme (`#875 <https://github.com/astronomy-commons/lsdb/pull/875>`__)
- Use UPath in DES-Gaia NB (`#874 <https://github.com/astronomy-commons/lsdb/pull/874>`__)
- Add links to crossmatch notebook, fix errors (`#876 <https://github.com/astronomy-commons/lsdb/pull/876>`__)
- Fix docstring for Catalog.reduce (`#878 <https://github.com/astronomy-commons/lsdb/pull/878>`__)
- Mention available columns (`#881 <https://github.com/astronomy-commons/lsdb/pull/881>`__)
- citation more prominent on web (`#879 <https://github.com/astronomy-commons/lsdb/pull/879>`__)
- remove date for Rubin on CANFAR (`#882 <https://github.com/astronomy-commons/lsdb/pull/882>`__)
- Docs: rename contact us to getting help (`#883 <https://github.com/astronomy-commons/lsdb/pull/883>`__)
- partition_size is in rows (`#884 <https://github.com/astronomy-commons/lsdb/pull/884>`__)
- Update path and date for RSP (`#893 <https://github.com/astronomy-commons/lsdb/pull/893>`__)
- Fix paths to Rubin data (`#897 <https://github.com/astronomy-commons/lsdb/pull/897>`__)
- Introduce and encourage LSST Community forum. (`#900 <https://github.com/astronomy-commons/lsdb/pull/900>`__)
- Remove mention of tract, patch for now (`#901 <https://github.com/astronomy-commons/lsdb/pull/901>`__)
- Add proper warning markdown (`#902 <https://github.com/astronomy-commons/lsdb/pull/902>`__)
- Rename `read_hats` in documentation (`#904 <https://github.com/astronomy-commons/lsdb/pull/904>`__)
- Find ra/dec columns in `from_dataframe` (`#905 <https://github.com/astronomy-commons/lsdb/pull/905>`__)
- Add title to the warning cell (`#903 <https://github.com/astronomy-commons/lsdb/pull/903>`__)
- Change is_builtin_algorithm to accept enum variants (`#907 <https://github.com/astronomy-commons/lsdb/pull/907>`__)
- Update NGC-BTS NB with `crossmatch()` function (`#909 <https://github.com/astronomy-commons/lsdb/pull/909>`__)
- Allow any column in include_columns (`#863 <https://github.com/astronomy-commons/lsdb/pull/863>`__)
- Make upath dependency very explicit. (`#914 <https://github.com/astronomy-commons/lsdb/pull/914>`__)
- Update min versions (`#917 <https://github.com/astronomy-commons/lsdb/pull/917>`__)
TODO - CONFIRM

New Contributors
-------------------------

* @kesiavino made their first contribution in https://github.com/astronomy-commons/lsdb/pull/828
* @clytieq made their first contribution in https://github.com/astronomy-commons/lsdb/pull/857


LSDB v0.6.0  (2025-06-11)
==========================================

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

TODO - CATEGORIZE

- Improve the row-filtering tutorial. (`#799 <https://github.com/astronomy-commons/lsdb/pull/799>`__)
- Skip lsst-sphgeom on Windows (`#809 <https://github.com/astronomy-commons/lsdb/pull/809>`__)
- Modify text with [full] install (`#810 <https://github.com/astronomy-commons/lsdb/pull/810>`__)
- Notebook template (`#784 <https://github.com/astronomy-commons/lsdb/pull/784>`__)
- Handle default columns in `to_hats` (`#806 <https://github.com/astronomy-commons/lsdb/pull/806>`__)
- Set default `margin_threshold` on crossmatch of dataframes (`#813 <https://github.com/astronomy-commons/lsdb/pull/813>`__)
- Accept array-like input for columns (`#812 <https://github.com/astronomy-commons/lsdb/pull/812>`__)
- Address smoke test failure. (`#818 <https://github.com/astronomy-commons/lsdb/pull/818>`__)
- Include skymap in to_hats (`#821 <https://github.com/astronomy-commons/lsdb/pull/821>`__)
- Update pyproject.toml (`#824 <https://github.com/astronomy-commons/lsdb/pull/824>`__)


LSDB v0.5.3  (2025-06-02)
==========================================

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

TODO - CATEGORIZE

- Add .loc/.iloc accessors (`#753 <https://github.com/astronomy-commons/lsdb/pull/753>`__)
- add nested_columns property to catalog (`#755 <https://github.com/astronomy-commons/lsdb/pull/755>`__)
- Add data thumbnail creation to `to_hats` (`#758 <https://github.com/astronomy-commons/lsdb/pull/758>`__)
- Display the total number of available columns (`#762 <https://github.com/astronomy-commons/lsdb/pull/762>`__)
- Support only providing list_columns in catalog.nest_lists (`#768 <https://github.com/astronomy-commons/lsdb/pull/768>`__)
- Add tests for nested column filters (`#763 <https://github.com/astronomy-commons/lsdb/pull/763>`__)
- Raise a helpful error message when the user is accessing a catalog's column that hasn't been loaded. (`#766 <https://github.com/astronomy-commons/lsdb/pull/766>`__)
- Split to_hats tests to separate file. (`#771 <https://github.com/astronomy-commons/lsdb/pull/771>`__)
- Rename lsdb.read_hats to lsdb.hats_catalog (`#770 <https://github.com/astronomy-commons/lsdb/pull/770>`__)
- Write an association table from a catalog (`#772 <https://github.com/astronomy-commons/lsdb/pull/772>`__)
- Update index search documentation (`#775 <https://github.com/astronomy-commons/lsdb/pull/775>`__)
- Refactor/rewrite filtering_large_catalogs nb (now column_filtering.ipynb) (`#777 <https://github.com/astronomy-commons/lsdb/pull/777>`__)
- Add brief note for tract-patch search and link to lsdb-rubin (`#779 <https://github.com/astronomy-commons/lsdb/pull/779>`__)
- Add name (`#781 <https://github.com/astronomy-commons/lsdb/pull/781>`__)
- Add more imports to top-level init. (`#783 <https://github.com/astronomy-commons/lsdb/pull/783>`__)
- 2024 to 2025 in docs (`#791 <https://github.com/astronomy-commons/lsdb/pull/791>`__)
- Tutorial: Setting up a Dask client (`#787 <https://github.com/astronomy-commons/lsdb/pull/787>`__)
- Describe use of `meta=`, reorganize map_partitions+histogram demo (`#792 <https://github.com/astronomy-commons/lsdb/pull/792>`__)
- Update plotting.ipynb (`#794 <https://github.com/astronomy-commons/lsdb/pull/794>`__)
- docs: link to dask from homepage (`#793 <https://github.com/astronomy-commons/lsdb/pull/793>`__)
- update nested-pandas pin (`#797 <https://github.com/astronomy-commons/lsdb/pull/797>`__)
- Require newer HATS (`#802 <https://github.com/astronomy-commons/lsdb/pull/802>`__)
TODO - CONFIRM

New Contributors
-------------------------

* @wilsonbb made their first contribution in https://github.com/astronomy-commons/lsdb/pull/768
* @domoritz made their first contribution in https://github.com/astronomy-commons/lsdb/pull/793


LSDB v0.5.2  (2025-05-07)
==========================================

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

TODO - CATEGORIZE

- Fix plotting defaults (`#707 <https://github.com/astronomy-commons/lsdb/pull/707>`__)
- Catalog collection (`#705 <https://github.com/astronomy-commons/lsdb/pull/705>`__)
- Wrap methods for getting catalog-level parquet statistics. (`#635 <https://github.com/astronomy-commons/lsdb/pull/635>`__)
- Add all columns and original schema properties (`#709 <https://github.com/astronomy-commons/lsdb/pull/709>`__)
- Add catalog `id_search`  (`#710 <https://github.com/astronomy-commons/lsdb/pull/710>`__)
- Address smoke test failure with updated pin (`#714 <https://github.com/astronomy-commons/lsdb/pull/714>`__)
- fix reading behaviour with new hats changes (`#717 <https://github.com/astronomy-commons/lsdb/pull/717>`__)
- Update to PPT v2.0.6 (`#712 <https://github.com/astronomy-commons/lsdb/pull/712>`__)
- Documentation sprint - LINCC up week (`#660 <https://github.com/astronomy-commons/lsdb/pull/660>`__)
- Don't flatten numpy array with unique call. (`#719 <https://github.com/astronomy-commons/lsdb/pull/719>`__)
- Migrate Nested-Dask into LSDB.nested (`#713 <https://github.com/astronomy-commons/lsdb/pull/713>`__)
- Add Nested Crossmatch function  (`#711 <https://github.com/astronomy-commons/lsdb/pull/711>`__)
- Crossmatch dataframes —expand ra/dec column name defaults, clarify docstring wording (`#728 <https://github.com/astronomy-commons/lsdb/pull/728>`__)
- Allow PixelSearch in generate_catalog (`#738 <https://github.com/astronomy-commons/lsdb/pull/738>`__)
- Support Nested Parquet Serialization (`#742 <https://github.com/astronomy-commons/lsdb/pull/742>`__)
- Reduce Updates: infer_nesting and docs for reduce arg limits (`#744 <https://github.com/astronomy-commons/lsdb/pull/744>`__)
- Give names to deps in requirements.txt (`#747 <https://github.com/astronomy-commons/lsdb/pull/747>`__)
- remove 3.9 handling in lsdb.nested backend (`#745 <https://github.com/astronomy-commons/lsdb/pull/745>`__)
- Update PPT and add lowest supported versions (`#746 <https://github.com/astronomy-commons/lsdb/pull/746>`__)
- LSDB.nested catalog generation (`#729 <https://github.com/astronomy-commons/lsdb/pull/729>`__)
- Require newest hats version. (`#752 <https://github.com/astronomy-commons/lsdb/pull/752>`__)


LSDB v0.5.1  (2025-04-17)
==========================================

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

TODO - CATEGORIZE

- Re-arrange to match actual release flow. (`#633 <https://github.com/astronomy-commons/lsdb/pull/633>`__)
- Add lsdb.crossmatch(left, right, ....) method, which supports frames and catalogs (`#629 <https://github.com/astronomy-commons/lsdb/pull/629>`__)
- Add plotting documentation page (`#598 <https://github.com/astronomy-commons/lsdb/pull/598>`__)
- Add an intro to the use of map_partitions. (`#625 <https://github.com/astronomy-commons/lsdb/pull/625>`__)
- Update the Alerce NB (`#648 <https://github.com/astronomy-commons/lsdb/pull/648>`__)
- Suppress pylint warning (`#656 <https://github.com/astronomy-commons/lsdb/pull/656>`__)
- New catalog preview methods tail, sample, and random_sample (`#659 <https://github.com/astronomy-commons/lsdb/pull/659>`__)
- Address mypy issues with latest numpy (`#700 <https://github.com/astronomy-commons/lsdb/pull/700>`__)
- Remove skymap method. (`#698 <https://github.com/astronomy-commons/lsdb/pull/698>`__)
- Add npartitions property to top of catalog. (`#701 <https://github.com/astronomy-commons/lsdb/pull/701>`__)
- Expand possible pixelsearch inputs (`#699 <https://github.com/astronomy-commons/lsdb/pull/699>`__)
- Improve code coverage of `.random_sample` (`#702 <https://github.com/astronomy-commons/lsdb/pull/702>`__)
- Add python 3.13 to CI. (`#704 <https://github.com/astronomy-commons/lsdb/pull/704>`__)
- Require newer hats (`#708 <https://github.com/astronomy-commons/lsdb/pull/708>`__)


LSDB v0.5.0  (2025-03-19)
==========================================

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

TODO - CATEGORIZE

- Operations update metadata correctly (`#596 <https://github.com/astronomy-commons/lsdb/pull/596>`__)
- Verify `UPath` initialization (`#600 <https://github.com/astronomy-commons/lsdb/pull/600>`__)
- Fix hive columns not being updated during merge operations (`#603 <https://github.com/astronomy-commons/lsdb/pull/603>`__)
- fix hive column NA values (`#607 <https://github.com/astronomy-commons/lsdb/pull/607>`__)
- Crossmatch pandas dataframe with an lsdb catalog (`#602 <https://github.com/astronomy-commons/lsdb/pull/602>`__)
- Support the `hats` property `npix_suffix` (`#586 <https://github.com/astronomy-commons/lsdb/pull/586>`__)
- Re-generate test data, and update expectations. (`#604 <https://github.com/astronomy-commons/lsdb/pull/604>`__)
- Add SNAD-PS1-Zubercal NB (`#444 <https://github.com/astronomy-commons/lsdb/pull/444>`__)
- Fix performance of merge functions after updated align_and_apply (`#608 <https://github.com/astronomy-commons/lsdb/pull/608>`__)
- Add loads of links to cite LSDB. (`#609 <https://github.com/astronomy-commons/lsdb/pull/609>`__)
- Add testing after updating catalog structure. (`#614 <https://github.com/astronomy-commons/lsdb/pull/614>`__)
- Fix display of links and headers (`#615 <https://github.com/astronomy-commons/lsdb/pull/615>`__)
- Add filter by column query section (`#616 <https://github.com/astronomy-commons/lsdb/pull/616>`__)
- Update margins when using `map_partitions` (`#621 <https://github.com/astronomy-commons/lsdb/pull/621>`__)
- Add wrapper for `sort_values` (`#619 <https://github.com/astronomy-commons/lsdb/pull/619>`__)
- Parametrize test instead of for loop. (`#623 <https://github.com/astronomy-commons/lsdb/pull/623>`__)
- Dask cluster tips docs (`#617 <https://github.com/astronomy-commons/lsdb/pull/617>`__)
- Don't assume Norder/Dir/Npix columns (`#628 <https://github.com/astronomy-commons/lsdb/pull/628>`__)
- Add simple check for expected common kwarg mistakes (`#630 <https://github.com/astronomy-commons/lsdb/pull/630>`__)
- Update pyproject.toml (`#632 <https://github.com/astronomy-commons/lsdb/pull/632>`__)
TODO - CONFIRM

New Contributors
-------------------------

* @troyraen made their first contribution in https://github.com/astronomy-commons/lsdb/pull/586


LSDB v0.4.6  (2025-03-04)
==========================================

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

TODO - CATEGORIZE

- Update 4-release_tracker.md (`#547 <https://github.com/astronomy-commons/lsdb/pull/547>`__)
- Pass args and kwargs to derive meta on `map_partitions` (`#568 <https://github.com/astronomy-commons/lsdb/pull/568>`__)
- O(1B) -> ~10⁹ (`#567 <https://github.com/astronomy-commons/lsdb/pull/567>`__)
- Update ztf-alerts-sne.ipynb with better plots (`#565 <https://github.com/astronomy-commons/lsdb/pull/565>`__)
- Add "all" options to read_hats columns (`#569 <https://github.com/astronomy-commons/lsdb/pull/569>`__)
- O(1B) -> ~10^9 (`#575 <https://github.com/astronomy-commons/lsdb/pull/575>`__)
- Issue/540/docs updates (`#542 <https://github.com/astronomy-commons/lsdb/pull/542>`__)
- Update performance tutorial with updated analysis (`#574 <https://github.com/astronomy-commons/lsdb/pull/574>`__)
- Separate tutorials into multiple sections (`#577 <https://github.com/astronomy-commons/lsdb/pull/577>`__)
- Do not include pandas metadata in leaf parquet files (`#581 <https://github.com/astronomy-commons/lsdb/pull/581>`__)
- Add example for anonymous S3 catalog reads (`#585 <https://github.com/astronomy-commons/lsdb/pull/585>`__)
- LSSTC to LSST Discovery Alliance (`#587 <https://github.com/astronomy-commons/lsdb/pull/587>`__)


LSDB v0.4.5  (2025-01-24)
==========================================

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

TODO - CATEGORIZE

- Drop support for python 3.9 (`#527 <https://github.com/astronomy-commons/lsdb/pull/527>`__)
- Add plotting to search filters (`#524 <https://github.com/astronomy-commons/lsdb/pull/524>`__)
- Update to PPT 2.0.5 - fixes slack notifications (`#528 <https://github.com/astronomy-commons/lsdb/pull/528>`__)
- Spruce up the contribution guide. (`#531 <https://github.com/astronomy-commons/lsdb/pull/531>`__)
- Address failing benchmarks (`#532 <https://github.com/astronomy-commons/lsdb/pull/532>`__)
- Fix docstrings, move slow nb to pre-executed. (`#533 <https://github.com/astronomy-commons/lsdb/pull/533>`__)
- Use a naive sparse histogram. (`#534 <https://github.com/astronomy-commons/lsdb/pull/534>`__)
- Add more helpful error with non overlapping catalogs (`#537 <https://github.com/astronomy-commons/lsdb/pull/537>`__)
- Remove in csv format in docstring for from_dataframe (`#538 <https://github.com/astronomy-commons/lsdb/pull/538>`__)
- Save named indices in `from_dataframe` (`#539 <https://github.com/astronomy-commons/lsdb/pull/539>`__)
- Require newer hats version (`#545 <https://github.com/astronomy-commons/lsdb/pull/545>`__)
- Use default_columns property from hats catalog if available in read_hats (`#543 <https://github.com/astronomy-commons/lsdb/pull/543>`__)
- Remove typing imports for List, Tuple, Union (`#544 <https://github.com/astronomy-commons/lsdb/pull/544>`__)


LSDB v0.4.4  (2024-12-06)
==========================================

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

TODO - CATEGORIZE

- Implement map type and merging (`#511 <https://github.com/astronomy-commons/lsdb/pull/511>`__)
- Update PPT to 2.0.4 (`#521 <https://github.com/astronomy-commons/lsdb/pull/521>`__)
- Update Catalog Lazy Representation (`#514 <https://github.com/astronomy-commons/lsdb/pull/514>`__)
- Test on Windows OS. (`#522 <https://github.com/astronomy-commons/lsdb/pull/522>`__)


LSDB v0.4.3  (2024-11-26)
==========================================

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

TODO - CATEGORIZE

- Add issue template for release tracking (`#496 <https://github.com/astronomy-commons/lsdb/pull/496>`__)
- Reimport `*Search` from root `__init__.py` (`#498 <https://github.com/astronomy-commons/lsdb/pull/498>`__)
- Fix link to index table tutorial (`#499 <https://github.com/astronomy-commons/lsdb/pull/499>`__)
- Use HATS `filter_by_*` methods for spatial filtering (`#497 <https://github.com/astronomy-commons/lsdb/pull/497>`__)
- Run CI using un-released nested packages. (`#500 <https://github.com/astronomy-commons/lsdb/pull/500>`__)
- Test can be unskipped for column names with spaces (`#501 <https://github.com/astronomy-commons/lsdb/pull/501>`__)
- Add ZTF alerts notebook (`#479 <https://github.com/astronomy-commons/lsdb/pull/479>`__)
- Remove the `lonlat` argument from `ang2vec` (`#504 <https://github.com/astronomy-commons/lsdb/pull/504>`__)
- Add MOC Filter (`#503 <https://github.com/astronomy-commons/lsdb/pull/503>`__)
- Update to hats healpix math (`#509 <https://github.com/astronomy-commons/lsdb/pull/509>`__)
- Update box search for migration (`#507 <https://github.com/astronomy-commons/lsdb/pull/507>`__)
- Add plot points method (`#510 <https://github.com/astronomy-commons/lsdb/pull/510>`__)
- Update margin docs notebook to use new plotting functions (`#513 <https://github.com/astronomy-commons/lsdb/pull/513>`__)
- Fix empty margin catalogs in `from_dataframe` (`#508 <https://github.com/astronomy-commons/lsdb/pull/508>`__)
- Remove margin fine filtering and remove healpy dependency (`#515 <https://github.com/astronomy-commons/lsdb/pull/515>`__)
- Require new hats (`#517 <https://github.com/astronomy-commons/lsdb/pull/517>`__)


LSDB v0.4.2  (2024-11-07)
==========================================

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

TODO - CATEGORIZE

- "Getting data into LSDB" should include a reference to the import topic (`#483 <https://github.com/astronomy-commons/lsdb/pull/483>`__)
- Add catalog validation tutorial (`#480 <https://github.com/astronomy-commons/lsdb/pull/480>`__)
- Tutorial for using index tables and search. (`#484 <https://github.com/astronomy-commons/lsdb/pull/484>`__)
- Address warning on writing catalog. (`#488 <https://github.com/astronomy-commons/lsdb/pull/488>`__)
- Add plot moc method (`#491 <https://github.com/astronomy-commons/lsdb/pull/491>`__)
- Add join catalogs notebook (`#481 <https://github.com/astronomy-commons/lsdb/pull/481>`__)
- Some grammar and clarity updates (`#482 <https://github.com/astronomy-commons/lsdb/pull/482>`__)
- Update nested-dask to v0.3.0 (`#493 <https://github.com/astronomy-commons/lsdb/pull/493>`__)
- Update pyproject.toml (`#494 <https://github.com/astronomy-commons/lsdb/pull/494>`__)
TODO - CONFIRM

New Contributors
-------------------------

* @gitosaurus made their first contribution in https://github.com/astronomy-commons/lsdb/pull/483
* @OliviaLynn made their first contribution in https://github.com/astronomy-commons/lsdb/pull/482


LSDB v0.4.1  (2024-10-29)
==========================================

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

TODO - CATEGORIZE

- Always load index column with no pandas metadata (`#449 <https://github.com/astronomy-commons/lsdb/pull/449>`__)
- Merge development branch (`#452 <https://github.com/astronomy-commons/lsdb/pull/452>`__)
- Update copier (`#448 <https://github.com/astronomy-commons/lsdb/pull/448>`__)
- Fix benchmarking data for HATS migration (`#457 <https://github.com/astronomy-commons/lsdb/pull/457>`__)
- Fix shallow copy of catalog structure (`#447 <https://github.com/astronomy-commons/lsdb/pull/447>`__)
- Write point map on `to_hipscat` (`#439 <https://github.com/astronomy-commons/lsdb/pull/439>`__)
- Disable fine filtering in margin generation for from_dataframe (`#458 <https://github.com/astronomy-commons/lsdb/pull/458>`__)
- Use new healpix plotting from hats (`#462 <https://github.com/astronomy-commons/lsdb/pull/462>`__)
- Update output from docs. Update path to hats catalogs. (`#445 <https://github.com/astronomy-commons/lsdb/pull/445>`__)
- Pre-factor to simplify hats loading (`#461 <https://github.com/astronomy-commons/lsdb/pull/461>`__)
- Patch sky map when catalog has empty partitions (`#474 <https://github.com/astronomy-commons/lsdb/pull/474>`__)
- Remove margin object from `read_hats` (`#475 <https://github.com/astronomy-commons/lsdb/pull/475>`__)
- Update dependencies (`#476 <https://github.com/astronomy-commons/lsdb/pull/476>`__)


LSDB v0.4.0  (2024-10-17)
==========================================

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

TODO - CATEGORIZE

- Use healpix shim for healpy operations. (`#412 <https://github.com/astronomy-commons/lsdb/pull/412>`__)
- Wrap nested_dask dropna function (`#410 <https://github.com/astronomy-commons/lsdb/pull/410>`__)
- Replace FilePointer with universal pathlib (`#413 <https://github.com/astronomy-commons/lsdb/pull/413>`__)
- Set _hipscat_index as pandas index, where possible. (`#415 <https://github.com/astronomy-commons/lsdb/pull/415>`__)
- HATS renaming (`#443 <https://github.com/astronomy-commons/lsdb/pull/443>`__)


LSDB v0.3.0  (2024-08-27)
==========================================

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

TODO - CATEGORIZE

- Replace dask dataframe with nested-dask (`#368 <https://github.com/astronomy-commons/lsdb/pull/368>`__)
- Reformat Docs Structure, Index page, and Getting Started Guide (`#406 <https://github.com/astronomy-commons/lsdb/pull/406>`__)
- Create "Getting data into LSDB" documentation (`#400 <https://github.com/astronomy-commons/lsdb/pull/400>`__)
- update LINCC Logo (`#407 <https://github.com/astronomy-commons/lsdb/pull/407>`__)
- Add merge_asof function to catalog (`#409 <https://github.com/astronomy-commons/lsdb/pull/409>`__)


LSDB v0.2.9  (2024-08-05)
==========================================

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

TODO - CATEGORIZE

- Allow `margin_cache` provided with PosixPath (`#384 <https://github.com/astronomy-commons/lsdb/pull/384>`__)
- Provide arrow schema on HiPSCat catalog creation (`#383 <https://github.com/astronomy-commons/lsdb/pull/383>`__)
- Require recent hipscat. (`#395 <https://github.com/astronomy-commons/lsdb/pull/395>`__)


LSDB v0.2.8  (2024-07-24)
==========================================

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

TODO - CATEGORIZE

- Docs hot fixes (`#378 <https://github.com/astronomy-commons/lsdb/pull/378>`__)
- Make from_dataframe defaults consistent with hipscat-import (`#379 <https://github.com/astronomy-commons/lsdb/pull/379>`__)


LSDB v0.2.7  (2024-07-15)
==========================================

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

TODO - CATEGORIZE

- Docs: performance (`#360 <https://github.com/astronomy-commons/lsdb/pull/360>`__)
- Clean up docstrings for sphinx rendering (`#362 <https://github.com/astronomy-commons/lsdb/pull/362>`__)
- Adds fine filtering on `read_hipscat` (`#350 <https://github.com/astronomy-commons/lsdb/pull/350>`__)
- Change to using dask expressions (`#364 <https://github.com/astronomy-commons/lsdb/pull/364>`__)
- Workflow diagram (`#363 <https://github.com/astronomy-commons/lsdb/pull/363>`__)


LSDB v0.2.6  (2024-06-14)
==========================================

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

TODO - CATEGORIZE

- Enable python 3.12 in CI (`#344 <https://github.com/astronomy-commons/lsdb/pull/344>`__)
- Rename search method and add fine as a search object argument (`#345 <https://github.com/astronomy-commons/lsdb/pull/345>`__)
- Fix empty right catalog error in cross-matching (`#349 <https://github.com/astronomy-commons/lsdb/pull/349>`__)
- Refactor AbstractCrossmatchAlgorithm (`#347 <https://github.com/astronomy-commons/lsdb/pull/347>`__)
- Allow path-like objects in the margin cache argument (`#353 <https://github.com/astronomy-commons/lsdb/pull/353>`__)
- Update catalog threshold estimation (`#356 <https://github.com/astronomy-commons/lsdb/pull/356>`__)
- Use pathlib for test path construction. (`#354 <https://github.com/astronomy-commons/lsdb/pull/354>`__)
- Specify argument name for optional storage_options argument. (`#358 <https://github.com/astronomy-commons/lsdb/pull/358>`__)
- Support to query params on http filesystem (`#355 <https://github.com/astronomy-commons/lsdb/pull/355>`__)
- Support map_partitions ufunc with non data frame output (`#357 <https://github.com/astronomy-commons/lsdb/pull/357>`__)
- Require recent hipscat (`#361 <https://github.com/astronomy-commons/lsdb/pull/361>`__)


LSDB v0.2.5  (2024-05-31)
==========================================

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

TODO - CATEGORIZE

- Allow margin path argument in `read_hipscat` (`#328 <https://github.com/astronomy-commons/lsdb/pull/328>`__)
- Add catalog partition indexer (`#334 <https://github.com/astronomy-commons/lsdb/pull/334>`__)
- Use MOCs for spatial filters (`#331 <https://github.com/astronomy-commons/lsdb/pull/331>`__)


LSDB v0.2.4  (2024-05-21)
==========================================

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

TODO - CATEGORIZE

- Use MOCs for alignment and generate MOC in from_dataframe (`#322 <https://github.com/astronomy-commons/lsdb/pull/322>`__)
- Recreate output directory on `to_hipscat` when using overwrite (`#327 <https://github.com/astronomy-commons/lsdb/pull/327>`__)


LSDB v0.2.3  (2024-05-15)
==========================================

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

TODO - CATEGORIZE

- Load data using pyarrow types (`#306 <https://github.com/astronomy-commons/lsdb/pull/306>`__)
- Fix empty search with margins failing (`#312 <https://github.com/astronomy-commons/lsdb/pull/312>`__)
- Allow empty left partitions on crossmatch (`#315 <https://github.com/astronomy-commons/lsdb/pull/315>`__)
- Update the exporting results page (`#318 <https://github.com/astronomy-commons/lsdb/pull/318>`__)
- Only pass catalog_info to delayed tasks instead of full hipscat catalog metadata (`#317 <https://github.com/astronomy-commons/lsdb/pull/317>`__)
- Address pylint issues (`#321 <https://github.com/astronomy-commons/lsdb/pull/321>`__)
- Allow margin catalogs to be empty on filtering (`#320 <https://github.com/astronomy-commons/lsdb/pull/320>`__)
- Add fixes to documentation on RTD (`#323 <https://github.com/astronomy-commons/lsdb/pull/323>`__)


LSDB v0.2.2  (2024-05-09)
==========================================

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

TODO - CATEGORIZE

- Don't re-execute slow notebooks. (`#299 <https://github.com/astronomy-commons/lsdb/pull/299>`__)
- Remove **kwargs from crossmatching algorithms methods (`#301 <https://github.com/astronomy-commons/lsdb/pull/301>`__)
- Remove usage of pixel tree builder (`#304 <https://github.com/astronomy-commons/lsdb/pull/304>`__)
- Change `require_right_margin` default and update warning message (`#307 <https://github.com/astronomy-commons/lsdb/pull/307>`__)
- Replace `.values` accessor with `.to_numpy()` (`#305 <https://github.com/astronomy-commons/lsdb/pull/305>`__)
- Require new hipscat. (`#311 <https://github.com/astronomy-commons/lsdb/pull/311>`__)


LSDB v0.2.1  (2024-04-26)
==========================================

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

TODO - CATEGORIZE

- Add support for Dask versions >=2024.3.0 with dask expressions (`#288 <https://github.com/astronomy-commons/lsdb/pull/288>`__)
- Use faster hipscat paths method (`#284 <https://github.com/astronomy-commons/lsdb/pull/284>`__)
- Update python and hipscat version pins. (`#289 <https://github.com/astronomy-commons/lsdb/pull/289>`__)
- Add benchmarks for larger catalogs. (`#292 <https://github.com/astronomy-commons/lsdb/pull/292>`__)
- Add map_partitions function to catalog (`#290 <https://github.com/astronomy-commons/lsdb/pull/290>`__)


LSDB v0.2.0  (2024-04-22)
==========================================

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

TODO - CATEGORIZE

- Pin python 3.11 version in CI. (`#269 <https://github.com/astronomy-commons/lsdb/pull/269>`__)
- Add minimum radius option to KDTree crossmatch (`#246 <https://github.com/astronomy-commons/lsdb/pull/246>`__)
- Update unit tests for interval pixel tree (`#272 <https://github.com/astronomy-commons/lsdb/pull/272>`__)
- Change readthedocs URLs to stable (`#275 <https://github.com/astronomy-commons/lsdb/pull/275>`__)
- Restructure docs tree (`#276 <https://github.com/astronomy-commons/lsdb/pull/276>`__)
- Cross-reference hipscat in the API docs (`#277 <https://github.com/astronomy-commons/lsdb/pull/277>`__)
- Optimize skymap at high fixed order (`#274 <https://github.com/astronomy-commons/lsdb/pull/274>`__)
- Improve contribution guide (`#278 <https://github.com/astronomy-commons/lsdb/pull/278>`__)
- Update acknowledgment text (`#280 <https://github.com/astronomy-commons/lsdb/pull/280>`__)
- Add __getitem__ accessor for catalogs (`#281 <https://github.com/astronomy-commons/lsdb/pull/281>`__)


LSDB v0.1.6  (2024-04-05)
==========================================

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

TODO - CATEGORIZE

- Change sphinx theme (`#233 <https://github.com/astronomy-commons/lsdb/pull/233>`__)
- fix notebook tqdm rendering (`#234 <https://github.com/astronomy-commons/lsdb/pull/234>`__)
- Use custom representation for catalog. (`#235 <https://github.com/astronomy-commons/lsdb/pull/235>`__)
- Filter based on HEALpix order (`#238 <https://github.com/astronomy-commons/lsdb/pull/238>`__)
- Initialize BoxSearch in search package (`#240 <https://github.com/astronomy-commons/lsdb/pull/240>`__)
- Add "Working with large catalogs" notebook (`#236 <https://github.com/astronomy-commons/lsdb/pull/236>`__)
- Update unit test data (`#243 <https://github.com/astronomy-commons/lsdb/pull/243>`__)
- Add installation section (`#239 <https://github.com/astronomy-commons/lsdb/pull/239>`__)
- add skymap_histogram function and  skymap at specified order (`#245 <https://github.com/astronomy-commons/lsdb/pull/245>`__)
- Update Skymap default (`#250 <https://github.com/astronomy-commons/lsdb/pull/250>`__)
- Polish landing page (`#256 <https://github.com/astronomy-commons/lsdb/pull/256>`__)
- Add quickstart guide (`#255 <https://github.com/astronomy-commons/lsdb/pull/255>`__)
- DES-GAIA cross-match tutorial (`#166 <https://github.com/astronomy-commons/lsdb/pull/166>`__)
- Add margin documentation notebook (`#244 <https://github.com/astronomy-commons/lsdb/pull/244>`__)
- Update PPT version (`#262 <https://github.com/astronomy-commons/lsdb/pull/262>`__)
- Update readme acknowledgement (`#263 <https://github.com/astronomy-commons/lsdb/pull/263>`__)
- Update getting started link README (`#264 <https://github.com/astronomy-commons/lsdb/pull/264>`__)
TODO - CONFIRM

New Contributors
-------------------------

* @ykwang1 made their first contribution in https://github.com/astronomy-commons/lsdb/pull/238


LSDB v0.1.5  (2024-03-14)
==========================================

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

TODO - CATEGORIZE

- Update README.md (`#218 <https://github.com/astronomy-commons/lsdb/pull/218>`__)
- Pin dask. (`#221 <https://github.com/astronomy-commons/lsdb/pull/221>`__)
- Add plot_pixels method to Catalog (`#229 <https://github.com/astronomy-commons/lsdb/pull/229>`__)
- Change metadata loading method to use right file (`#230 <https://github.com/astronomy-commons/lsdb/pull/230>`__)
- Documentation improvements (`#222 <https://github.com/astronomy-commons/lsdb/pull/222>`__)
- Work around pandas warnings. (`#220 <https://github.com/astronomy-commons/lsdb/pull/220>`__)
- Load catalog subset at the I/O level (`#231 <https://github.com/astronomy-commons/lsdb/pull/231>`__)
- Update pyproject.toml (`#232 <https://github.com/astronomy-commons/lsdb/pull/232>`__)


LSDB v0.1.4  (2024-03-11)
==========================================

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

TODO - CATEGORIZE

- Fix doc build error. (`#185 <https://github.com/astronomy-commons/lsdb/pull/185>`__)
- Add test that we can handle pathlib.Path (`#186 <https://github.com/astronomy-commons/lsdb/pull/186>`__)
- Create add-issue-to-project-tracker.yml (`#184 <https://github.com/astronomy-commons/lsdb/pull/184>`__)
- Numpy tricks for box_search.py (`#188 <https://github.com/astronomy-commons/lsdb/pull/188>`__)
- Add box filter benchmark on partition (`#190 <https://github.com/astronomy-commons/lsdb/pull/190>`__)
- Add spatial filtering to margin catalogs (`#192 <https://github.com/astronomy-commons/lsdb/pull/192>`__)
- Create validation method. (`#183 <https://github.com/astronomy-commons/lsdb/pull/183>`__)
- Optionally suppress margin on from_dataframe (`#194 <https://github.com/astronomy-commons/lsdb/pull/194>`__)
- Add Skymap function to catalog (`#198 <https://github.com/astronomy-commons/lsdb/pull/198>`__)
- Change _DIST to _dist_arcsec (`#207 <https://github.com/astronomy-commons/lsdb/pull/207>`__)
- Notebook for generating test data (`#210 <https://github.com/astronomy-commons/lsdb/pull/210>`__)
- Save margin catalog to disk (`#208 <https://github.com/astronomy-commons/lsdb/pull/208>`__)
- Prune empty partitions from catalog (`#205 <https://github.com/astronomy-commons/lsdb/pull/205>`__)
- Apply PPT v2.0  (`#217 <https://github.com/astronomy-commons/lsdb/pull/217>`__)
- Add option for coarse spatial filtering (`#209 <https://github.com/astronomy-commons/lsdb/pull/209>`__)
- Patch margin cache generation (`#216 <https://github.com/astronomy-commons/lsdb/pull/216>`__)


LSDB v0.1.3  (2024-02-27)
==========================================

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

TODO - CATEGORIZE

- Docs: fix link to notebooks page (`#154 <https://github.com/astronomy-commons/lsdb/pull/154>`__)
- Add PS1 image to ZTF-NGC NB (`#153 <https://github.com/astronomy-commons/lsdb/pull/153>`__)
- Fix inconsistent `_hipscat_index` type (`#157 <https://github.com/astronomy-commons/lsdb/pull/157>`__)
- Apply copier update and black formatting (`#158 <https://github.com/astronomy-commons/lsdb/pull/158>`__)
- Apply PPT 1.5.3 (`#164 <https://github.com/astronomy-commons/lsdb/pull/164>`__)
- Catch warnings, and add data to avoid them. (`#168 <https://github.com/astronomy-commons/lsdb/pull/168>`__)
- Added basic tutorial outline (`#162 <https://github.com/astronomy-commons/lsdb/pull/162>`__)
- Add package version property (`#165 <https://github.com/astronomy-commons/lsdb/pull/165>`__)
- Fix installation errors (`#173 <https://github.com/astronomy-commons/lsdb/pull/173>`__)
- Use margin in joining catalogs (`#177 <https://github.com/astronomy-commons/lsdb/pull/177>`__)
- Change cone search argument to radius_arcsec (`#179 <https://github.com/astronomy-commons/lsdb/pull/179>`__)
- Query method updates margin catalog (`#172 <https://github.com/astronomy-commons/lsdb/pull/172>`__)
- Generate margin catalog on `from_dataframe` (`#175 <https://github.com/astronomy-commons/lsdb/pull/175>`__)

