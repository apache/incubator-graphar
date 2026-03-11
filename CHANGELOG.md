# Changelog
All changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [v0.11.4] - 2024-03-27
### Added

- [Minor][Spark] Add SPARK_TESTING variable to increase tests performance by @SemyonSinchenko in https://github.com/apache/graphar/pull/405
- Bump up GraphAr version to v0.11.4 by @acezen in https://github.com/apache/graphar/pull/417

### Changed

- [FEAT][C++] Enhance the validation of writer with arrow::Table's Validate by @acezen in https://github.com/apache/graphar/pull/410
- [FEAT][C++] Change the default namespace to `graphar` by @acezen in https://github.com/apache/graphar/pull/413
- [FEAT][C++] Not allow setting custom namespace for code clarity by @acezen in https://github.com/apache/graphar/pull/415

### Docs

- [Feat][Doc] Refactor and update the format specification document by @acezen in https://github.com/apache/graphar/pull/387

## [v0.11.3] - 2024-03-12
### Added

- [Feat][Spark] Split datasources and core, prepare for support of multiple spark versions by @SemyonSinchenko in https://github.com/apache/graphar/pull/369
- [Feat][Format][Spark] Add nullable key in meta-data by @Thespica in https://github.com/apache/graphar/pull/365
- [Feat][Spark] Spark 3.3.x support as a Maven Profile by @SemyonSinchenko in https://github.com/apache/graphar/pull/376
- [C++] Include an example of converting SNAP datasets to GraphAr format by @lixueclaire in https://github.com/apache/graphar/pull/386
- [Feat][C++] Support  `Date` and `Timestamp` data type by @acezen in https://github.com/apache/graphar/pull/398
- Bump up GraphAr version to v0.11.3 by @acezen in https://github.com/apache/graphar/pull/400

### Changed

- [Feat][Spark] Update PySpark bindings following GraphAr Spark by @SemyonSinchenko in https://github.com/apache/graphar/pull/374
- [Minor][C++] Revise the unsupported data type error msg to give more information by @acezen in https://github.com/apache/graphar/pull/391

### Fixed

- [BugFix][C++] Fix bug: PropertyGroup with empty properties make VertexInfo/EdgeInfo dumps failed by @acezen in https://github.com/apache/graphar/pull/393
- [BugFix][C++]: Fix `VertexInfo/EdgeInfo` can not be saved to a URI path by @acezen in https://github.com/apache/graphar/pull/395
- [Improvement][C++] Fixes compilation warnings in C++ SDK by @sighingnow in https://github.com/apache/graphar/pull/388

### Docs

- [Feat][Doc] update Spark documentation by introducing Maven Profiles by @SemyonSinchenko in https://github.com/apache/graphar/pull/380
- [Improvement][Doc] Provide an implementation status page to indicate libraries status of format implementation support by @acezen in https://github.com/apache/graphar/pull/373
- [Minor][Doc] Fix the link of the images by @acezen in https://github.com/apache/graphar/pull/383
- [Minor][Doc] Update and fix the implementation status page by @lixueclaire in https://github.com/apache/graphar/pull/385
- [Feat][Doc] switch to poetry project for docs generating by @SemyonSinchenko in https://github.com/apache/graphar/pull/384

## [v0.11.2] - 2024-02-24
### Added

- [Feat][Format][C++] Support nullable key for property in meta-data by @Thespica in https://github.com/apache/graphar/pull/355
- [Feat][Format][C++] Support extra info in graph info by @acezen in https://github.com/apache/graphar/pull/356

### Changed
- [Improvement][Spark] Try to make neo4j generate DataFrame with the correct data type by @acezen in https://github.com/apache/graphar/pull/353
- [Improve][C++] Revise the ArrowChunkReader constructors by remove redundant parameter by @acezen in https://github.com/apache/graphar/pull/360
- [Improvement][Doc][CPP] Complement the api reference document of cpp by @acezen in https://github.com/apache/graphar/pull/364
- Bump up GraphAr version to v0.11.2 by @acezen in https://github.com/apache/graphar/pull/371

### Fixed

- [Chore][C++] fix err message by @jasinliu in https://github.com/apache/graphar/pull/345
- [BugFix][C++] Update the testing path with latest testing repo by @acezen in https://github.com/apache/graphar/pull/346

### Docs

- [Doc] Enhance the ReadMe with additional information about the GraphAr libraries by @lixueclaire in https://github.com/apache/graphar/pull/349
- [Minor][Doc] Update publication information and fix link in ReadMe by @lixueclaire in https://github.com/apache/graphar/pull/350
- [Minor][Doc] Minor fix typo of cpp reference by @acezen in https://github.com/apache/graphar/pull/363

## [v0.11.1] - 2024-01-24
### Changed

- [Improvement][Spark] Improve the writer efficiency with parallel process by @acezen in https://github.com/apache/graphar/pull/329
- [Feat][Spark] Memory tuning for GraphAr spark with persist and storage level by @acezen in https://github.com/apache/graphar/pull/326
- Bump up GraphAr version to v0.11.1 by @acezen in https://github.com/apache/graphar/pull/342

### Fixed

- [Minor][Spark] Fix typo by @acezen in https://github.com/apache/graphar/pull/327
- [Bug][C++] Add implement of property<bool> by @jasinliu in https://github.com/apache/graphar/pull/337
- [BugFix][C++] Check  is not nullptr before calling ToString and fix empty prefix bug by @acezen in https://github.com/apache/graphar/pull/339

### Docs

- [Minor][Doc] Update getting-started.rst to fix a typo by @jasinliu in https://github.com/apache/graphar/pull/325
- [Minor][Doc] Remove unused community channel and add publication citation by @acezen in https://github.com/apache/graphar/pull/331
- [Minor][Doc] Fix README by @acezen in https://github.com/apache/graphar/pull/332
- [Minor][Spark] minor doc fix by @acezen in https://github.com/apache/graphar/pull/336

## [v0.11.0] - 2024-01-15
### Added

- Bump up GraphAr version to v0.11.0 @acezen
- [Feat][Spark] Align info implementation of spark with c++ (#316) Weibin Zeng
- [Feat][Spark] Implementation of PySpark bindings to Scala API (#300) Semyon
- [Feat][C++] Initialize the micro benchmark for c++ (#299) Weibin Zeng
- [Improve][Java] Get test resources form environment variables, and remove all print sentences (#309) John
- [Feat][Spark] Add Neo4j importer (#243) Liu Jiajun
- [FEAT][C++] Support `list<string>` data type (#302) Weibin Zeng
- [Minor][Dev] Update the PR template (#301) Weibin Zeng
- [Feat][C++] Support List Data Type, use `list<float>` as example (#296) Weibin Zeng
- [FEAT][C++] Refactor the C++ SDK with forward declaration and shared ptr (#290) Weibin Zeng
- [FEAT][C++] Use `shared_ptr` in all readers and writers (#281) Weibin Zeng
- [Feat][Java] Fill two incompatible gaps between C++ and Java (#279) John @Thespica

### Changed

- [Improvement][Spark] Change VertexWriter constructor signature (#314) Semyon
- [Feat][Spark] Update snakeyaml to 2.x.x version (#312) Semyon
- [Minor][License] Update the license header and add license check in CI (#294) Weibin Zeng
- [Minor][C++] Improve the validation check (#310) Weibin Zeng
- [Minor][Dev] Update release workflow to make release easy and revise other workflows (#323) Weibin Zeng

### Fixed

- [Minor][Spark] Fix Spark comparison bug (#318) Zhang Lei
- [Minor][Doc] Fix spark url in README.m (#317) Zhang Lei
- [BugFix][Spark] Fix the comparison behavior of Property/PropertyGroup/AdjList (#306) Weibin Zeng
- [BugFix][Spark] change maven-site-plugin to 3.7.1 (#305) Weibin Zeng
- [Minor][Doc] Fix the cpp reference doc (#295) Weibin Zeng
- [Minor][C++] Fix typo: REGULAR_SEPERATOR -> REGULAR_SEPARATOR (#293) Weibin Zeng
- [BugFix][C++] Finalize S3 in FileSystem destructor (#289) Weibin Zeng
- [Minor][Doc] Fix the typos of document (#282) Weibin Zeng
- [BugFix][JAVA] Fix invalid option to skip building GraphAr c++ internally for java (#284) John

### Docs

- [Doc][Improvement] Reorg the document structure by libraries (#292) Weibin Zeng

## [v0.10.0] - 2023-11-10
### Added

- [Feat][Spark] Add examples to show how to load/dump data from/to GraphAr for Nebula (#244) (Liu Xiao) [#244](https://github.com/apache/graphar/pull/244)
- [Minor][Spark] Support get GraphAr Spark from Maven (#250) (Weibin Zeng) [#250](https://github.com/apache/graphar/pull/250)
- [Improvement][C++] Use inherit to implement EdgesCollection (#238) (Weibin Zeng) [#238](https://github.com/apache/graphar/pull/238)
- [C++] Add examples about how to use C++ reader/writer (#252) (lixueclaire) [#252](https://github.com/apache/graphar/pull/252)
- [Improve][C++] Use arrow shared library if arrow installed (#263) (Weibin Zeng) [#263](https://github.com/apache/graphar/pull/263)
- [Improve][Java] Make EdgesCollection and VerticesCollection support foreach loop (#270) (John) [#270](https://github.com/apache/graphar/pull/270)
- [Minor][CI] Install certain version of arrow in CI to avoid breaking down CI when arrow upgrade (#273) (Weibin Zeng) [#273](https://github.com/apache/graphar/pull/273)
- [Improvement][Spark] Complement the error messages of spark SDK (#278) (Weibin Zeng) [#278](https://github.com/apache/graphar/pull/278)
- [Feat][Format] Add internal id column to vertex payload file (#264) (Weibin Zeng) [#264](https://github.com/apache/graphar/pull/264)

### Changed

- [Minor][C++] Update the C++ SDK version config (#266) (Weibin Zeng) [#266](https://github.com/apache/graphar/pull/266)
- [Doc][BugFix] Fix missing of scaladoc and javadoc in website (#269) (John) [#269](https://github.com/apache/graphar/pull/269)

### Fixed

- [BUG][C++] Fix testing data path of examples (#251) (lixueclaire) [#251](https://github.com/apache/graphar/pull/251)
- [BugFix][Spark] Close the FileSystem Object (haohao0103) [#258](https://github.com/apache/graphar/pull/258)
- [BugFix][JAVA] Fix the building order bug of JAVA SDK (#261) (Weibin Zeng) [#261](https://github.com/apache/graphar/pull/261)

### Docs

- [Minor][Doc]Add release-process.md to explain the release process, as supplement of road map (#254) (Weibin Zeng) [#254](https://github.com/apache/graphar/pull/254)
- [Doc][Spark] Update the doc: fix the outdated argument annotations and typo (#267) (Weibin Zeng) [#267](https://github.com/apache/graphar/pull/267)
- [Doc] Provide Java's reference library, documentation for users and developers (#242) (John) [#242](https://github.com/apache/graphar/pull/242)


## [v0.9.0] - 2023-10-08
### Added

- Define code style for spark and java and add code format check to CI (#232) (Weibin Zeng) [#232](https://github.com/apache/graphar/pull/232)
- [FEAT][JAVA] Implement READERS and WRITERS for Java (#233) (John) [#233](https://github.com/apache/graphar/pull/233)
- [Spark] Support property filter pushdown by utilizing payload file formats (#221) (Ziyi Tan) [#221](https://github.com/apache/graphar/pull/221)

## [v0.8.0] - 2023-08-30
### Added

- [Minor][Spark] Adapt spark yaml format to BLOCK  (#217) (Weibin Zeng) [#217](https://github.com/apache/graphar/pull/217)
- [Feat][C++] Output the error message when access value in Result fail (#222) (Weibin Zeng) [#222](https://github.com/apache/graphar/pull/222)
- [Feat][Java] Initialize the JAVA SDK: add INFO implementation (#212) (John) [#212](https://github.com/apache/graphar/pull/212)
- [Feat][C++] Support building GraphAr with system installed arrow (#230) (Weibin Zeng) [#230](https://github.com/apache/graphar/pull/230)

### Changed

- [FEAT] Unify the name:`utils` -> `util` and the namespace of `GraphAr::util` (#225) (Weibin Zeng) [#225](https://github.com/apache/graphar/pull/225)

### Fixed

- [Minor] Fix the broken CI of doc (#214) (Weibin Zeng) [#214](https://github.com/apache/graphar/pull/214)
- [BugFix][Spark] Fix compile error under JDK8 and maven 3.9.x (#216) (Liu Xiao) [#216](https://github.com/apache/graphar/pull/216)
- [BugFix][C++] Remove arrow header from GraphAr's header (#229) (Weibin Zeng) [#229](https://github.com/apache/graphar/pull/229)

## [v0.7.0] - 2023-07-24
### Added

- [C++] Support property filter pushdown by utilizing payload file formats (#178) (Ziyi Tan) [#178](https://github.com/apache/graphar/pull/178)

### Changed

- [C++][Improvement] Redesign and unify the implementation of validation in C++ Writer/Builder (#186) (lixueclaire) [#186](https://github.com/apache/graphar/pull/186)
- [Improvement][C++] Refine the error message of errors of C++ SDK (#192) (Weibin Zeng) [#192](https://github.com/apache/graphar/pull/192)
- [Improvement][C++] Refine the error message of Reader SDK (#195) (Ziyi Tan) [#195](https://github.com/apache/graphar/pull/195)
- Update the favicon image (#199) (Weibin Zeng) [#199](https://github.com/apache/graphar/pull/199)
- Update doc comments in graph_info.h (#204) (John) [#204](https://github.com/apache/graphar/pull/204)
- [Spark] Refine the `GraphWriter` to automatically generate graph info and improve the Neo4j case (#196) (Weibin Zeng) [#196](https://github.com/apache/graphar/pull/196)

### Fixed

- Fixes the pull_request_target usage to avoid the secret leak issue. (#193) (Tao He) [#193](https://github.com/apache/graphar/pull/193)
- Fixes the link to the logo image in README (#198) (Tao He) [#198](https://github.com/apache/graphar/pull/198)
- [Minor][C++] Fix grammar mistakes. (#208) (John) [#208](https://github.com/apache/graphar/pull/208)

### Docs

- [Minor][Doc] Add GraphAr logo to README (#197) (Weibin Zeng) [#197](https://github.com/apache/graphar/pull/197)
- [Spark][Doc]Add java version for neo4j example. (#207) (Liu Jiajun) [#207](https://github.com/apache/graphar/pull/207)

## [v0.6.0] - 2023-06-09
### Added

- [C++] Support to get reference of the property in Vertex/Edge (#156) (lixueclaire) [#156](https://github.com/apache/graphar/pull/156)
- [C++] Align arrow version to system if arrow installed (#162) (@acezen Weibin Zeng) [#162](https://github.com/apache/graphar/pull/162)
- [BugFix] [C++] Make examples to generate result files under build type of release (#173) (lixueclaire) [#173](https://github.com/apache/graphar/pull/173)
- [Improvement][C++] Use recommended parameter to sort in Writer (#177) (@lixueclaire lixueclaire) [#177](https://github.com/apache/graphar/pull/177)
- [C++][Improvement] Add validation of different levels for builders in C++ library (#181) (lixueclaire) [#181](https://github.com/apache/graphar/pull/181)

### Changed

### Fixed

- Fix compile error on ARM platform (#158) (Weibin Zeng) [#158](https://github.com/apache/graphar/pull/158)
- [C++][BugFix] Fix the arrow acero not found error when building with arrow 12.0.0 or greater (#164) (Weibin Zeng) [#164](https://github.com/apache/graphar/pull/164)

### Docs

- [Doc] Refine the documentation of file format design (#165) (lixueclaire) [#165](https://github.com/apache/graphar/pull/165)
- [Doc] Improve spelling (#175) (Ziyi Tan) [#175](https://github.com/apache/graphar/pull/175)
- [MINOR][DOC] Add mail list to our communication tools and add community introduction (#179) (Weibin Zeng) [#179](https://github.com/apache/graphar/pull/179)
- [Doc]Refine README in cpp about building (#182) (John) [#182](https://github.com/apache/graphar/pull/182)

## [v0.5.0] - 2023-05-12
### Added

- Enable  arrow S3 support to support reading and writing file with S3/OSS (#125) (Weibin Zeng) [#125](https://github.com/apache/graphar/pull/125)
- [Improvement][C++] Add validation for data types for writers in C++ library (#136) (lixueclaire) [#136](https://github.com/apache/graphar/pull/136)
- [C++] Add vertex_count file for storing edges in GraphAr (#138) (lixueclaire) [#138](https://github.com/apache/graphar/pull/138)
- [FEAT] Use single header yaml parser `mini-yaml` (#142) (Weibin Zeng) [#142](https://github.com/apache/graphar/pull/142)
- Implement the add-assign operator for VertexIter (#151) (lixueclaire) [#151](https://github.com/apache/graphar/pull/151)

### Changed

- [Improvement][C++] Improve the usability of EdgesCollection (#133) (lixueclaire) [#133](https://github.com/apache/graphar/pull/133)
- [Minor] Update README: add information about weekly meeting (#139) (Weibin Zeng) [#139](https://github.com/apache/graphar/pull/139)
- [Minor] Make the curl interface private (#146) (Weibin Zeng) [#146](https://github.com/apache/graphar/pull/146)
- [Doc] Update the images of README (#145) (Weibin Zeng) [#145](https://github.com/apache/graphar/pull/145)
- [Spark] Update the Spark library to align with the latest file format design (#144) (lixueclaire) [#144](https://github.com/apache/graphar/pull/144)
- [Minor][Doc]Remove deleted methods from API Reference (#149) (lixueclaire) [#149](https://github.com/apache/graphar/pull/149)
- [Doc] Refine building steps to be more clear in ReadMe (#154) (lixueclaire) [#154](https://github.com/apache/graphar/pull/154)

### Fixed

- [BugFix][C++] Fix next_chunk() of readers in the C++ library (#137) (lixueclaire) [#137](https://github.com/apache/graphar/pull/137)
- [Minor] HotFix the link error of libcurl when building test (#147) (Weibin Zeng) [#147](https://github.com/apache/graphar/pull/147)
- [Minor] Fix the overview image (#148) (Weibin Zeng) [#148](https://github.com/apache/graphar/pull/148)
- [Minor] Fix building arrow bug on centos8 (#150) (Weibin Zeng) [#150](https://github.com/apache/graphar/pull/150)

## [v0.4.0] - 2023-04-13
### Added

- [Minor] Add discord invite link and banner to README (#129) (@acezen Weibin Zeng) [#129](https://github.com/apache/graphar/pull/129)
- [Improvement][C++] Implement the add operator for VertexIter (#128) (@lixueclaire lixueclaire) [#128](https://github.com/apache/graphar/pull/128)
- [C++] Add edge count file in GraphAr (#132) (lixueclaire) [#132](https://github.com/apache/graphar/pull/132)

### Changed

- Disable jemalloc when building the bundled arrow (#122) (@sighingnow Tao He) [#122](https://github.com/apache/graphar/pull/122)
- [Minor][C++] Adjust the dependency version of arrow and fix arrow header conflict bug (#134) (Weibin Zeng) [#134](https://github.com/apache/graphar/pull/134)
- [Minor] Update testing data (#135) (Weibin Zeng) [#135](https://github.com/apache/graphar/pull/135)

### Fixed

- [Minor][C++] Fix compile warning (#123) (Yee) [#123](https://github.com/apache/graphar/pull/123)
- Fix test data path for examples (#131) (lixueclaire) [#131](https://github.com/apache/graphar/pull/131)

## [v0.3.0] - 2023-03-10
### Added

- [Improvement][Spark] Add helper objects and methods for loading info classes from files (#112) (lixueclaire) [#112](https://github.com/apache/graphar/pull/112)
- [Improvement][Spark] Provide APIs for data transformation at the graph level (#113) (lixueclaire) [#113](https://github.com/apache/graphar/pull/113)
- [Improvement][Spark] Provide APIs for data reading and writing at the graph level  (#114) (Weibin Zeng) [#114](https://github.com/apache/graphar/pull/114)
- [Examples][Spark] Add examples of integrating with the Neo4j spark connector as an application of GraphAr (#107) (lixueclaire) [#107](https://github.com/apache/graphar/pull/107)

### Changed

- Refine the overview figure and fix the typos in documentation (#117) (lixueclaire) [#117](https://github.com/apache/graphar/pull/117)
- [Improvement][DevInfra] Reorg the code directory to easily to extend libraries (#116) (Weibin Zeng) [#116](https://github.com/apache/graphar/pull/116)
- [Minor][Doc] Remove the invalid link (#121) (Weibin Zeng) [#121](https://github.com/apache/graphar/pull/121)

### Fixed

- [BugFix][Spark] Fix the bug that VertexWrite does not generate vertex count file (#110) (Weibin Zeng) [#110](https://github.com/apache/graphar/pull/110)

## [v0.2.0] - 2023-02-23
### Added

- [Improvement] [Spark] Add methods for Spark Reader and improve the performance (#87) (lixueclaire) [#87](https://github.com/apache/graphar/pull/87)
- Add pre-commit configuration and instructions (#93) (Tao He) [#93](https://github.com/apache/graphar/pull/93)
- Handle comments correctly for preview PR docs (#94) (Tao He) [#94](https://github.com/apache/graphar/pull/94)
- [Improve] Add auxiliary functions to get vertex chunk num or edge chunk num with infos (#95) (Weibin Zeng) [#95](https://github.com/apache/graphar/pull/95)
- [Improve] Use gar-related names for arrow project and ccache to avoid duplicated project name (#102) (Weibin Zeng) [#102](https://github.com/apache/graphar/pull/102)
- Add prefix to arrow definitions to avoid conflicts (#106) (Tao He) [#106](https://github.com/apache/graphar/pull/106)

### Changed

- [Improve][Spark] Improve the performance of GraphAr Spark Reader (#84) (lixueclaire) [#84](https://github.com/apache/graphar/pull/84)
- Cast StringArray to LargeStringArray otherwise we will fill when we need to contenate chunks (#105) (Tao He) [#105](https://github.com/apache/graphar/pull/105)
- [Improvement] Improve GraphAr spark writer performance and implement custom writer builder to bypass spark's write behavior (#92) (Weibin Zeng) [#92](https://github.com/apache/graphar/pull/92)
- [Improvement][FileFormat] Write CSV payload files with header (#85) (Weibin Zeng) [#85](https://github.com/apache/graphar/pull/85)
- Update the source code url of GraphScope fragment builder and writer (#103) (Weibin Zeng) [#103](https://github.com/apache/graphar/pull/103)

### Fixed

- [BugFix] Fix the Spark Writer bug when the column name contains a dot(.) (#101) (lixueclaire) [#101](https://github.com/apache/graphar/pull/101)
- It should be linker flags, suppressing the clang warnings (#104) (Tao He) [#104](https://github.com/apache/graphar/pull/104)
- Address issues in handling yaml-cpp correctly when requires GraphAr in external projects (#91) (Tao He) [#91](https://github.com/apache/graphar/pull/91)

## [v0.1.0] - 2023-01-11
### Added
- Add ccache to github actions by @acezen in https://github.com/apache/incubator-graphar/pull/12
- Add issue template and pull request template to help user easy to get… by @acezen in https://github.com/apache/incubator-graphar/pull/13
- Add CODE_OF_CONDUCT.md by @acezen in https://github.com/apache/incubator-graphar/pull/26
- Add InfoVersion to store version information of info and support data type extension base on info version by @acezen in https://github.com/apache/incubator-graphar/pull/27
- Initialize the spark tool of GraphAr and implement the Info and IndexGenerator  by @acezen in https://github.com/apache/incubator-graphar/pull/45
- organize an example pagerank app employing the gar library (#44) by @andydiwenzhu in https://github.com/apache/incubator-graphar/pull/46
- Initialize the implementation of spark writer by @acezen in https://github.com/apache/incubator-graphar/pull/51
- Initialize implementation for spark reader by @lixueclaire in https://github.com/apache/incubator-graphar/pull/52
- Add release and reviewing tutorial to contributing guide by @acezen in https://github.com/apache/incubator-graphar/pull/53
- Add introduction about GraphAr Spark tools in document  by @lixueclaire in https://github.com/apache/incubator-graphar/pull/58
- Add spark tool api reference to doc by @acezen in https://github.com/apache/incubator-graphar/pull/59
- Add Spark application examples using GraphAr Spark tools by @lixueclaire in https://github.com/apache/incubator-graphar/pull/61
### Changed

- Use the apache URL to download apache-arrow. by @sighingnow in https://github.com/apache/incubator-graphar/pull/7
- Update gar-test submodule url by @acezen in https://github.com/apache/incubator-graphar/pull/6
- Update README.rst by @yecol in https://github.com/apache/incubator-graphar/pull/11
- Revise image links in docs by @lixueclaire in https://github.com/apache/incubator-graphar/pull/10
- Refine documentation about integrating into GraphScope by @lixueclaire in https://github.com/apache/incubator-graphar/pull/15
- Refine the contributing doc to more readable and easy to get started by @acezen in https://github.com/apache/incubator-graphar/pull/16
- [Minor] Remove `docutils` version limit to fix docs ci by @acezen in https://github.com/apache/incubator-graphar/pull/57
- Remove `include "arrow/api.h" from graph.h by @acezen in https://github.com/apache/incubator-graphar/pull/50
- [Improve][Doc] Revise the README and APIs docstring of GraphAr by @acezen in https://github.com/apache/incubator-graphar/pull/64
- [Improve][Doc] Refine the documentation about user guide and applications by @lixueclaire in https://github.com/apache/incubator-graphar/pull/69

### Fixed

- Fix the inconsistent prefix for vertex property chunks and update image links by @acezen in https://github.com/apache/incubator-graphar/pull/4
- Fix the file suffix of bug report template by @acezen in https://github.com/apache/incubator-graphar/pull/17
- Fix prefix of GAR files in document by @lixueclaire in https://github.com/apache/incubator-graphar/pull/56
- [BugFix][Spark] Fix offset chunk output path and offset value of spark writer by @acezen in https://github.com/apache/incubator-graphar/pull/63
- [MinorFix] Remove unnecessary file by @acezen in https://github.com/apache/incubator-graphar/pull/43
- [BugFix] Hide the interface of dependencies of GraphAr with  `PRIVATE` link type by @acezen in https://github.com/apache/incubator-graphar/pull/71
