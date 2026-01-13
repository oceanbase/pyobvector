# Release Notes

This file documents all released versions and their notable changes for the pyobvector project. Changes are grouped by version and categorized as Added (new features), Changed (modifications), Fixed (bug fixes), and Security (security updates).

## [0.2.21](https://github.com/oceanbase/pyobvector/compare/release-v0.2.20...release-v0.2.21) - 2026-01-13

- Migrate tool.poetry section to project section following PEP 518
- use [dependency-groups] as PEP 735 defined
- Feat: Add full-text index type support
- Fix typo in word partition
- Align required python versions and test on python version matrix

## [0.2.20](https://github.com/oceanbase/pyobvector/compare/release-v0.2.19...release-v0.2.20) - 2025-11-20

### Added

- Support seekdb sparse vector index

## [0.2.19](https://github.com/oceanbase/pyobvector/compare/release-v0.2.18...release-v0.2.19) - 2025-11-10

### Added

- Add SeekDB version check
- Docs: add hybrid search documentation

## [0.2.18](https://github.com/oceanbase/pyobvector/compare/release-v0.2.17...release-v0.2.18) - 2025-11-05

### Added

- Add the `HybridSearch` client that provides methods `search` and `get_stql`

## [0.2.17](https://github.com/oceanbase/pyobvector/compare/release-v0.2.16...release-v0.2.17) - 2025-11-03

### Added

- Add `output_columns` parameter to ann_search method
  - Support SQLAlchemy expressions for flexible column selection
  - Support column objects, expressions, JSON queries, and string functions
- Add `distance_threshold` parameter to ann_search method
  - Filter results by distance threshold in SQL WHERE clause
  - Only return results where distance <= threshold
- Support sparse vector
- Add additional param to create_table method
- Add sparse_vector_calculation & similarity_search_with_scalar_filter tests

### Changed

- Update sqlglot version
- Refactor: add base class ObClient and format code

### Fixed

- Fix docstring format and typos

### Security

- Bump aiomysql to 0.3.2 to resolve CVE-2025-62611

## [0.2.16](https://github.com/oceanbase/pyobvector/compare/release-v0.2.15...release-v0.2.16) - 2025-09-03

### Fixed

- Fix MilvusLikeClient does not support cosine metric type

## [0.2.15](https://github.com/oceanbase/pyobvector/compare/release-v0.2.14...release-v0.2.15) - 2025-08-18

### Fixed

- Update key regex of parser and update test cases
- Add type check for constraints spec to deal with string value

## [0.2.14](https://github.com/oceanbase/pyobvector/compare/release-v0.2.13...release-v0.2.14) - 2025-06-11

### Added

- Add `add_columns` and `drop_columns` method to `ObVecClient`
- Simplify array type instantiation and fix autocommit config

## [0.2.13](https://github.com/oceanbase/pyobvector/compare/release-v0.2.12...release-v0.2.13) - 2025-06-11

### Added

- Add `refresh_metadata` method to `ObVecClient`
- Validate array dimension

### Fixed

- Fix nested array for cache warnings and duplicate json process

## [0.2.12](https://github.com/oceanbase/pyobvector/compare/release-v0.2.11...release-v0.2.12) - 2025-06-09

### Added

- Add array type and override parsing method for array column
- Support TIMESTAMPTZ

## [0.2.11](https://github.com/oceanbase/pyobvector/compare/release-v0.2.10...release-v0.2.11) - 2025-04-19

### Changed

- Rename JSON tables

## [0.2.10](https://github.com/oceanbase/pyobvector/compare/release-v0.2.9...release-v0.2.10) - 2025-04-14

### Added

- JSON TABLE: support alias in order by clause

## [0.2.9](https://github.com/oceanbase/pyobvector/compare/release-v0.2.8...release-v0.2.9) - 2025-04-11

### Changed

- Update SQLAlchemy version constraint to `>=1.4,<=3`

## [0.2.8](https://github.com/oceanbase/pyobvector/compare/release-v0.2.7...release-v0.2.8) - 2025-04-10

### Added

- JSON table select with data id support

### Changed

- JSONTable: return row count for insert/update/delete operations
- Add `opt_user_id` parameter for `perform_json_table_sql` method

### Fixed

- Fix None where clause in update and delete operations

## [0.2.7](https://github.com/oceanbase/pyobvector/compare/release-v0.2.6...release-v0.2.7) - 2025-04-09

### Fixed

- Fix user & password encoding bug

## [0.2.6](https://github.com/oceanbase/pyobvector/compare/release-v0.2.5...release-v0.2.6) - 2025-04-08

### Added

- Support JSON table select with data id

## [0.2.5](https://github.com/oceanbase/pyobvector/compare/release-v0.2.4...release-v0.2.5) - 2025-04-07

### Added

- Support user group & multi users client for `ObVecJsonTableClient`
  - Support User Groups (admin_id user is the administrator in the user group)
  - Support Multi-Users Client (set user_id to None to enable Multi-Users mode)

## [0.2.4](https://github.com/oceanbase/pyobvector/compare/release-v0.2.3...release-v0.2.4) - 2025-03-27

### Changed

- Use Python 3.9 compatible union type hint

## [0.2.3](https://github.com/oceanbase/pyobvector/compare/release-v0.2.2...release-v0.2.3) - 2025-03-25

### Changed

- Downgrade numpy version (>=1.17.0,<2.0.0)

## [0.2.2](https://github.com/oceanbase/pyobvector/compare/release-v0.2.1...release-v0.2.2) - 2025-03-24

### Changed

- Downgrade pydantic version (>=2.7.0, <3.0.0)

## [0.2.1](https://github.com/oceanbase/pyobvector/compare/release-v0.2.0...release-v0.2.1) - 2025-03-24

### Fixed

- Fix MatchAgainst function compilation cache issue

## [0.2.0](https://github.com/oceanbase/pyobvector/compare/release-v0.1.20...release-v0.2.0) - 2025-03-23

### Added

- Full text search & More Vector Search Algorithm supporting

## [0.1.20](https://github.com/oceanbase/pyobvector/compare/release-v0.1.19...release-v0.1.20) - 2025-03-17

### Added

- Support online basic cases

## [0.1.19](https://github.com/oceanbase/pyobvector/compare/release-v0.1.18...release-v0.1.19) - 2025-01-02

### Added

- Support virtual SQL for JSON TABLE

## [0.1.18](https://github.com/oceanbase/pyobvector/compare/release-v0.1.17...release-v0.1.18) - 2024-12-20

### Fixed

- Fix Chinese-English-hybrid table name issue

## [0.1.17](https://github.com/oceanbase/pyobvector/compare/release-v0.1.16...release-v0.1.17) - 2024-12-05

### Fixed

- Fix SQLAlchemy metadata concurrency issue

## [0.1.16](https://github.com/oceanbase/pyobvector/compare/release-v0.1.15...release-v0.1.16) - 2024-12-03

### Changed

- Remove 'func.' prefix from vector distance function declaration
- Add docs for pure SQLAlchemy mode

## [0.1.15](https://github.com/oceanbase/pyobvector/compare/release-v0.1.14...release-v0.1.15) - 2024-12-02

### Changed

- Set SQLAlchemy version to >=1.4,<2.0.36 (for LangChain compatibility)

### Added

- Add downloads metric

## [0.1.14](https://github.com/oceanbase/pyobvector/compare/release-v0.1.13...release-v0.1.14) - 2024-11-20

### Fixed

- Escape @ to avoid parsing errors

### Changed

- Update ob_vec_client.py

## [0.1.13](https://github.com/oceanbase/pyobvector/compare/release-v0.1.12...release-v0.1.13) - 2024-11-12

### Fixed

- Fix embedchain unexpected keyword app_id

## [0.1.12](https://github.com/oceanbase/pyobvector/compare/release-v0.1.11...release-v0.1.12) - 2024-11-11

### Changed

- Bump version

### Fixed

- Fix Python 3.9 zip() issue

## [0.1.11](https://github.com/oceanbase/pyobvector/compare/release-v0.1.10...release-v0.1.11) - 2024-11-11

### Changed

- Show stmt str

## [0.1.10](https://github.com/oceanbase/pyobvector/compare/release-v0.1.9...release-v0.1.10) - 2024-11-08

### Added

- Support post ann_search

## [0.1.9](https://github.com/oceanbase/pyobvector/compare/release-v0.1.8...release-v0.1.9) - 2024-11-07

### Added

- Add `extra_output_cols` parameter
- Support GIS search
- Support GIS POINT DATATYPE & ST_GeomFromText func

### Fixed

- Add Tuple[float, float] type check

## [0.1.8](https://github.com/oceanbase/pyobvector/compare/release-v0.1.7...release-v0.1.8) - 2024-11-05

### Added

- Support GIS basically

## [0.1.7](https://github.com/oceanbase/pyobvector/compare/release-v0.1.6...release-v0.1.7) - 2024-10-30

### Added

- Support aiomysql basically
- Add aiomysql

### Fixed

- Fix schema metadata warning
- Fix all warnings

## [0.1.6](https://github.com/oceanbase/pyobvector/compare/release-v0.1.5...release-v0.1.6) - 2024-10-14

### Added

- ObVecClient::get support where clause

## [0.1.5](https://github.com/oceanbase/pyobvector/compare/release-v0.1.4...release-v0.1.5) - 2024-10-12

### Fixed

- Fix typo

## [0.1.4](https://github.com/oceanbase/pyobvector/compare/release-v0.1.3...release-v0.1.4) - 2024-10-09

### Changed

- Python version >=3.9,<4.0

## [0.1.3](https://github.com/oceanbase/pyobvector/compare/release-v0.1.2...release-v0.1.3) - 2024-10-09

### Changed

- Change Python & numpy version
- Use LONGTEXT

## [0.1.2](https://github.com/oceanbase/pyobvector/compare/release-v0.1.1...release-v0.1.2) - 2024-09-30

### Added

- Add negative_inner_product function

### Fixed

- Fix DataType.INT8 invalid

### Changed

- ARRAY is not supported in mysql dialect: Remove it from available Datatype

## [0.1.1](https://github.com/oceanbase/pyobvector/compare/release-v0.1.0...release-v0.1.1) - 2024-09-26

### Added

- Add arg:partition_names for ObVecClient.ann_search

### Changed

- Update README.md

## [0.1.0](https://github.com/oceanbase/pyobvector/releases/tag/release-v0.1.0) - 2024-09-25

### Added

- Initial release of pyobvector
- Add LICENSE
- Create python-publish.yml
- Add .gitignore & .pylintrc

### Changed

- Update README.md
- Change poetry src structure

### Fixed

- Fix upsert JSON column: no literal value renderer

