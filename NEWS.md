# AzureKusto 1.1.3

* Fix test that broke on Linux due to upgrading the tzdata system package.

# AzureKusto 1.1.2

* Import S3 generics `tidyr::nest()` and `tidyr::unnest()`
* Fix S3 method consistency issues with r-devel

# AzureKusto 1.1.1

* Fixed bug in KQL translation of `%in%` operator so that it now works when the
  right hand side is either a vector or a tabular expression.

# AzureKusto 1.1.0

* New function `export()` to export query results to Azure Storage.
* `server` argument of `kusto_database_endpoint` can be just cluster name and no
  longer needs to be the fully qualified URI. E.g. can pass either
  `server = "help"` or `server = "https://help.kusto.windows.net"`
* Changed default `get_kusto_token()` auth_type from "device_code" to
  "authorization_code" for easier to use sign-in flow.
* Added `$` as an infix operator in a KQL expression now translates to `.` to
  enable nested dynamic field access.
* Added `slice_sample` dplyr verb

# AzureKusto 1.0.7

* Re-release to resolve "Version contains large components" note.

# AzureKusto 1.0.6.9001

* Regenerate .Rd files for R 4.2+ using updated Roxygen to fix HTML5 issues.

# AzureKusto 1.0.6.9000

* Switch to AAD v2 when obtaining OAuth tokens with `get_kusto_token`.

# AzureKusto 1.0.6

* Compatibility update for dplyr 1.0.0.

# AzureKusto 1.0.5

* Compatibility update for tidyselect 1.0.0.

# AzureKusto 1.0.4

* Default `queryconsistency` query setting changed to `strongconsistency`, which fixes query errors under certain cluster configurations.
* New maintainer (Alex Kyllo; jekyllo@microsoft.com).

# AzureKusto 1.0.3

* Compatibility update for tidyr 1.0.

# AzureKusto 1.0.2

* Implement `nest` and `unnest` verbs.

# AzureKusto 1.0.1

* Initial CRAN submission.
