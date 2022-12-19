# AzureKusto 1.1.0

* New function `export()` to export query results to Azure Storage.
* `get_kusto_token()` defaults to `auth_type = 'authorization_code'` for ease of
use.
* `server` argument of `kusto_database_endpoint` can be just cluster name and
  no longer needs to be the fully qualified URI.

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
