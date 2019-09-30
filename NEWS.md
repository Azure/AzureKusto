# AzureKusto 1.0.3.9000

* Change default query consistency setting to `strong`.
* Enable basic authentication (username & password) for signing in to an endpoint. This is not recommended, but necessary for some connections. To use basic auth, supply the `user` and `pwd` properties to `kusto_database_endpoint` and set the `.query_token` argument to `FALSE`.

# AzureKusto 1.0.3

* Compatibility update for tidyr 1.0.

# AzureKusto 1.0.2

* Implement `nest` and `unnest` verbs.

# AzureKusto 1.0.1

* Initial CRAN submission.
