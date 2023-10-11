# CRAN Comments for AzureKusto

## R CMD check results

There were no ERRORs, WARNINGs, or NOTEs.

Previously submitted on October 4th, 2023. The following review comments were
provided:

> We see:
>      Warning: Unexecutable code in man/DBI_query.Rd:
>            server="https//mycluster.location.kusto.windows.net",
> database="database"...:
>      Warning: Unexecutable code in man/DBI_table.Rd:
>            server="https//mycluster.location.kusto.windows.net",
> database="database"...:
> --> Please check that
> 
> You have examples for unexported functions. Please either omit these examples or export these functions.
> Examples for unexported function
>    base_window() in:
>       create_kusto_cluster.Rd
>    compute.tbl_kusto() in:
>       delete_kusto_cluster.Rd
>    create_kusto_cluster() in:
>       az_kusto.Rd
>       az_kusto_database.Rd
>       get_kusto_cluster.Rd
> 
> \dontrun{} should only be used if the example really cannot be executed (e.g.
> because of missing additional software, missing API keys, ...) by the user.
> That's why wrapping examples in \dontrun{} adds the comment ("# Not run:") as a
> warning for the user. Does not seem necessary for all. Please unwrap the
> examples if they are executable in < 5 sec, or replace \dontrun{} with
> \donttest{}.
> 
> Please ensure that your functions do not write by default or in your
> examples/vignettes/tests in the user's home filespace (including the package
> directory and getwd()). This is not allowed by CRAN policies.
>
> Please omit any default path in writing functions. In your
> examples/vignettes/tests you can write to tempdir().

My responses:

The examples marked \dontrun{} in DBI_query.Rd and DBI_table.Rd are marked so
because they really cannot be executed. The strings
"https://mycluster.location.kusto.windows.net" and "database" are placeholders;
the user needs to provide a real connection string and account credentials to
their own database, which is a cloud resource they must first provision in their
Azure subscription.

Per lines 6 and 102 in the NAMESPACE file, it appears to me that
compute.tbl_kusto() and base_window() are already exported. I do not understand
why they are being reported as unexported functions. create_kusto_cluster is a
method added to extend the AzureRMR::az_resource_group R6 class.

I was not able to locate any instances of this package writing files in the
user's home file system.
