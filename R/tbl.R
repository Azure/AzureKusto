#' @export
tbl <- function(object, ...)
{
    UseMethod("tbl")
}

#' Create a dummy tbl from a data frame.
#' Useful for testing KQL generation without a remote connection.
tbl.tbl_df <- function(table, name, object=simulate_ade())
{
    vars = names(table)
    ops = list()
    dplyr::make_tbl(
               "ade",
               db = object,
               table = table,
               vars = vars,
               ops = ops,
               name = name
           )
}

#' @export
tbl.ade_database_endpoint <- function(object, table, ...)
{
    # TODO Not Implemented
}

simulate_ade <- function()
{
  structure(
      list(
          db = "local_df",
          cluster = "local_df"
      ),
    class = "ade_database_endpoint"
  )
}
