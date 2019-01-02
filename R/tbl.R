#' @export
tbl <- function(object, ...)
{
    UseMethod("tbl")
}

tbl_ade <- function(object, table, ...)
{
    table <- list(x = table, vars = names(table))
    ops <- list()
    dplyr::make_tbl("ade", db = object, table = table, ops = ops)
}

#' Create a dummy tbl from a data frame.
#' Useful for testing KQL generation without a remote connection.
tbl.tbl_df <- function(table, object=simulate_ade())
{
    tbl_ade(object, table)
}

#' @export
tbl.ade_database_endpoint <- function(object, table, ...)
{
    tbl_ade(object, table)
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
