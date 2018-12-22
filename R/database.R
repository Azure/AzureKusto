#' @export
ade_database_endpoint <- function(object, ...)
{
    UseMethod("ade_database_endpoint")
}


#' @export
ade_database_endpoint.ade_cluster_endpoint <- function(object, database, ...)
{
    out <- list(db=database, cluster=object)
    class(out) <- "ade_database_endpoint"
    out
}


#' @export
ade_database_endpoint.ade_database_resource <- function(object, ...)
{
    object$get_database_endpoint(...)
}
