ade_database <- function(object, ...)
{
    UseMethod("ade_database")
}


ade_database.ade_cluster <- function(object, database, ...)
{
    out <- list(db=database, cluster=object)
    class(out) <- "ade_database"
    out
}



