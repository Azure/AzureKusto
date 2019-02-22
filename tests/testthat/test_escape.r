context("KQL: escaping")

# Identifiers ------------------------------------------------------------------

ei <- function(...) unclass(escape(ident(c(...))))

test_that("identifiers are bracket-escaped", {
  expect_equal(ei("x"), "['x']")
})

test_that("identifiers are comma separated", {
  expect_equal(ei("x", "y"), "['x'], ['y']")
})

# Zero-length inputs ------------------------------------------------------

test_that("zero length inputs yield zero length output when not collapsed", {
  expect_equal(kql_vector(kql(), collapse = NULL), kql())
  expect_equal(kql_vector(ident(), collapse = NULL), kql())
})

test_that("zero length inputs yield length-1 output when collapsed", {
  expect_equal(kql_vector(kql(), parens = FALSE, collapse = ""), kql(""))
  expect_equal(kql_vector(kql(), parens = TRUE, collapse = ""), kql("()"))
  expect_equal(kql_vector(ident(), parens = FALSE, collapse = ""), kql(""))
  expect_equal(kql_vector(ident(), parens = TRUE, collapse = ""), kql("()"))
})

# Special values ----------------------------------------------------------------

test_that("missing vaues become null", {
  expect_equal(escape(NA), kql("null"))
  expect_equal(escape(NA_real_), kql("real(null)"))
  expect_equal(escape(NA_integer_), kql("int(null)"))
  expect_equal(escape(NA_character_), kql("''"))
})

test_that("-Inf and Inf are expanded and quoted", {
  expect_equal(escape(Inf), kql("'real(+inf)'"))
  expect_equal(escape(-Inf), kql("'real(-inf)'"))
})

test_that("can escape logical values", {
  expect_equal(escape(TRUE), kql("true"))
  expect_equal(escape(FALSE), kql("false"))
  expect_equal(escape(NA), kql("null"))
})

test_that("can escape integer64 values", {
  skip_if_not_installed("bit64")

  expect_equal(escape(bit64::as.integer64(NA)), kql("long(null)"))
  expect_equal(escape(bit64::as.integer64("123456789123456789")), kql("123456789123456789"))
})

# Times -------------------------------------------------------------------

test_that("times are converted to ISO 8601", {
  x <- ISOdatetime(2000, 1, 2, 3, 4, 5, tz = "US/Central")
  expect_equal(escape(x), kql("'2000-01-02T09:04:05Z'"))
})
