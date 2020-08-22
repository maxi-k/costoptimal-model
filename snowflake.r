source("./aws.r")
source("./model.r")
source("./.pgconnect.r")

con <- snowset.db.connect()

snowset.q <- function(str, ...) {
  dbGetQuery(con, sprintf(str, ...))
}

snowset.warehouse.sample <- function() {
  ## Requires postgres 9.5 +
  ## SELECT * FROM c TABLESAMPLE SYSTEM (0.1) LIMIT 1
  snowset.q("
SELECT * FROM snowset
WHERE persistentReadBytesS3 >= warehouseSize * POWER(1024, 3)
  AND intDataReadBytesLocalSSD >= warehouseSize * POWER(1024, 3)
  AND intDataReadBytesS3 >= warehouseSize * POWER(1024, 3)
")
}

row <- snowset.warehouse.sample()
snowflake.instance <- dplyr::filter(aws.data.current, id == "c5d.2xlarge")

snowset.row.est.skew <- function(row) {
  .inst <- model.with.speeds(snowflake.instance)
  .scanned <- (row$persistentreadbytescache + row$persistentreadbytess3) / row$warehousesize / 1024^3
  .tail <- row$persistentreadbytess3 / row$warehousesize / 1024^3

  .bins <- list(
    data.mem = list(size = round(.inst$calc.mem.caching), prio = .inst$calc.mem.speed),
    data.sto = list(size = round(.inst$calc.sto.caching), prio = .inst$calc.sto.speed),
    data.s3  = list(size = .scanned, prio = .inst$calc.net.speed)
  )
  .skew <- 0.99999
  .error <- 1
  # TODO: fix error update condition
  while(.error > 0.1) {
    dist.est <- model.make.distr.fn(.skew)(round(.scanned))
    pack <- model.distr.pack(.bins, dist.est) %>% as.data.frame()
    print(pack)
    if (is.null(pack$data.s3)) {
      break;
    }
    err.abs <- pack$data.s3 - .tail
    print(err.abs)
    .skew <- .skew - err.abs / .scanned / 10
    .error <- abs(err.abs / .scanned)
    print(c(err.abs, .error, .skew))
  }
  .skew
}

relevant <- dplyr::filter(row, persistentreadbytess3 + persistentreadbytescache > warehousesize * (300 * 1024^3))
print(head(relevant, n = 1)$persistentreadbytess3 / 1024^3)
snowset.row.est.skew(head(relevant, n = 1))

snowset.row.to.model <- function(row) {


}
