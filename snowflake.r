source("./aws.r")
source("./model.r")
source("./.pgconnect.r")

con <- snowset.db.connect()

snowset.q <- function(str, ...) {
  dbGetQuery(con, sprintf(str, ...))
}

snowset.warehouse.random <- function(fraction = 0.01) {
  ## Requires postgres 9.5 +
  snowset.q("SELECT warehouseId FROM snowset TABLESAMPLE SYSTEM (%f)", fraction)
}

snowset.warehouse.sample <- function(fraction = 0.01) {
  ## persistentReadBytesS3 >= warehouseSize * POWER(1024, 3)
  ## intDataReadBytesLocalSSD >= warehouseSize * POWER(1024, 3)
  ## intDataReadBytesS3 >= warehouseSize * POWER(1024, 3)
  snowset.q("
SELECT s.warehouseId,
       sum(systemCpuTime) + sum(userCpuTime) AS cpuMicros,
       sum(persistentReadBytesS3)            AS scanS3,
       sum(persistentReadBytesCache)         AS scanCache,
       sum(intDataReadBytesLocalSSD)         AS spoolSSD,
       sum(intDataReadBytesS3)               AS spoolS3,
       avg(warehousesize)                    AS warehousesize
  FROM snowset s
  JOIN (SELECT warehouseId FROM snowset TABLESAMPLE SYSTEM (%f)) w ON w.warehouseId = s.warehouseId
 WHERE s.warehouseSize = 1
 GROUP BY s.warehouseId
", fraction)
}

row <- snowset.warehouse.sample()

snowflake.instance <- dplyr::filter(aws.data.current, id == "c5d.2xlarge")

snowset.row.est.skew <- function(row) {
  .inst <- model.with.speeds(snowflake.instance)
  .scanned <- (row$scans3 + row$scancache) / row$warehousesize / 1024^3
  .tail <- row$scans3 / row$warehousesize / 1024^3

  .bins <- list(
    data.mem = list(size = round(.inst$calc.mem.caching), prio = .inst$calc.mem.speed),
    data.sto = list(size = round(.inst$calc.sto.caching), prio = .inst$calc.sto.speed),
    data.s3  = list(size = .scanned, prio = .inst$calc.net.speed)
  )
  .skew <- 0.00001
  .error <- 1
  .iter <- 0
  # TODO: fix error update condition
  while(.error > 0.01 && .iter < 100 && .skew > 0) {
    dist.est <- model.make.distr.fn(.skew)(round(.scanned))
    pack <- model.distr.pack(.bins, dist.est) %>% as.data.frame()
    if (is.null(pack$data.s3)) {
      break;
    }
    err.abs <- pack$data.s3 - .tail
    .error <- abs(err.abs / .tail)
    .skew <- .skew + sign(err.abs) * min(0.1, .error)
    .iter <- .iter + 1
  }
  if (.iter >= 100) {
    warning("Aborted after 100 iterations, skew might not be very accurate")
    print(.skew)
  }
  if (.skew >= 0) {
    warning("Skew < 0, this is a weird row.")
    print(.skew)
  }
  data.frame(
    cache.skew = .skew
  )
}

relevant <- dplyr::filter(row, scans3 + scancache > warehousesize * (300 * 1024^3))
print(head(relevant, n = 1)$scans3 / 1024^3)

cache.skews <- slider::slide_dfr(relevant, snowset.row.est.skew)

snowset.row.to.model <- function(row) {


}
