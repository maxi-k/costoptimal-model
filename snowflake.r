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

snowflake.instance <- dplyr::filter(aws.data.current, id == "c5d.2xlarge") %>% model.with.speeds()

snowset.row.est.cache.skew <- function(row, df) {
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
  while(.error > 0.01 && .iter < 100 && .skew > 0) {
    dist.est <- model.make.distr.fn(.skew)(round(.scanned))
    pack <- model.distr.pack(.bins, dist.est) %>% as.data.frame()
    if (is.null(pack$data.s3)) {
      break;
    }
    err.abs <- pack$data.s3 - .tail
    .error <- abs(err.abs / .tail)
    .skew <- .skew + sign(err.abs) * min(0.1, .error / (.iter * 0.5))
    .iter <- .iter + 1
  }
  if (.iter >= 100) {
    print(c("Aborted after 100 iterations, skew might not be very accurate", .skew))
  }
  if (.skew <= 0) {
    print(c("Skew < 0, this is a weird row.", .skew))
  }
  dplyr::mutate(df,
                wh.id            = row$warehouseid,
                data.scan        = .scanned,
                cache.skew.tail  = .tail,
                cache.skew       = .skew,
                cache.skew.error = .error,
                cache.skew.iter  = .iter)
}

snowset.row.est.spool.frac <- function(row, df) {
  .scanned <- row$scans3 + row$scancache
  .spooled <- row$spools3 + row$spoolssd
  dplyr::mutate(df, spool.frac = .spooled / .scanned)
}

snowset.row.est.spool.skew <- function(row, df) {
  .inst <- model.with.speeds(snowflake.instance)
  .scanned <- df$spool.frac * df$data.scan
  .tail <- row$spools3 / row$warehousesize / 1024^3
  if(.scanned < 1) {
    return(
      dplyr::mutate(df,
                    wh.id            = row$warehouseid,
                    spool.skew.tail  = .tail,
                    spool.skew       = 0.0001,
                    spool.skew.error = 0,
                    spool.skew.iter  = 0)
    )
  }

  .bins <- list(
    data.mem = list(size = round(.inst$calc.mem.spooling), prio = .inst$calc.mem.speed),
    data.sto = list(size = round(.inst$calc.sto.spooling), prio = .inst$calc.sto.speed),
    data.s3  = list(size = .scanned, prio = .inst$calc.net.speed)
  )
  .skew <- 0.00001
  .error <- 1
  .iter <- 0
  while(.error > 0.01 && .iter < 100 && .skew > 0) {
    dist.est <- model.make.distr.fn(.skew)(round(.scanned))
    pack <- model.distr.pack(.bins, dist.est) %>% as.data.frame()
    if (is.null(pack$data.s3)) {
      break;
    }
    err.abs <- pack$data.s3 - .tail
    .error <- abs(err.abs / .tail)
    .skew <- .skew + sign(err.abs) * min(0.1, .error / (.iter * 0.5))
    .iter <- .iter + 1
  }
  if (.iter >= 100) {
    print(c("Aborted after 100 iterations, skew might not be very accurate", .skew))
  }
  if (.skew <= 0) {
    print(c("Skew < 0, this is a weird row.", .skew))
  }
  dplyr::mutate(df,
                wh.id            = row$warehouseid,
                spool.skew.tail  = .tail,
                spool.skew       = .skew,
                spool.skew.error = .error,
                spool.skew.iter  = .iter)
}

snowset.gen.for.model <- function() {
  relevant <- row %>%
    dplyr::filter(
             scans3 != 0,
             scancache > (snowflake.instance$calc.sto.caching + snowflake.instance$calc.mem.caching) * 1024^3,
             scans3 + scancache > warehousesize * (300 * 1024^3)) %>%
    dplyr::arrange(scans3 + scancache, -(spools3 + spoolssd))

  testset <- head(relevant, n = 30)
  frames <- dplyr::transmute(testset,
                             wh.id = warehouseid,
                             cpu.hours = cpuMicros / 10^6 / 60)

  workload <- frames %>%
    slider::slide2_dfr(testset, ., snowset.row.est.cache.skew) %>% # cache skew
    snowset.row.est.spool.frac(testset, .) %>%                     # spool frac
    slider::slide2_dfr(testset, ., snowset.row.est.spool.skew)     # spool skew
  workload
}

snowset.gen.for.model()
