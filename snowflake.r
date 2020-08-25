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

system.time({
  sample.row <- snowset.warehouse.sample()
})

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

snowset.gen.for.model <- function(row = sample.row) {
  relevant <- row %>%
    dplyr::filter(
             scans3 != 0,
             scancache > (snowflake.instance$calc.sto.caching + snowflake.instance$calc.mem.caching) * 1024^3,
             scans3 + scancache > warehousesize * (300 * 1024^3)) %>%
    dplyr::arrange(scans3 + scancache, -(spools3 + spoolssd))

  testset <- head(relevant, n = 30)
  frames <- dplyr::transmute(testset,
                             wh.id = warehouseid,
                             cpu.hours = cpumicros / 10^6 / 60^2)

  workload <- frames %>%
    slider::slide2_dfr(testset, ., snowset.row.est.cache.skew) %>% # cache skew
    snowset.row.est.spool.frac(testset, .) %>%                     # spool frac
    slider::slide2_dfr(testset, ., snowset.row.est.spool.skew)     # spool skew

  workload %>%
    dplyr::filter(
             cache.skew.iter != 100,
             spool.skew.iter != 100)
}

plots.m3.time.cost.draw <- function(.row = sample.row) {
  .gen <-  snowset.gen.for.model(.row) %>%
    dplyr::filter(cache.skew.iter != 100, spool.skew.iter != 0)
  .wl <- .gen %>%
    head(n = 1) %>%
    dplyr::mutate(
             max.count = 128,
             scale.fact = 0.95,
             load.first = FALSE
           )
  .def <- model.gen.workload(.wl)
  .query <- .def$query
  .time.fn <- .def$time.fn

  .inst <- rbind(
    aws.data.current.large.relevant,
    aws.data.current %>% dplyr::filter(id == "c5d.2xlarge") %>% dplyr::mutate(id = "snowflake")
  )
  .inst.id <- c("c5d", "snowflake")

  .cost.all <- model.calc.costs(.query, .inst, .time.fn)
  .frontier <- model.calc.frontier(.cost.all,
                                   x = "stat.time.sum", y = "stat.price.sum",
                                   id = "id", quadrant = "bottom.left")
  .df <- .cost.all %>% dplyr::mutate(
                                id.prefix = sub("^([A-Za-z1-9-]+)\\..*", "\\1", id.name),
                                group = ifelse(id %in% .frontier$id | id.prefix %in% .inst.id, id.prefix, "other"))

  .groups <- unique(.df$group)
  .levels <- intersect(unique(.inst$id.prefix), .groups)
  .levels <- c(.levels, "snowflake", "other")

  .palette <- styles.color.palette1[1:length(.levels)]
  names(.palette) <- .levels
  .palette["other"] <- "#eeeeee"
  .palette <- rev(.palette)

  .colored <- .df %>% dplyr::filter(group != "other")
  .greys <- .df %>% dplyr::filter(group == "other")

  .labels <- .colored %>%
    dplyr::group_by(group) %>%
    dplyr::filter(stat.price.sum == max(stat.price.sum))

  ggplot(.df, aes(x = stat.time.sum, y = stat.price.sum, color = group, label = group)) +
    scale_color_manual(values = .palette, limits = .levels) +
    geom_point(data = .greys) +
    geom_point(data = .colored) +
    geom_text(data = .labels, nudge_x = -0.15, nudge_y = 0.08) +
    # geom_text(data = .colored, aes(label = count)) +
    scale_x_log10() +
    labs(y = "Workload Cost ($) [log]",
         x = "Workload Execution Time (s) [log]",
         color = "Instance")
  }


large.row <- snowset.q(
"
WITH wh AS(
     SELECT warehouseid
     FROM snowset
     WHERE persistentwritebytess3 = 0
     ORDER BY persistentreadbytess3 desc
     LIMIT 20
)
SELECT s.warehouseid,
       sum(systemCpuTime) + sum(userCpuTime) AS cpuMicros,
       sum(persistentReadBytesS3)            AS scanS3,
       sum(persistentReadBytesCache)         AS scanCache,
       sum(intDataReadBytesLocalSSD)         AS spoolSSD,
       sum(intDataReadBytesS3)               AS spoolS3,
       avg(warehousesize)                    AS warehousesize
FROM snowset s
JOIN wh ON wh.warehouseid = s.warehouseid
GROUP BY s.warehouseid
"
)


plots.m3.time.cost.draw(large.row)
