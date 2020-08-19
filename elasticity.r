source("./aws.r")
source("./model.r")
source("./plots.r")

model.modulate.workload <- function(.workload, .mod.fn, .duration, .window) {
  .num.windows <- floor(lubridate::seconds(.duration) / lubridate::seconds(.window))
  purrr::map_dfr(1:.num.windows, function(.window.id) {
    dplyr::mutate(.mod.fn(.workload, .window.id, .num.windows),
                  window.id = .window.id,
                  window.duration = .window,
                  window.seconds = lubridate::dseconds(.window)
                  )
  })
}

model.modulator.sin <- function(.w, .window, .num) {
  .factor <- (sin(.window / (2 * pi)) + 1)
  dplyr::mutate_all(.w, list(~ . * .factor / .num))
}

model.modulator.sin.other <- function(.w, .window, .num) {
  .freqs <- c(1, 1.5, 2, 3, 3.5, 4) * pi
  .factor <- max((sum(sin(.window / .freqs)) + 1) / 4, 0)
  dplyr::mutate(.w,
                time.cpu = time.cpu * .factor / .num,
                data.read = data.read * .factor / .num)
}

model.modulator.snowflake <- function(.df, .window, .num) {
  ## warehouse 4089697793760189952
  ## one day
  .multipliers <- c(
    0,  # 1
    0,  # 2
    0,  # 3
    20, # 4
    16, # 5
    28, # 6
    25, # 7
    50, # 8
    18, # 9
    22, # 10
    19, # 11
    4,  # 12
    5,  # 13
    6,  # 14
    4,  # 15
    5,  # 16
    4,  # 17
    3,  # 18
    2,  # 19
    0,  # 20
    0,  # 21
    0,  # 22
    0,  # 23
    0   # 24
  )
  .mult <- .multipliers[.window] / .num
  data.frame(
    time.cpu  = .df$time.cpu  * .mult,
    data.read = .df$data.read * .mult
  )
}

plot.modulated.workload <- function(.mod) {
  .df <- tidyr::pivot_longer(.mod, -window.id, names_to = "work.type", values_to = "value")
  ggplot(.df, aes(x = window.id, y = value, color = work.type)) + geom_line()
}

model.merge.window.costs <- function(.recom.cur, .recom.nxt) {
  dplyr::mutate(.recom.cur,  # TODO: implement better
                read.cache.load   = read.cache.load  + .recom.nxt$read.cache.load,
                read.cache.mem    = read.cache.mem   + .recom.nxt$read.cache.mem,
                read.cache.sto    = read.cache.sto   + .recom.nxt$read.cache.sto,
                read.cache.s3     = read.cache.s3    + .recom.nxt$read.cache.s3,

                read.spool.mem    = read.spool.mem   + .recom.nxt$read.spool.mem,
                read.spool.sto    = read.spool.sto   + .recom.nxt$read.spool.sto,
                read.spool.s3     = read.spool.s3    + .recom.nxt$read.spool.s3,

                rw.mem            = rw.mem           + .recom.nxt$rw.mem,
                rw.sto            = rw.sto           + .recom.nxt$rw.sto,
                rw.s3             = rw.s3            + .recom.nxt$rw.s3,
                rw.xchg           = rw.xchg          + .recom.nxt$rw.xchg,

                stat.read.spool   = stat.read.spool  + .recom.nxt$stat.read.spool,
                stat.read.work    = stat.read.work   + .recom.nxt$stat.read.work,

                time.cpu          = time.cpu         + .recom.nxt$time.cpu,
                time.mem          = time.mem         + .recom.nxt$time.mem,
                time.sto          = time.sto         + .recom.nxt$time.sto,
                time.s3           = time.s3          + .recom.nxt$time.s3,
                time.xchg         = time.xchg        + .recom.nxt$time.xchg,
                time.load         = time.load        + .recom.nxt$time.load,

                stat.time.sum     = stat.time.sum    + .recom.nxt$stat.time.sum,
                stat.time.max     = stat.time.max    + .recom.nxt$stat.time.max,
                stat.time.period  = stat.time.period + .recom.nxt$stat.time.period,
                stat.time.switch  = stat.time.switch,

                stat.price.sum    = stat.price.sum    + .recom.nxt$stat.price.sum,
                stat.price.max    = stat.price.max    + .recom.nxt$stat.price.max,
                stat.price.period = stat.price.period + .recom.nxt$stat.price.period,
                stat.price.switch = stat.price.switch,
                stat.price.stay   = stat.price.stay,

                window.start      = .recom.cur$window.start,
                window.end        = .recom.nxt$window.start,

                stat.elas.s3bw   = .recom.nxt$stat.elas.s3bw,
                stat.elas.effi   = .recom.nxt$stat.elas.effi,
                stat.elas.scan   = .recom.nxt$read.cache.mem + .recom.nxt$read.cache.sto
  )
}

model.make.distrs <- function(.skew, .count, .read) {
  .dfn <- model.make.distr.fn(.skew)
  purrr::map(1:.count, function(.n) {
    .read.frac <- .read / .n
    if (.read.frac < 1) {
      0
    } else {
      .dfn(round(.read.frac))
    }
  })
}

model.scale.down.distrs <- function(.distrs, .factor) {
  purrr::map(.distrs, function(.dist) {
    .dist * .factor
  })
}

## params <- list(
##   time.cpu    = 24 * 7 * 20,
##   data.read   = 24 * 7 * 1000,
##   cache.skew  = 0.1,
##   spool.frac  = 0.1,
##   spool.skew  = 0.8,
##   scale.eff   = 0.9,
##   max.inst    = 32,
##   window.size = duration(1, "hour"),
##   period.size = duration(1, "week")
##   max.latency = duration(1, "hour")
## )
model.elastic.costs <- function(.inst, .params, .mod.fn = model.modulator.sin.other) {
  stopifnot(nrow(.inst) > 0)
  .latency.sec <- int_length(.params$max.latency)
  .window.sec  <- int_length(.params$window.size)
  .n.windows <- int_length(.params$period.size) / .window.sec
  .wl <- data.frame(
    time.cpu  = .params$time.cpu * .n.windows,
    data.read = .params$data.read * .n.windows
  )

  .dist.cache.whole <- model.make.distrs(.params$cache.skew, .params$max.inst, .wl$data.read)
  .dist.spool.whole <- model.make.distrs(.params$spool.skew, .params$max.inst, .wl$data.read * .params$spool.frac)
  .dist.split.fn <- model.distr.split.fn(FALSE)
  .scale.eff.fn  <- model.make.scaling.fn(list(p = .params$scale.eff))

  make.window.timing.fn <- function(.w) {
    .cache.dist <- model.scale.down.distrs(.dist.cache.whole, .w$data.read / .wl$data.read)
    .spool.dist <- model.scale.down.distrs(.dist.spool.whole, .w$data.read / .wl$data.read)
    model.make.timing.fn(.cache.dist,
                         .spool.dist,
                         .params$max.inst,
                         .scale.eff.fn,
                         .dist.split.fn,
                         .window.sec)
  }

  costs.for.window <- function(.w, .max.latency = .latency.sec) {
    possible <- model.calc.costs(.w, .inst, make.window.timing.fn(.w)) %>%
      dplyr::filter(stat.time.sum <= .max.latency) %>%
      dplyr::arrange(stat.price.period) %>%
      dplyr::mutate(window.start = .w$window.id,
                    window.end = .w$window.id,
                    stat.time.switch = 0,
                    stat.price.stay = 0,
                    stat.price.switch = 0)
  }
  best <- function(.w) head(.w, n = 1)

  .mod <- model.modulate.workload(.wl, .mod.fn, .params$period.size, .params$window.size)
  .split <- split(.mod, f = 1:nrow(.mod))
  .init <- list(best(costs.for.window(.split[[1]])))
  .stats <- list("same best" = 0, "switch" = 0, "stay" = 0, "phony" = 0)
  addstat <- function(name) {
    .stats[[name]] <<- .stats[[name]] + 1
  }
  res <- purrr::reduce(.split[2:length(.split)], .init = .init, function(.acc, .cur) {
    .recom.prv <- .acc[[length(.acc)]]
    stopifnot(.recom.prv$window.end + 1 == .cur$window.id)
    .costs.cur <- costs.for.window(.cur)
    if(nrow(.costs.cur) == 0) {
      stop("no instance satisfies time constraint")
    }
    ## browser()
    if (.recom.prv$id == best(.costs.cur)$id) { # best stays the same
      addstat("same best")
      .acc[[length(.acc)]] <-
        dplyr::mutate(
                 model.merge.window.costs(.recom.prv, best(.costs.cur)),
                 window.start   = .recom.prv$window.start,
                 window.end     = .cur$window.id)
      return(.acc)
    } # best is different
    .time.switch <- (if (best(.costs.cur)$id.name == .recom.prv$id.name) {
                       .recom.prv$stat.elas.scan / .recom.prv$count
                     } else {
                       .recom.prv$stat.elas.scan
                     }) / (best(.costs.cur)$stat.elas.s3bw * best(.costs.cur)$count)
    .cost.switch <- .time.switch * best(.costs.cur)$cost.usdph / 3600
    .switch.candidate <- best(dplyr::filter(.costs.cur, stat.time.sum + .time.switch <= .window.sec))
    .can.switch <- nrow(.switch.candidate) > 0
    .can.stay <- .recom.prv$id %in% .costs.cur$id
    .cost.stay <- 0
    if(.can.stay) {
      .prv.incur <- .costs.cur %>% dplyr::filter(id == .recom.prv$id) %>% best()
      .cost.stay <- .prv.incur$stat.price.period - best(.costs.cur)$stat.price.period
      if (!.can.switch || .cost.stay < .cost.switch * (1 + .params$min.cost.frac)) {
        addstat("stay")
        .acc[[length(.acc)]] <- model.merge.window.costs(.recom.prv, .prv.incur) %>%
          dplyr::mutate(stat.price.stay = stat.price.stay + .cost.stay,
                        stat.price.switch = stat.price.switch + .cost.switch)
        return(.acc)
      }
    }
    if (!.can.stay && !.can.switch) {
      ## warning(...)
      stop("Can neither stay nor switch. stopping.")
      ## addstat("phony")
      ## .switch.candidate <- .costs.cur %>% dplyr::arrange(stat.time.sum, stat.price.sum) %>% best()
      ## browser()
    }
    addstat("switch")
    .acc[[length(.acc) + 1]] <- .switch.candidate %>%
      dplyr::mutate(stat.time.switch = .time.switch,
                    stat.price.stay  = .cost.stay,
                    stat.price.switch = .cost.switch)
    .acc
  }) %>% bind_rows()
  print(.stats)
  res
}

## --------------------------------------------------------------------------------
.params <- list(
  time.cpu    = 0.2,
  data.read   = 1000,
  cache.skew  = 0.4,
  spool.frac  = 0.2,
  spool.skew  = 0.1,
  scale.eff   = 0.9,
  max.inst    = 16,
  window.size = duration(1, "hour"),
  period.size = duration(1, "day"),
  max.latency = duration(1, "hour"),
  min.cost.frac = 0.3
)
.recoms <- model.elastic.costs(aws.data.current.relevant, .params, .mod.fn = model.modulator.snowflake)


.inst.c5n <- dplyr::filter(aws.data.current.relevant, str_detect(id, "c5n"))
.recoms.c5n <- model.elastic.costs(.inst.c5n, .params, .mod.fn = model.modulator.snowflake)

.params.snowflake <- .params
.params.snowflake[["max.latency"]] <- duration(1, "hour")
.params.snowflake[["scale.eff"]] <- 1
.inst.snowflake <- dplyr::filter(aws.data.current.relevant, id == "c5d.2xlarge")
.recoms.snowflake <- model.elastic.costs(.inst.snowflake, .params.snowflake, .mod.fn = model.modulator.snowflake)
## --------------------------------------------------------------------------------

plot.elastic.recoms <- function(.wl, .costs) {
  .maxval <- max(.wl$time.cpu)
  .split <- slider::slide_dfr(.costs, function(.recom) {
    .start <- .recom$window.start
    .end <- .recom$window.end
    .n <- .end - .start + 1
    purrr::map_dfr(1:.n, function(i) {
      .recom %>% dplyr::mutate(
                          window.start = .start + i - 1,
                          window.end = .start + i - 1,
                          window.start.orig = .start,
                          window.end.orig = .end,
                          angle = ifelse(window.start.orig == window.end.orig, 90, 90))
    })
  })
  .joined <- dplyr::inner_join(.wl, .split, by = c("window.id" = "window.start")) %>%
    dplyr::group_by(window.start.orig) %>%
    dplyr::mutate(
             id.prefix = sub("^([A-Za-z1-9-]+)\\..*", "\\1", id),
             id.short = str_replace(id.name, "large", "l"),
             time.cpu.window.max = max(time.cpu.x),
             time.cpu.window.avg = mean(time.cpu.x)
           ) %>%
    dplyr::ungroup()

  .joined.text <- .joined %>% dplyr::filter(window.start.orig == window.id) %>%
    dplyr::mutate(
             label = paste(count, "×", id.short),
             label.debug = paste(label, "(switch:", stat.price.switch, ", stay:", stat.price.stay, ")")
           )

  .labels.x <- as.character(c(.wl$window.id - 1, max(.wl$window.id)))
  .labels.x[1:length(.labels.x) %% 2 == 0] <- ""

  ggplot(.joined, aes(x = window.id, y = time.cpu.x, color = id.prefix)) +
    scale_color_manual(values = style.instance.colors.vibrant) +
    geom_line(color = "black") +
    geom_segment(aes(x = window.id - 0.5, xend = window.end + 0.5,
                     y = time.cpu.window.avg,
                     yend = time.cpu.window.avg),
                 size = 1.2,
                 alpha = 0.8) +
    geom_text(data = .joined.text, aes(y = time.cpu.window.avg, label = label,
                                       x = (window.end.orig + window.start.orig) / 2),
              angle = .joined.text$angle,
              size = 2.8,
              nudge_y = 0.05 * .maxval,
              hjust = 0
              ) +
    scale_x_continuous(
      breaks = c(.wl$window.id - 0.5, max(.wl$window.id) + 0.5),
      labels = .labels.x,
      minor_breaks = c()) +
    scale_y_continuous(limits = c(0, .maxval * 1.47)) +
    labs(
      x = "Hour",
      y = "Workload"
    ) +
    theme_bw() +
    theme(legend.position = "none")
}
## --------------------------------------------------------------------------------

.mod.workload <- model.modulate.workload(
    data.frame(
      time.cpu  = .params$time.cpu,
      data.read = .params$data.read
    ),
  model.modulator.snowflake, .params$period.size, .params$window.size
)
## plot.elastic.recoms(.mod.workload, .recoms)
ggsave(plots.mkpath("m4-time-workload-best.pdf"), plot.elastic.recoms(.mod.workload, .recoms),
       width = 3.6, height = 2.6, units = "in",
       device = cairo_pdf)

## plot.elastic.recoms(.mod.workload, .recoms.c5n)
ggsave(plots.mkpath("m4-time-workload-best-c5n.pdf"), plot.elastic.recoms(.mod.workload, .recoms.c5n),
       width = 3.6, height = 2.6, units = "in",
       device = cairo_pdf)

## ggsave(plots.mkpath("m4-time-workload-best-snowflake.pdf"), plot.elastic.recoms(.mod.workload, .recoms.snowflake),
##        width = 3.6, height = 2.6, units = "in",
##        device = cairo_pdf)

write.csv(.recoms, "data/elasticity.data.csv")
## ggsave("../figures/m5-elasticity-initial.pdf")

## plot.elastic.recoms(.mod.workload, .recoms.snowflake)

## --------------------------------------------------------------------------------

local({
  print("---------- Snowflake Diff -------------")
  ## print(paste("Snowflake cost sum: ", sum(.recoms.snowflake$stat.price.period)))
  print(paste("Elastical cost sum: ", sum(.recoms$stat.price.period)))
  print(paste("c5n cost sum: ", sum(.recoms.c5n$stat.price.period)))
  #
  ## print(paste("Snowflake time sum: ", sum(.recoms.snowflake$stat.time.sum)))
  print(paste("Elastical time sum: ", sum(.recoms$stat.time.sum)))
  print(paste("c5n time sum: ", sum(.recoms.c5n$stat.time.sum)))
})

local({
  .wl <- data.frame(
    time.cpu  = .params$time.cpu,
    data.read = .params$data.read
  )
  .dist.cache.whole <- model.make.distrs(.params$cache.skew, .params$max.inst, .params$data.read)
  .dist.spool.whole <- model.make.distrs(.params$cache.skew, .params$max.inst, .params$data.read * .params$spool.frac)
  .dist.split.fn <- model.distr.split.fn(FALSE)
  .scale.eff.fn  <- model.make.scaling.fn(list(p = .params$scale.eff))
  .time.fn <- model.make.timing.fn(.dist.cache.whole,
                                   .dist.spool.whole,
                                   .params$max.inst,
                                   .scale.eff.fn,
                                   .dist.split.fn,
                                   int_length(.params$period.size))
  .nwindows <- int_length(.params$period.size) / int_length(.params$window.size)
  .baseline.costs <- model.calc.costs(.wl, aws.data.current.relevant, .time.fn)
  .baseline.recom <- .baseline.costs %>%
    dplyr::filter(round(stat.time.sum) <= int_length(.params$max.latency) * .nwindows) %>%
    top_n(-1, wt = stat.price.sum) %>%
    head(n = 1)
  #
  print(paste("Baseline with", .baseline.recom$id, "costs", .baseline.recom$stat.price.period))
  print(paste("Split workload costs ", sum(.recoms$stat.price.period)))
  .price.diff <- .baseline.recom$stat.price.period - sum(.recoms$stat.price.period)
  print(paste("Difference: ", .price.diff))
})
