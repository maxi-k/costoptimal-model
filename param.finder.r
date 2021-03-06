source("./model.r")
source("./aws.r")

try.params <- function(.insts = aws.data.current.large.relevant, by = stat.price.sum) {

  .range.scan  <- 16*2^(2:11) # ?
  .range.cache <- c(0.001)
  .range.sdist <- c(0.001)
  .range.spool <- seq(0, 1, by = 0.1)
  .range.cpu   <- c(1)
  .range.split <- c(FALSE)

  .inst.meta <- .insts %>% dplyr::select(id, starts_with("meta."))
  by_var = enquo(by)

  purrr::map_dfr(.range.scan, function(.scanned) {
    purrr::map_dfr(.range.cache, function(.cache.skew) {
      .cache.distr <- model.make.distr.fn(.cache.skew)(round(.scanned))
      .cache.distr.list <- list(.cache.distr)
      purrr::map_dfr(.range.sdist, function(.spool.skew) {
        purrr::map_dfr(.range.spool, function(.spool.frac) {
          .spooled <- round(.spool.frac * .scanned)
          .spool.distr <- if(.spooled < 1) { 0 } else { model.make.distr.fn(.spool.skew)(.spooled) }
          .spool.distr.list <- list(.spool.distr)
          purrr::map_dfr(.range.cpu, function(.cpuh) {
            purrr::map_dfr(.range.split, function(.do.split) {
              .split.fn <-
                .query <- data.frame(
                  time.cpu  = .cpuh,
                  data.read = .scanned
                )
              .timer <- model.make.timing.fn(
                .distr.list.caching  = .cache.distr.list,
                .distr.list.spooling = .spool.distr.list,
                .max.count <- 1,
                .distr.caching.split.fn = model.distr.split.fn(.do.split)
              )
              .times <- model.calc.costs(.query, .insts, timing.fn = .timer)
              .recom <- .times %>%
                dplyr::mutate(rank = rank(!! by_var)) %>%
                dplyr::arrange(rank) %>%
                dplyr::ungroup()
              dplyr::mutate(.recom,
                            param.cpuh = .cpuh,
                            param.scanned = .scanned,
                            param.cache.skew = .cache.skew,
                            param.spool.skew = .spool.skew,
                            param.spool.frac = .spool.frac,
                            param.do.split   = .do.split
                            )
            })
          })
        })
      })
    })
  }) %>% dplyr::inner_join(.inst.meta, by = c("id.name" = "id"))
}

##  system.time({
##    tested.params <- try.params(aws.data.current.large.relevant %>% aws.data.filter.spot.price.inter.freq())
##    write.csv(tested.params, "data/tested.params.spot.freq<5.csv")
##  })
