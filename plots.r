source("./model.r")
source("./aws.r")
source("./param.finder.r")

plots.inst <- aws.data.paper
plots.path <- "../figures"

plots.mkpath <- function(filename) {
  paste(plots.path, filename, sep="/")
}

## ---------------------------------------------------------------------------------------------- ##
                                        # M1: Basic Model #
## ---------------------------------------------------------------------------------------------- ##

plots.m1.timing.fn <- function(.query, .inst) {
  dplyr::mutate(.inst,
                time.cpu.s = .query$time.cpu  * 3600 / calc.cpu.real,
                time.net.s = .query$data.read / calc.net.speed,
                stat.time.sum = time.cpu.s + time.net.s
                )
}

plots.m1.draw <- function() {
  .n.points <- 10
  .query <- data.frame(
    time.cpu  = seq(0, 200, length.out = .n.points),
    data.read =  rep(10000, .n.points)
  )
  .costs <- model.calc.costs(.query, plots.inst, plots.m1.timing.fn)
  .recom <- model.recommend.from.timings.arr(.query, .costs)
  .df <- .recom %>%
    dplyr::transmute(
             x = .query$time.cpu,
             y = stat.price.sum,
             is.flank = x == 0 | id != dplyr::lag(id),
             color = ifelse(is.flank, "blue", "black"),
             label = str_replace(id, "xlarge", "")
           )
  .df.flanks <- .df %>% dplyr::filter(is.flank)
  ggplot(.df, aes(x = x, y = y)) +
    geom_line(color = "black") +
    geom_point(color = .df$color) +
    # geom_vline(data = .df.flanks, aes(xintercept=x)) +
    geom_text(aes(label = label), nudge_x = 17, nudge_y = 0.03, size = 2.2) +
    theme_bw() +
    theme(text = element_text(size = 7), plot.margin=grid::unit(c(0,0,0,0), "mm")) +
    labs(x = "CPUh", y = "Workload Cost ($)")
}

# ggsave(plots.mkpath("m1-cost-cpu.pdf"), plots.m1.draw(),
#        width = 3, height = 2, units = "in",
#        device = cairo_pdf)

                                        # - 40cpuh
                                        # - remove cross over points, add manually

plots.m1.all.draw <- function() {
  .n.points <- 100
  .value.range <- seq(10, 60, length.out = .n.points)
  .xlim <- tail(.value.range, n = 1)
  .query <- data.frame(
    time.cpu  = .value.range,
    data.read = 3000
  )
  .ids <- c("c5n", "c5", "c5d", "m5dn", "i3en", "other")

  .inst <- aws.data.current.large.relevant

  palette <- style.instance.colors
  names(palette) <- .ids
  palette["other"] <- "#cccccc"

  .costs <- model.calc.costs(.query, .inst, plots.m1.timing.fn)
  .df <- dplyr::group_modify(.costs, function(group, qid) {
    dplyr::transmute(group,
                     id = as.character(id),
                     x = .query$time.cpu[qid$queryIndex],
                     y = stat.price.sum,
                     label = sub("^([A-Za-z1-9-]+)\\..*", "\\1", id),
                     is.best = y == min(y),
                     rank = rank(y),
                     color = ifelse(label %in% .ids, label, "other"))
  }) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(is.best, color) %>%
    dplyr::group_by(label) %>%
    dplyr::mutate(is.flank = rank != dplyr::lag(rank))

  .flanks <- .df %>% dplyr::filter(is.flank, is.best) %>% dplyr::ungroup()
  .transitions <- dplyr::filter(.flanks, x != min(x))
  .labeled <- .df %>%
    dplyr::filter(!any(is.best)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(color != "other", x == max(x))

  ggplot(.df, aes(x = x, y = y, group = label, color = color)) +
    scale_color_manual(values=palette) +
    geom_line(data = .df %>% dplyr::filter(color == "other")) +
    geom_line(data = .df %>% dplyr::filter(color != "other")) +
    theme_bw() +
    geom_point(data = .transitions, shape = 4, color = "black") +
    geom_vline(data = .transitions, aes(xintercept=x), color = "black", linetype = "dashed", size = 0.2) +
    geom_text(data = .flanks, aes(y = min(y) / 2, x = x + 0.35 * max(x), label = paste(label, "best"))) +
    geom_text(data = .labeled, aes(x = max(x) + 1, label = label),
              hjust = 0) +
    theme(plot.margin=grid::unit(c(1,1,1,1), "mm"),
          legend.position = "none") +
    coord_cartesian(xlim = c(10, 70), ylim = c(0, 15)) +
    labs(x = "CPU Hours", y = "Workload Cost ($) [log]")
}

# plots.m1.all.draw()
ggsave(plots.mkpath("m1-cost-cpu-all.pdf"), plots.m1.all.draw(),
       width = 3.6, height = 2.5, units = "in",
       device = cairo_pdf)


## ---------------------------------------------------------------------------------------------- ##
                                        # M2: Caching #
## ---------------------------------------------------------------------------------------------- ##

## Config where changing only access distribution results in c5n -> c5d -> c5
## http://127.0.0.1:3030/?_inputs_&instances.slicing_search=%22%22&instances.times_cell_clicked=%7B%7D&timings.plot.budget.ticks.at.limits=true&timings.plot.budget.step.digits=3&instances.rws.gib_rows_current=%5B1%2C2%2C3%5D&instance=%22c5.24xlarge%22&instances.specs.derived_rows_all=%5B1%2C2%5D&instances.times.perc_rows_all=%5B1%2C2%2C3%5D&instances.slicing_rows_current=%5B1%2C2%5D&instances.slicing_cell_clicked=%7B%7D&instances.times_rows_all=%5B1%2C2%2C3%5D&instances.prices_rows_all=%5B1%2C2%2C3%5D&time.cpu=12&instances.plot.frontier.quadrant=%22top.left%22&instances.metadata_search=%22%22&instances.specs_rows_current=%5B1%2C2%5D&instances.reads.gib_state=null&instances.prices_search=%22%22&instances.times.perc_state=null&instances.reads.gib_rows_all=%5B1%2C2%2C3%5D&instFilter.details.show=0&config.options.show=1&instances.plot.display.frontier.only=false&time.period.num=1&instances.plot.x=%22stat.time.sum%22&instances.slicing_rows_selected=null&plotly_hover-A=null&instances.rws.gib_state=null&instances.specs.derived_cell_clicked=%7B%7D&timings.plot.budget.step=100&timings.plot.budget.duplicates.filter=true&timings.plot.budget.limits.logarithmic=true&scaling.efficiency.param.%20p=0.98&instances.plot.scale.y=%22Linear%22&.clientValue-default-plotlyCrosstalkOpts=%7B%22on%22%3A%22plotly_click%22%2C%22persistent%22%3Afalse%2C%22dynamic%22%3Afalse%2C%22selectize%22%3Afalse%2C%22opacityDim%22%3A0.2%2C%22selected%22%3A%7B%22opacity%22%3A1%7D%2C%22debounce%22%3A0%2C%22color%22%3A%5B%5D%7D&instances.specs.derived_rows_current=%5B1%2C2%5D&instances.specs_rows_selected=null&timings.plot.budget.col.cost=%22stat.price.sum%22&instances.specs_cell_clicked=%7B%7D&instances.rws.gib_search=%22%22&instances.metadata_rows_selected=null&instances.metadata_rows_current=%5B1%2C2%5D&instances.metadata_state=null&instances.prices_cell_clicked=%7B%7D&user.notes=%22%22&time.period.unit=%22Weeks%22&timings.plot.budget.col.optim=%22stat.time.sum%22&instanceSet=%222019-11-30%20%7C%20101%20%7C%20website%22&timings.plot.budget.limits.display=true&instances.specs.derived_search=%22%22&instances.specs_state=null&instances.rws.gib_rows_all=%5B1%2C2%2C3%5D&instances.times_rows_selected=null&distr.caching.load.first=false&instance.count.max=1&instances.specs_rows_all=%5B1%2C2%5D&instances.reads.gib_rows_current=%5B1%2C2%2C3%5D&locality=0.96&instances.specs_search=%22%22&instSet.details.show=0&instances.reads.gib_rows_selected=null&instanceFilter=%22Paper%20Table%201%22&instances.times_rows_current=%5B1%2C2%2C3%5D&plotly_relayout-A=%22%7B%5C%22width%5C%22%3A1242%2C%5C%22height%5C%22%3A400%7D%22&spooling.fraction=0&tables.frontier.show=false&instances.reads.gib_search=%22%22&instances.plot.display.frontier=true&instances.metadata_cell_clicked=%7B%7D&instances.times_search=%22%22&data.read=1798&instances.times.perc_rows_selected=null&instance.type.opt.include=%22frontier%22&time.cpu.maxval=1000&instances.slicing_state=null&instances.prices_state=null&spooling.shape=0.1&data.read.maxval=10000&instances.times_state=null&instances.prices_rows_current=%5B1%2C2%2C3%5D&instances.reads.gib_cell_clicked=%7B%7D&instances.rws.gib_cell_clicked=%7B%7D&recommendationColumn=%22stat.price.sum%22&instances.specs.derived_rows_selected=null&plotly_afterplot-A=%22%5C%22distr.spooling.recommended.plot%5C%22%22&instances.specs.derived_state=null&instances.rws.gib_rows_selected=null&instances.metadata_rows_all=%5B1%2C2%5D&instances.times.perc_cell_clicked=%7B%7D&instances.prices_rows_selected=null&instances.plot.y=%22col.recom.inv%22&instances.plot.scale.x=%22Linear%22&instances.times.perc_search=%22%22&comparison.count=1&instances.times.perc_rows_current=%5B1%2C2%2C3%5D&instances.slicing_rows_all=%5B1%2C2%5D

plots.m2.cost.draw <- function() {
  .read <- 1800
  .query <- data.frame(
    time.cpu  = 12,
    data.read =  1800
  )
  .shapes <- c(0.001, seq(0.1, 0.9, 0.1), 0.999)
  .cost.list <- purrr::map_dfr(.shapes, function(.shape) {
    .distr <- model.make.distr.fn(.shape)(.read)
    .time.fn <- model.make.timing.fn(
      .distr.list.caching = list(.distr),
      .distr.list.spooling = list(0),
      .max.count = 1,
      .distr.caching.split.fn = model.distr.split.fn(FALSE)
    )
    .costs <- model.calc.costs(.query, plots.inst, .time.fn)
    .recom <- model.recommend.from.timings.arr(.query, .costs)
    head(.recom, n = 1)
  })
  .df <- .cost.list %>%
    dplyr::transmute(
             x = .shapes,
             y = stat.price.sum,
             is.flank = x == .shapes[1] | id != dplyr::lag(id),
             color = ifelse(is.flank, "blue", "black"),
             label = str_replace(id.name, "xlarge", "")
           )
  .df.flanks <- .df %>% dplyr::filter(is.flank)
  ggplot(.df, aes(x = x, y = y)) +
    geom_line(color = "black") +
    geom_point(color = .df$color) +
    geom_text(aes(label = label), nudge_x = 0.06, nudge_y = 0.015, size = 2.0, angle = 30) +
    theme_bw() +
    theme(plot.margin=grid::unit(c(1,1,1,1), "mm")) +
    labs(x = "Locality Distribution Factor", y = "Workload Cost ($)")
}

# plots.m2.cost.draw()

# ggsave(plots.mkpath("m2-cost-zipf.pdf"), plots.m2.cost.draw(),
#        width = 3, height = 2, units = "in",
#        device = cairo_pdf)

plots.m2.distr.draw <- function() {
  .id <- "z1d.12xlarge"
  .id.short <- str_replace(.id, "xlarge", "")
  instance <- plots.inst %>% dplyr::filter(id == .id) %>% model.with.speeds()
  .read = 2000
  .shape1 = 0.1
  .shape2 = 0.4

  data <- rbind(
    data.frame(y = model.make.distr.fn(.shape1)(.read), x = 1:.read, dist = .shape1),
    data.frame(y = model.make.distr.fn(.shape2)(.read), x = 1:.read, dist = .shape2)
  ) # %>% dplyr::filter(x > 5) %>% dplyr::mutate(x = x - 5)
  if (instance$calc.net.speed >= instance$calc.sto.speed) {
    data$fill = if_else(data$x <= instance$calc.mem.caching, "Memory", "S3")
  } else {
    data$fill = if_else(data$x <= instance$calc.mem.caching, "Memory",
                        if_else(data$x <= instance$calc.mem.caching + instance$calc.sto.caching, "SSD", "S3"))
  }
  nudge.x <- 0.03 * max(c(max(instance$calc.mem.caching + instance$calc.sto.spooling), nrow(data)))
  nudge.y <- max(data$y) / 4
  plot <- ggplot(instance) +
    scale_fill_manual(values=styles.color.palette1) +
    geom_area(data=data, aes(y = y, x = x, fill = fill), stat="identity") +
    geom_vline(aes(xintercept=calc.mem.caching), colour="blue") +
    geom_vline(aes(xintercept=calc.mem.caching + calc.sto.spooling), colour="blue") +
    geom_text(aes(x=calc.mem.caching, y=nudge.y * 2, label=paste(.id.short, "Memory")), colour="blue", angle=90,
              nudge_x = nudge.x, size = 2) +
    geom_text(aes(x=calc.mem.caching + calc.sto.caching, y=nudge.y * 2, label=paste(.id.short, "Storage")), colour="blue", angle=90,
              nudge_x = nudge.x, size = 2) +
    labs(x = "GiB in Workload", y = "Number of Accesses", fill = "Hierarchy") +
    theme_bw() +
    theme(text = element_text(size = 7), plot.margin = grid::unit(c(0.5, 0, 0, 0), "mm"),
          legend.title = element_blank(), legend.text = element_text(size = 7),
          legend.position = "bottom", legend.key.size = unit(0.5, "lines"),
          legend.margin = margin(0, 0, 0, 0, "cm")) +
    facet_grid(rows = vars(dist))
  plot
}

## plots.m2.distr.draw()

# ggsave(plots.mkpath("m2-distr-zipf.pdf"), plots.m2.distr.draw(),
#        width = 3, height = 2, units = "in",
#        device = cairo_pdf)


## ---------------------------------------------------------------------------------------------- ##
                                        # M3: Materialization #
## ---------------------------------------------------------------------------------------------- ##

## Config where chaning the materialization fraction changes from c5 -> c5d -> c5n
## http://127.0.0.1:3030/?_inputs_&instance.count.max=32&locality=0.8&timings.plot.budget.ticks.at.limits=true&instSet.details.show=0&instanceFilter=%22Paper%20Table%201%22&timings.plot.budget.step.digits=3&instance=%22%22&spooling.fraction=0.4&tables.frontier.show=false&plotly_relayout-A=%22%7B%5C%22width%5C%22%3A1242%2C%5C%22height%5C%22%3A9220%7D%22&instances.plot.display.frontier=true&time.cpu=20&instances.plot.frontier.quadrant=%22top.left%22&data.read=1998&instance.type.opt.include=%22frontier%22&instFilter.details.show=0&config.options.show=0&instances.plot.display.frontier.only=false&spooling.shape=0.1&instances.plot.x=%22stat.time.sum%22&time.period.num=1&plotly_hover-A=%22%5B%7B%5C%22curveNumber%5C%22%3A0%2C%5C%22pointNumber%5C%22%3A86%2C%5C%22x%5C%22%3A285.5405361662%2C%5C%22y%5C%22%3A0.28472599407325%7D%5D%22&timings.plot.budget.step=100&timings.plot.budget.duplicates.filter=true&timings.plot.budget.limits.logarithmic=true&recommendationColumn=%22stat.price.sum%22&instances.plot.scale.y=%22Linear%22&scaling.efficiency.param.%20p=0.98&.clientValue-default-plotlyCrosstalkOpts=%7B%22on%22%3A%22plotly_click%22%2C%22persistent%22%3Afalse%2C%22dynamic%22%3Afalse%2C%22selectize%22%3Afalse%2C%22opacityDim%22%3A0.2%2C%22selected%22%3A%7B%22opacity%22%3A1%7D%2C%22debounce%22%3A0%2C%22color%22%3A%5B%5D%7D&plotly_afterplot-A=%22%5C%22instances.plot.queriesPerDollar%5C%22%22&timings.plot.budget.col.cost=%22stat.price.sum%22&instances.plot.y=%22col.recom.inv%22&instances.plot.scale.x=%22Linear%22&user.notes=%22%22&time.period.unit=%22Weeks%22&timings.plot.budget.col.optim=%22stat.time.sum%22&instanceSet=%222019-11-30%20%7C%20101%20%7C%20website%22&timings.plot.budget.limits.display=true&comparison.count=1&distr.caching.load.first=false

plots.m3.cost.draw <- function() {
  .read <- 2000
  .query <- data.frame(
    time.cpu  = 20,
    data.read = .read
  )
  .distr.cache <- model.make.distr.fn(0.8)(.read)
  .distr.fracs <- seq(0, 0.55, 0.05)
  .cost.list <- purrr::map_dfr(.distr.fracs, function(.frac) {
    .read.spool <- .frac * .read
    .distr.spool <- if (.frac == 0) { 0 } else {
      model.make.distr.fn(0.1)(round(.read.spool))
    }
    .time.fn <- model.make.timing.fn(
      .distr.list.caching  = list(.distr.cache),
      .distr.list.spooling = list(.distr.spool),
      .max.count = 1,
      .distr.caching.split.fn = model.distr.split.fn(FALSE)
    )
    .costs <- model.calc.costs(.query, plots.inst, .time.fn)
    .recom <- model.recommend.from.timings.arr(.query, .costs)
    head(.recom, n = 1)
  })
  .df <- .cost.list %>%
    dplyr::transmute(
             x = .distr.fracs,
             y = stat.price.sum,
             is.flank = x == 0 | id != dplyr::lag(id),
             color = ifelse(is.flank, "blue", "black"),
             label = str_replace(id.name, "xlarge", "")
           )
  .df.flanks <- .df %>% dplyr::filter(is.flank)
  ggplot(.df, aes(x = x, y = y)) +
    geom_line(color = "black") +
    geom_point(color = .df$color) +
    # geom_vline(data = .df.flanks, aes(xintercept=x)) +
    geom_text(aes(label = label), size = 2.2, nudge_x = 0.035, nudge_y = -0.015) +
    theme_bw() +
    theme(text = element_text(size = 7), plot.margin=grid::unit(c(0,0,0,0), "mm")) +
    labs(x = "Materialized Fraction", y = "Workload Cost ($)")
}

## plots.m3.cost.draw()

## ggsave(plots.mkpath("m3-cost-spool.pdf"), plots.m3.cost.draw(),
##        width = 3, height = 2, units = "in",
##        device = cairo_pdf)
##
all.params <- try.params()
plots.m3.spool.draw <- function() {
  res <- all.params
  plotdata <- res %>%
    dplyr::filter(rank == 1) %>%
    dplyr::mutate(
             id.prefix = sub("^([A-Za-z1-9-]+)\\..*", "\\1", id)
           )
  palette <- styles.color.palette.light
  ggplot(plotdata, aes(x = param.scanned, y = param.spool.frac,
                       label = str_replace(id.name, "xlarge", ""))) +
    scale_fill_manual(values = palette) +
    geom_tile(aes(fill = id.prefix)) +
    scale_y_continuous(expand = c(0, 0)) +
    scale_x_log10(expand = c(0, 0), breaks = c(100, 1024, 10 * 1024, 100 * 1024),
                  labels = c("100GB", "1TB", "10TB", "100TB")) +
    theme(legend.position = "bottom") +
    labs(
      x = "Data Scanned [log]",
      y = "Materialization Fraction")
}

## plots.m3.spool.draw()
ggsave(plots.mkpath("m3-spool-frac-areas.pdf"), plots.m3.spool.draw(),
       width = 3.6, height = 2.5, units = "in",
       device = cairo_pdf)


## ---------------------------------------------------------------------------------------------- ##
                                        # M4: Scale Out & Down #
## ---------------------------------------------------------------------------------------------- ##

## Interesting Budget Configuration c5d -> c5n -> c5
## http://127.0.0.1:3030/?_inputs_&instance.count.max=32&locality=0.2&timings.plot.budget.ticks.at.limits=true&instSet.details.show=0&instanceFilter=%22Paper%20Table%201%22&timings.plot.budget.step.digits=3&instance=%22c5d.24xlarge%22&spooling.fraction=0.4&tables.frontier.show=false&plotly_relayout-A=%22%7B%5C%22width%5C%22%3A1242%2C%5C%22height%5C%22%3A9600%7D%22&instances.plot.display.frontier=true&time.cpu=25&instances.plot.frontier.quadrant=%22top.left%22&data.read=2008&plotly_doubleclick-A=%22%5C%22instances.plot%5C%22%22&instance.type.opt.include=%22all%22&plotly_click-A=%22%5B%7B%5C%22curveNumber%5C%22%3A0%2C%5C%22pointNumber%5C%22%3A4%2C%5C%22x%5C%22%3A2383.80703584345%2C%5C%22y%5C%22%3A0.116526956083714%7D%5D%22&instFilter.details.show=0&config.options.show=0&instances.plot.display.frontier.only=false&spooling.shape=0.1&instances.plot.x=%22stat.time.sum%22&time.period.num=1&plotly_hover-A=null&timings.plot.budget.step=100&timings.plot.budget.duplicates.filter=true&timings.plot.budget.limits.logarithmic=true&recommendationColumn=%22stat.price.sum%22&instances.plot.scale.y=%22Linear%22&scaling.efficiency.param.%20p=0.98&.clientValue-default-plotlyCrosstalkOpts=%7B%22on%22%3A%22plotly_click%22%2C%22persistent%22%3Afalse%2C%22dynamic%22%3Afalse%2C%22selectize%22%3Afalse%2C%22opacityDim%22%3A0.2%2C%22selected%22%3A%7B%22opacity%22%3A1%7D%2C%22debounce%22%3A0%2C%22color%22%3A%5B%5D%7D&plotly_afterplot-A=%22%5C%22instances.plot.queriesPerDollar%5C%22%22&timings.plot.budget.col.cost=%22stat.price.sum%22&instances.plot.y=%22col.recom.inv%22&instances.plot.scale.x=%22Linear%22&user.notes=%22%22&time.period.unit=%22Weeks%22&timings.plot.budget.col.optim=%22stat.time.sum%22&instanceSet=%222019-11-30%20%7C%20101%20%7C%20website%22&timings.plot.budget.limits.display=true&comparison.count=1&distr.caching.load.first=false

## TODO: add snowflake c5d for comparison
## TODO: logarithmic budget limits
## TODO: all c5d's
plots.m4.budget.draw <- function() {
  .read <- 10000
  .query <- data.frame(
    time.cpu  = 5,
    data.read = .read
  )
  .max.count <- 128
  .distr.cache <- purrr::map(1:.max.count, function(count) {
    model.make.distr.fn(0.001)(round(.read / count))
  })
  .distr.spool <- purrr::map(1:.max.count, function(count) {
    model.make.distr.fn(0.001)(round(0.3 * .read / count))
  })
  .scaling.fac <- 0.95

  .time.fn <- model.make.timing.fn(
    .distr.list.caching  = .distr.cache,
    .distr.list.spooling = .distr.spool,
    .max.count = .max.count,
    .eff.fn = model.make.scaling.fn(list(p = .scaling.fac)),
    .distr.caching.split.fn = model.distr.split.fn(FALSE),
    .time.period = 2^30
  )

  .inst <- rbind(aws.data.current.large.relevant,
                 aws.data.current %>% dplyr::filter(id == "c5d.2xlarge") %>% dplyr::mutate(id = "snowflake"))
  .inst.id <- c("c5d", "snowflake", "x1")

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
    geom_point(data = .greys, size = 0.7) +
    geom_point(data = .colored, size = 1) +
    geom_text(data = .labels, nudge_x = -0.15, nudge_y = 0.08) +
    scale_x_log10(limits = c(30, 1e05)) +
    scale_y_log10() +
    labs(y = "Workload Cost ($) [log]",
         x = "Workload Execution Time (s) [log]",
         color = "Instance") +
    theme_bw() +
    theme(plot.margin=grid::unit(c(1,1,1,1), "mm"),
          legend.position = "none")
}

## plots.m4.budget.draw()
ggsave(plots.mkpath("m4-budget.pdf"), plots.m4.budget.draw(),
       width = 3.6, height = 2.6, units = "in",
       device = cairo_pdf)
