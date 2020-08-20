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

plots.m1.all.draw <- function() {
  .n.points <- 100
  .value.range <- seq(0, 60, length.out = .n.points)
  .xlim <- tail(.value.range, n = 1)
  .query <- data.frame(
    time.cpu  = .value.range,
    data.read = 3000
  )
  .ids <- c("c5n", "c5", "c5d", "m5dn", "i3", "i3en", "other")

  .inst <- aws.data.current.large.relevant

  palette <- style.instance.colors.vibrant
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
    geom_text(data = .flanks, aes(y = y, x = x + 0.35 * max(x), label = paste(label, "best"))) +
    geom_text(data = .labeled, aes(x = max(x) + 1, label = label), hjust = 0) +
    annotate(geom = "text", label = "x1e", x = 22.7, y = 14.3, color = "grey") +
    theme(plot.margin=grid::unit(c(1,1,1,1), "mm"),
          legend.position = "none") +
    coord_cartesian(xlim = c(1, 69), ylim = c(0, 15)) +
    labs(x = "CPU Hours", y = "Workload Cost ($)")
}

## plots.m1.all.draw()
## ggsave(plots.mkpath("m1-cost-cpu-all.pdf"), plots.m1.all.draw(),
##        width = 3.6, height = 2.3, units = "in",
##        device = cairo_pdf)


## ---------------------------------------------------------------------------------------------- ##
                                        # M2: Caching & Materialization #
## ---------------------------------------------------------------------------------------------- ##

## Config where changing only access distribution results in c5n -> c5d -> c5
## http://127.0.0.1:3030/?_inputs_&instances.slicing_search=%22%22&instances.times_cell_clicked=%7B%7D&timings.plot.budget.ticks.at.limits=true&timings.plot.budget.step.digits=3&instances.rws.gib_rows_current=%5B1%2C2%2C3%5D&instance=%22c5.24xlarge%22&instances.specs.derived_rows_all=%5B1%2C2%5D&instances.times.perc_rows_all=%5B1%2C2%2C3%5D&instances.slicing_rows_current=%5B1%2C2%5D&instances.slicing_cell_clicked=%7B%7D&instances.times_rows_all=%5B1%2C2%2C3%5D&instances.prices_rows_all=%5B1%2C2%2C3%5D&time.cpu=12&instances.plot.frontier.quadrant=%22top.left%22&instances.metadata_search=%22%22&instances.specs_rows_current=%5B1%2C2%5D&instances.reads.gib_state=null&instances.prices_search=%22%22&instances.times.perc_state=null&instances.reads.gib_rows_all=%5B1%2C2%2C3%5D&instFilter.details.show=0&config.options.show=1&instances.plot.display.frontier.only=false&time.period.num=1&instances.plot.x=%22stat.time.sum%22&instances.slicing_rows_selected=null&plotly_hover-A=null&instances.rws.gib_state=null&instances.specs.derived_cell_clicked=%7B%7D&timings.plot.budget.step=100&timings.plot.budget.duplicates.filter=true&timings.plot.budget.limits.logarithmic=true&scaling.efficiency.param.%20p=0.98&instances.plot.scale.y=%22Linear%22&.clientValue-default-plotlyCrosstalkOpts=%7B%22on%22%3A%22plotly_click%22%2C%22persistent%22%3Afalse%2C%22dynamic%22%3Afalse%2C%22selectize%22%3Afalse%2C%22opacityDim%22%3A0.2%2C%22selected%22%3A%7B%22opacity%22%3A1%7D%2C%22debounce%22%3A0%2C%22color%22%3A%5B%5D%7D&instances.specs.derived_rows_current=%5B1%2C2%5D&instances.specs_rows_selected=null&timings.plot.budget.col.cost=%22stat.price.sum%22&instances.specs_cell_clicked=%7B%7D&instances.rws.gib_search=%22%22&instances.metadata_rows_selected=null&instances.metadata_rows_current=%5B1%2C2%5D&instances.metadata_state=null&instances.prices_cell_clicked=%7B%7D&user.notes=%22%22&time.period.unit=%22Weeks%22&timings.plot.budget.col.optim=%22stat.time.sum%22&instanceSet=%222019-11-30%20%7C%20101%20%7C%20website%22&timings.plot.budget.limits.display=true&instances.specs.derived_search=%22%22&instances.specs_state=null&instances.rws.gib_rows_all=%5B1%2C2%2C3%5D&instances.times_rows_selected=null&distr.caching.load.first=false&instance.count.max=1&instances.specs_rows_all=%5B1%2C2%5D&instances.reads.gib_rows_current=%5B1%2C2%2C3%5D&locality=0.96&instances.specs_search=%22%22&instSet.details.show=0&instances.reads.gib_rows_selected=null&instanceFilter=%22Paper%20Table%201%22&instances.times_rows_current=%5B1%2C2%2C3%5D&plotly_relayout-A=%22%7B%5C%22width%5C%22%3A1242%2C%5C%22height%5C%22%3A400%7D%22&spooling.fraction=0&tables.frontier.show=false&instances.reads.gib_search=%22%22&instances.plot.display.frontier=true&instances.metadata_cell_clicked=%7B%7D&instances.times_search=%22%22&data.read=1798&instances.times.perc_rows_selected=null&instance.type.opt.include=%22frontier%22&time.cpu.maxval=1000&instances.slicing_state=null&instances.prices_state=null&spooling.shape=0.1&data.read.maxval=10000&instances.times_state=null&instances.prices_rows_current=%5B1%2C2%2C3%5D&instances.reads.gib_cell_clicked=%7B%7D&instances.rws.gib_cell_clicked=%7B%7D&recommendationColumn=%22stat.price.sum%22&instances.specs.derived_rows_selected=null&plotly_afterplot-A=%22%5C%22distr.spooling.recommended.plot%5C%22%22&instances.specs.derived_state=null&instances.rws.gib_rows_selected=null&instances.metadata_rows_all=%5B1%2C2%5D&instances.times.perc_cell_clicked=%7B%7D&instances.prices_rows_selected=null&instances.plot.y=%22col.recom.inv%22&instances.plot.scale.x=%22Linear%22&instances.times.perc_search=%22%22&comparison.count=1&instances.times.perc_rows_current=%5B1%2C2%2C3%5D&instances.slicing_rows_all=%5B1%2C2%5D

## Config where chaning the materialization fraction changes from c5 -> c5d -> c5n
## http://127.0.0.1:3030/?_inputs_&instance.count.max=32&locality=0.8&timings.plot.budget.ticks.at.limits=true&instSet.details.show=0&instanceFilter=%22Paper%20Table%201%22&timings.plot.budget.step.digits=3&instance=%22%22&spooling.fraction=0.4&tables.frontier.show=false&plotly_relayout-A=%22%7B%5C%22width%5C%22%3A1242%2C%5C%22height%5C%22%3A9220%7D%22&instances.plot.display.frontier=true&time.cpu=20&instances.plot.frontier.quadrant=%22top.left%22&data.read=1998&instance.type.opt.include=%22frontier%22&instFilter.details.show=0&config.options.show=0&instances.plot.display.frontier.only=false&spooling.shape=0.1&instances.plot.x=%22stat.time.sum%22&time.period.num=1&plotly_hover-A=%22%5B%7B%5C%22curveNumber%5C%22%3A0%2C%5C%22pointNumber%5C%22%3A86%2C%5C%22x%5C%22%3A285.5405361662%2C%5C%22y%5C%22%3A0.28472599407325%7D%5D%22&timings.plot.budget.step=100&timings.plot.budget.duplicates.filter=true&timings.plot.budget.limits.logarithmic=true&recommendationColumn=%22stat.price.sum%22&instances.plot.scale.y=%22Linear%22&scaling.efficiency.param.%20p=0.98&.clientValue-default-plotlyCrosstalkOpts=%7B%22on%22%3A%22plotly_click%22%2C%22persistent%22%3Afalse%2C%22dynamic%22%3Afalse%2C%22selectize%22%3Afalse%2C%22opacityDim%22%3A0.2%2C%22selected%22%3A%7B%22opacity%22%3A1%7D%2C%22debounce%22%3A0%2C%22color%22%3A%5B%5D%7D&plotly_afterplot-A=%22%5C%22instances.plot.queriesPerDollar%5C%22%22&timings.plot.budget.col.cost=%22stat.price.sum%22&instances.plot.y=%22col.recom.inv%22&instances.plot.scale.x=%22Linear%22&user.notes=%22%22&time.period.unit=%22Weeks%22&timings.plot.budget.col.optim=%22stat.time.sum%22&instanceSet=%222019-11-30%20%7C%20101%20%7C%20website%22&timings.plot.budget.limits.display=true&comparison.count=1&distr.caching.load.first=false

all.params <- try.params()
plots.m2.spool.draw <- function() {
  res <- all.params
  plotdata <- res %>%
    dplyr::filter(rank == 1) %>%
    dplyr::mutate(id.prefix = sub("^([A-Za-z1-9-]+)\\..*", "\\1", id))
  palette <- style.instance.colors
  ggplot(plotdata, aes(x = param.scanned, y = param.spool.frac,
                       label = str_replace(id.name, "xlarge", ""))) +
    scale_fill_manual(values = palette) +
    geom_tile(aes(fill = id.prefix)) +
    ## geom_text() +
    scale_y_continuous(expand = c(0, 0),
                       breaks = seq(0, 1.0, 0.2),
                       labels = c("0", ".2", ".4", ".6", ".8", "1")) +
    scale_x_log10(expand = c(0, 0), breaks = c(100, 1024, 10 * 1024, 100 * 1024),
                  labels = c("100GB", "1TB", "10TB", "100TB")) +
    theme_bw() +
    theme(
      plot.margin=grid::unit(c(1,0,1,1), "mm"),
      legend.position = "none") +
    labs(
      x = "Scanned Data [log]",
      y = "Materialization Fraction") +
    facet_grid(cols = vars(param.cpuh), labeller = function(x) { "Best Instance" })
}

## plots.m2.spool.draw()
## ggsave(plots.mkpath("m2-spool-best.pdf"), plots.m2.spool.draw(),
##        width = 2.2, height = 2.5, units = "in",
##        device = cairo_pdf)


plots.m2.draw.diff.for <- function(.id) {
  .cost <- all.params %>% dplyr::group_by_at(vars(starts_with("param.")))

  .diff <- dplyr::group_modify(.cost, function(group, keys) {
    .best <- dplyr::filter(group, rank == 1)
    .self <-  dplyr::filter(group, id.name %in% .id)
    dplyr::mutate(.self,
                  id.short = str_replace(id.name, "xlarge", ""),
                  price.diff.absolute = stat.price.sum - .best$stat.price.sum,
                  price.diff.fraction = stat.price.sum / .best$stat.price.sum,
                  price.diff.fractext = sprintf("%.2f", price.diff.fraction),
                  price.diff.fracdisc = ifelse(price.diff.fraction != 1 & price.diff.fraction < 1.1,
                                                "1.0x",
                                               sprintf("%.1f", pmin(2.5, price.diff.fraction))))
  })

  .diff$id.factor <- factor(.diff$id.short, levels = str_replace(.id, "xlarge", ""))

  palette <- styles.color.palette.temperature

  ggplot(.diff, aes(x = param.scanned, y = param.spool.frac,
                    label = price.diff.fractext, fill = price.diff.fracdisc)) +
    scale_fill_manual(values = palette) +
    geom_tile() +
    geom_text(size = 2) +
    scale_y_continuous(expand = c(0, 0)) +
    scale_x_log10(expand = c(0, 0), breaks = c(100, 1024, 10 * 1024, 100 * 1024),
                  labels = c("100GB", "1TB", "10TB", "100TB")) +
    theme_bw() +
    theme(legend.position = "none",
          axis.title.y = element_blank(),
          axis.text.y  = element_blank(),
          axis.ticks.y = element_blank(),
          plot.margin=grid::unit(c(1,1,1,0), "mm")) +
    labs(x = "Scanned Data [log]", y = "Materialization Fraction") +
    facet_grid(cols = vars(id.factor))
}

## plots.m2.diff.inst <- c("c5.24xlarge", "c5d.24xlarge", "i3.16xlarge","c5n.18xlarge")
## ggsave(plots.mkpath("m2-spool-diff.pdf"), plots.m2.draw.diff.for(plots.m2.diff.inst),
##        width = 3 * 2.5, height = 2.5, units = "in",
##        device = cairo_pdf)

## ---------------------------------------------------------------------------------------------- ##
                                        # M3: Scale Out & Down #
## ---------------------------------------------------------------------------------------------- ##

## Interesting Budget Configuration c5d -> c5n -> c5
## http://127.0.0.1:3030/?_inputs_&instance.count.max=32&locality=0.2&timings.plot.budget.ticks.at.limits=true&instSet.details.show=0&instanceFilter=%22Paper%20Table%201%22&timings.plot.budget.step.digits=3&instance=%22c5d.24xlarge%22&spooling.fraction=0.4&tables.frontier.show=false&plotly_relayout-A=%22%7B%5C%22width%5C%22%3A1242%2C%5C%22height%5C%22%3A9600%7D%22&instances.plot.display.frontier=true&time.cpu=25&instances.plot.frontier.quadrant=%22top.left%22&data.read=2008&plotly_doubleclick-A=%22%5C%22instances.plot%5C%22%22&instance.type.opt.include=%22all%22&plotly_click-A=%22%5B%7B%5C%22curveNumber%5C%22%3A0%2C%5C%22pointNumber%5C%22%3A4%2C%5C%22x%5C%22%3A2383.80703584345%2C%5C%22y%5C%22%3A0.116526956083714%7D%5D%22&instFilter.details.show=0&config.options.show=0&instances.plot.display.frontier.only=false&spooling.shape=0.1&instances.plot.x=%22stat.time.sum%22&time.period.num=1&plotly_hover-A=null&timings.plot.budget.step=100&timings.plot.budget.duplicates.filter=true&timings.plot.budget.limits.logarithmic=true&recommendationColumn=%22stat.price.sum%22&instances.plot.scale.y=%22Linear%22&scaling.efficiency.param.%20p=0.98&.clientValue-default-plotlyCrosstalkOpts=%7B%22on%22%3A%22plotly_click%22%2C%22persistent%22%3Afalse%2C%22dynamic%22%3Afalse%2C%22selectize%22%3Afalse%2C%22opacityDim%22%3A0.2%2C%22selected%22%3A%7B%22opacity%22%3A1%7D%2C%22debounce%22%3A0%2C%22color%22%3A%5B%5D%7D&plotly_afterplot-A=%22%5C%22instances.plot.queriesPerDollar%5C%22%22&timings.plot.budget.col.cost=%22stat.price.sum%22&instances.plot.y=%22col.recom.inv%22&instances.plot.scale.x=%22Linear%22&user.notes=%22%22&time.period.unit=%22Weeks%22&timings.plot.budget.col.optim=%22stat.time.sum%22&instanceSet=%222019-11-30%20%7C%20101%20%7C%20website%22&timings.plot.budget.limits.display=true&comparison.count=1&distr.caching.load.first=false

plots.m3.time.cost.draw <- function() {
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
    geom_point(data = .greys, size = 1) +
    # geom_point(data = .colored, size = 1) +
    geom_text(data = .labels, nudge_x = -0.15, nudge_y = 0.08) +
    geom_text(data = .colored, aes(label = count)) +
    scale_x_log10(limits = c(30, 6e03)) +
    scale_y_log10() +
    labs(y = "Workload Cost ($) [log]",
         x = "Workload Execution Time (s) [log]",
         color = "Instance") +
    theme_bw() +
    theme(plot.margin=grid::unit(c(1,1,1,1), "mm"),
          legend.position = "none")
}

## plots.m3.time.cost.draw()
## ggsave(plots.mkpath("m3-time-cost.pdf"), plots.m3.time.cost.draw(),
##        width = 3.6, height = 2.6, units = "in",
##        device = cairo_pdf)

## TODO: dedicated snowflake plot?

## ---------------------------------------------------------------------------------------------- ##
                                        # MH: History #
## ---------------------------------------------------------------------------------------------- ##
##
history.mkdata <- memoize(function() {
  .wl1 <- data.frame(
    cpu.hours  = 5,
    data.scan  = 10000,
    max.count  = 1,
    cache.skew = 0.001,
    spool.skew = 0.001,
    spool.frac = 0.3,
    scale.fact = 0.95,
    load.first = FALSE,
    max.period = 2^30)
  .wl2 <- dplyr::mutate(.wl1, data.scan = data.scan * 100)
  .wl3 <- dplyr::mutate(.wl1, cpu.hours = cpu.hours * 100000)
  #
  .def1 <- model.gen.workload(.wl1)
  .def2 <- model.gen.workload(.wl2)
  .def3 <- model.gen.workload(.wl3)
  #
  .filtered <- c("m6g")
  .inst <- aws.data.all.by.date %>% dplyr::filter(!(id.prefix %in% .filtered))
  .cost <- dplyr::group_modify(.inst, function(.slice, date) {
    .large <- aws.data.filter.large(.slice)
    .recom <- rbind(
      model.calc.costs(.def1$query, .large, .def1$time.fn) %>%
      model.recommend.from.timings.arr(.def1$query, .) %>%
      dplyr::mutate(workload.id = "A"),
      #
      model.calc.costs(.def2$query, .large, .def2$time.fn) %>%
      model.recommend.from.timings.arr(.def2$query, .) %>%
      dplyr::mutate(workload.id = "B"),
      #
      model.calc.costs(.def3$query, .large, .def3$time.fn) %>%
      model.recommend.from.timings.arr(.def3$query, .) %>%
      dplyr::mutate(workload.id = "C")
    )
    dplyr::inner_join(.large, .recom, by = c("id" = "id.name")) %>%
      dplyr::mutate(configuration = id.y)
  })
  #
  .cost %>%
    dplyr::mutate(label = paste(count, "×", str_replace(id, "xlarge", ""), sep=" "))
})

                                        # TODO: annotate text with aws 'introduces ...'
                                        # - 100Gbit network
                                        # - 25Gibt network
                                        # - cpu cost doesn't decrease much
                                        # - fast nvme ssds
plots.mh.history.cost.draw <- function() {
  .df <- history.mkdata() %>%
    dplyr::group_by(workload.id) %>%
    dplyr::group_modify(function(group, key) {
      .first <- dplyr::filter(group, commit.date == min(commit.date))
      dplyr::mutate(group,
                    stat.price.change = stat.price.sum / .first$stat.price.sum
                    )
    })
  ggplot(.df, aes(x = meta.join.time, y = stat.price.change, label = label, color = workload.id)) +
    geom_line(aes(group = workload.id), linetype = "dashed") +
    ## geom_text(data = dplyr::filter(.df, workload.id == "A"), nudge_y = 0.01, nudge_x = 0.2) +
    geom_line(aes(group = paste(workload.id, id)), size = 1.5) +
    labs(x = "Date", y = "Normalized Workload Cost", color = "Workload") +
    theme_bw() +
    scale_y_continuous(limits = c(0, 1.1),
                       breaks = seq(0, 1, 0.2),
                       labels = c("0", ".2", ".4", ".6", ".8", "1")) +
    theme(legend.position = "none",
          plot.margin=grid::unit(c(1,1,1,1), "mm"))
}

## plots.mh.history.cost.draw()
ggsave(plots.mkpath("mh-date-cost.pdf"), plots.mh.history.cost.draw(),
       width = 3.6, height = 2.3, units = "in",
       device = cairo_pdf)
