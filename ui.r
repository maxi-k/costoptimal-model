source("./util.r")
source("./aws.r")
source("./model.r")

## ---------------------------------------------------------------------------------------------- ##
                                        # UI constants & definitions
## ---------------------------------------------------------------------------------------------- ##

## From https://github.com/arcticicestudio/nord
ui.styles.color.palette1 <- c(
    "#a3be8c",
    "#ebcb8b",
    "#b48ead",
    "#8fbcbb",
    "#5e81ac",
    "#4c566a",
    "#bf616a",
    "#d08770",
    "#81a1c1",
    "#d8dee9",
    "#88c0d0"
)

ui.scaling.def <- list(
  formula = "$$w = W \\cdot ((1 - p) + \\frac{p}{s})$$",
  fn = model.make.scaling.fn,
  params = list(
    p = list(
      min = 0.5,
      max = 1,
      default = 0.98,
      step = 0.0001,
      description = "Portion of the workload that can be parallelized. Note that \\(p = 1\\) equates to linear speedup over \\(\\eta\\)."
    ),
    eta = list(
      min = 0.1,
      max = 1,
      default = 1,
      description = "Factor that scales the speedup with number of instances: \\( \\eta \\cdot n = s \\) from Amdahls law."
    )
  )
)

ui.time.units <- list(
  "Seconds" = 1,
  "Minutes" = 60,
  "Hours" = 60 * 60,
  "Days" = 60 * 60 * 24,
  "Weeks" = 60 * 60 * 24 * 7,
  "Months" = round(60 * 60 * 24 * 7 * 52 / 12)
)

ui.instance.filters <- list(
  "Without Slices" = aws.data.filter.large,
  "Without A1.*, G3.*, P3.*" = aws.data.filter.relevant.family,
  "Spot Prices (eu-central avg)" = aws.data.filter.spot.price,
  "S3 Benchmarks" = aws.data.filter.s3join.all,
  "Spot Interruptions <5%" = aws.data.mkfilter.spot.inter.freq(5),
  "Spot Interruptions <10%" = aws.data.mkfilter.spot.inter.freq(10),
  "Spot Interruptions <20%" = aws.data.mkfilter.spot.inter.freq(20)
)
ui.instance.filter.initial = c("Without Slices", "Without A1.*, G3.*, P3.*")
ui.instance.filter.for.snowset = "Without A1.*, G3.*, P3.*"

ui.instance.sets <- aws.data.all.by.date %>% dplyr::group_split() %>% rev()
names(ui.instance.sets) <- aws.data.all.by.date %>% group_keys() %>% .$meta.group %>% rev()

ui.instance.opts.count.max <- 128
ui.instance.opts.count.initial <- 32

## ---------------------------------------------------------------------------------------------- ##
                                        # UI Calculations
## ---------------------------------------------------------------------------------------------- ##

ui.instance.filter.fn.get <- function(selection) {
  Reduce(f = function(g, f) function(x) f(g(x)),
         x = ui.instance.filters[selection],
         init = identity)
}

ui.calc.frontier <- function(data, x = "x", y = "y", id = "id",
                             quadrant = c("top.right", "bottom.right",
                                          "bottom.left", "top.left"),
                             decreasing = TRUE) {
  if (!is.data.frame(data)) {
    stop(deparse(substitute(data)), " is not a data frame.")
  }

  z <- data[, c(x, y, id)]
  z <- stats::na.omit(z)

  if (!is.numeric(z[[x]]) | !is.numeric(z[[y]])) {
    stop("both x and y must be numeric variables")
  }

  quadrant <- match.arg(quadrant)
  if (quadrant == "top.right") {
    zz <- z[order(z[, 1L], z[, 2L], decreasing = TRUE), ]
    zz <- zz[which(!duplicated(cummax(zz[, 2L]))), ]
    zz[order(zz[, 1L], zz[, 2L], decreasing = decreasing), ]
  } else if (quadrant == "bottom.right") {
    zz <- z[order(z[, 1L], z[, 2L], decreasing = TRUE), ]
    zz <- zz[which(!duplicated(cummin(zz[, 2L]))), ]
    zz <- zz[which(!duplicated(zz[, 1L])), ]
    zz[order(zz[, 1L], zz[, 2L], decreasing = decreasing), ]
  } else if (quadrant == "bottom.left") {
    zz <- z[order(z[, 1L], z[, 2L], decreasing = FALSE), ]
    zz <- zz[which(!duplicated(cummin(zz[, 2L]))), ]
    zz[order(zz[, 1L], zz[, 2L], decreasing = decreasing), ]
  } else {
    zz <- z[order(z[, 1L], z[, 2L], decreasing = FALSE), ]
    zz <- zz[which(!duplicated(cummax(zz[, 2L]))), ]
    zz <- zz[order(zz[, 1L], zz[, 2L], decreasing = TRUE), ]
    zz <- zz[which(!duplicated(zz[, 1L])), ]
    zz[order(zz[, 1L], zz[, 2L], decreasing = decreasing), ]
  }
}

ui.data.timing.enrich <- function(.df, col.rec) {
  .df %>%
    dplyr::mutate(
             col.recom.inv = 1 / .[[col.rec]]
           )
}

ui.as.dt.formatted <- function(df, signif = 4, ...) {
  numcols <- Filter(is.numeric, df) %>% colnames()
  df %>%
    DT::datatable(rownames = FALSE, ...) %>%
    DT::formatSignif(columns = numcols, digits = 3)
}

## ---------------------------------------------------------------------------------------------- ##
                                        # Plotting Functions
## ---------------------------------------------------------------------------------------------- ##

## Distribution Plot
ui.plot.distribution <- function(instance, dist) {
  initdata <- dist$initial %>% data.frame(y = ., x = 1:length(.), group = "S3 Initial Load")
  data <- dist$working %>% data.frame(y = . + initdata$y, x = 1:length(.))
  data$group = if_else(data$x <= instance$memory.GiB, "Memory", "SSD/S3")
  nudge.x <- 0.03 * max(c(max(instance$memory.GiB + instance$storage.GiB), nrow(data)))
  nudge.y <- max(data$y) / 4
  plot <- ggplot(instance) +
    scale_fill_manual(values=ui.styles.color.palette1) +
    geom_area(data=data, aes(y = y, x = x, fill = group), stat="identity") +
    geom_area(data=initdata, aes(y = y, x = x, fill = group), stat="identity") +
    geom_vline(aes(xintercept=memory.GiB), colour="blue") +
    geom_vline(aes(xintercept=memory.GiB + storage.GiB), colour="blue") +
    geom_text(aes(x=memory.GiB, y=nudge.y * 2, label="Memory"), colour="blue", angle=90,
              nudge_x = nudge.x, nudge_y = -nudge.y) +
    geom_text(aes(x=memory.GiB + storage.GiB, y=nudge.y * 2, label="Storage"), colour="blue", angle=90,
              nudge_x = nudge.x, nudge_y = nudge.y) +
    labs(x = "Data Read [GiB]",
         y = "Number of Accesses")
  ggplotly(plot, dynamicTicks = TRUE)
}

## Generic Instance Plot
ui.instance.plot.pareto.quadrants <- list(
  "Top Left" = "top.left",
  "Top Right" = "top.right",
  "Bottom Right" = "bottom.right",
  "Bottom Left" = "bottom.left"
)
ui.instance.opts.type.inclusion <- list(
  "Count Frontier" = "frontier",
  "Best Count" = "best.count",
  "All" = "all"
)

ui.instance.plot.scalings <- list(
  "Linear" = c(scale_x_continuous, scale_y_continuous),
  "Logarithmic" = c(scale_x_log10, scale_y_log10),
  "Reverse" = c(scale_x_reverse, scale_y_reverse)
)

ui.instance.plot.scale <- names(ui.instance.plot.scalings)

ui.instance.plot.numeric.cols <- model.calc.costs(
  query = data.frame(data.xchg = 10, data.read = 100, time.cpu = 100),
  inst = aws.data.current.large,
  timing.fn = model.make.timing.fn(purrr::map(3:8, function(x) model.make.distr.fn(10)(x)), 1)
) %>%
  ui.data.timing.enrich("stat.time.sum") %>%
  dplyr::select_if(is.numeric)

ui.instance.plot.colnames <- ui.instance.plot.numeric.cols %>% colnames()
ui.instance.plot.colnames.recom <- ui.instance.plot.numeric.cols %>%
  dplyr::select(-col.recom.inv) %>%
  colnames()


## ---------------------------------------------------------------------------------------------- ##
                                        # Shiny Server
## ---------------------------------------------------------------------------------------------- ##


server <- function(input, output, session) {

  instSet.all <- reactive({
    ui.instance.sets[[input$instanceSet]]
  })

  instSet.long <- reactive({
    filterDef <- ui.instance.filter.fn.get(input$instanceFilter)
    instSet.all() %>%
      filterDef() %>%
      dplyr::filter(complete.cases(.))
  })

  instSet <- reactive({
    instSet.long() %>%
      dplyr::select(-starts_with("commit."), -starts_with("meta."))
  })

  output$app.title <- renderUI({
    inst <- instSet.long()
    if (any(inst$loading.comment != "")) {
      msgs <- inst %>%
        dplyr::filter(loading.comment != "") %>%
        dplyr::group_by(loading.comment) %>%
        dplyr::summarise(msg.count = dplyr::n()) %>%
        dplyr::transmute(msg = paste(loading.comment, " (", msg.count, " rows).", sep="")) %>%
        .$msg
      shiny::wellPanel(
               class = "warning-msg",
               "Filtered dataset with ", shiny::strong(nrow(inst)), " rows has these warnings:",
               shiny::br(),
               paste(msgs, collapse=" ⚬ ")
             )
    } else {
      titlePanel("Instance Recommender")
    }
  })

  ## -------------------------------------------------------------------------------------- ##
                                        # TIMINGS & QUERY BASE DEFINITIONS #
  ## -------------------------------------------------------------------------------------- ##

  query.data.frame <- reactive({
    data.frame(
      time.cpu  = input$time.cpu %||% 0,
      data.read = input$data.read %||% ui.instance.opts.count.max,
      data.xchg = input$data.xchg %||% 0
    )
  })

  query.time.period.seconds <- reactive({
    input$time.period.num * ui.time.units[[input$time.period.unit]]
  })

  distribution.precomputed <- reactive({
    .scale.fn <- ui.scaling.def$fn(scaling.efficiency.params())
    .read <- input$data.read
    .distr.fn <- model.make.distr.fn(shape = input$locality)
    res <- purrr::map(1:ui.instance.opts.count.max, function(count) .distr.fn(.read))
    res
  })

  distribution.recommended <- reactive({
    model.distr.split.fn(input$distribution.load.first)(distribution.precomputed()[[head(inst.recommendation(), n = 1)$count]])
  })

  distribution.comparison <- reactive({
    model.distr.split.fn(input$distribution.load.first)(distribution.precomputed()[[input$comparison.count]])
  })

  instance.timings.all <- reactive({
    .col.rec <- input$recommendationColumn
    .scale.par <- scaling.efficiency.params()
    .inst <- instSet()
    timing.fn <- model.make.timing.fn(distribution.precomputed(),
                                     input$instance.count.max,
                                     ui.scaling.def$fn(.scale.par),
                                     model.distr.split.fn(input$distribution.load.first),
                                     query.time.period.seconds())
    model.calc.costs(
      query = query.data.frame(),
      inst = .inst,
      timing.fn = timing.fn
    ) %>% ui.data.timing.enrich(.col.rec)
  })

  instance.timings <- reactive({
    times <- instance.timings.all()
    .include <- input$instance.type.opt.include

    if (is.null(.include) | length(.include) == 0 | .include == "all") {
      return(times)
    } else {
      if(.include == "frontier") {
        frontiers <- times %>%
          dplyr::group_by(id.name) %>%
          dplyr::group_map(~ ui.calc.frontier(.,
                                             x = input$instances.plot.x,
                                             y = input$instances.plot.y,
                                             id = "id",
                                             quadrant = input$instances.plot.frontier.quadrant)) %>%
          bind_rows()
        best <- dplyr::filter(times, id %in% frontiers$id)
      } else if (.include == "best.count") {
        col.rec <- input$recommendationColumn
        by <- as.name(col.rec)
        best <- times %>%
          dplyr::group_by(id.name) %>%
          dplyr::top_n(-1, wt = !!by) %>%
          dplyr::ungroup()
      }
      .comp.id <- inst.comparison.id()
      compcnt <- dplyr::filter(best, id.name == .comp.id)$count
      rbind(best, dplyr::filter(times, id.name == .comp.id &
                                       count == input$comparison.count &
                                       !(count %in% compcnt)))
    }
  })

  inst.recommendation <- reactive({
    by <- as.name(input$recommendationColumn)
    .df <- query.data.frame()
    .times <- instance.timings() %>%
      dplyr::filter(stat.time.sum <= query.time.period.seconds())
    model.recommend.from.timings.arr(
      query = .df,
      timings = .times,
      by = !! by)
  })

  inst.recommended.id <- reactive({
    inst.recommendation()$id
  })

  inst.recommended <- reactive({
    instSet() %>% dplyr::filter(id %in% inst.recommendation()$id.name)
  })

  inst.recommended.timings <- reactive({
    instance.timings() %>% dplyr::filter(id %in% inst.recommended.id())
  })

  inst.comparison.id <- reactive({
    choice <- input$instance
    if (is.null(choice)) {
      instSet()$id[1]
    } else {
      choice
    }
  })

  inst.comparison <- reactive({
    instSet() %>% dplyr::filter(id == inst.comparison.id()) %>% head(n = 1)
  })

  inst.comparison.timings <- reactive({
    by <- as.name(input$recommendationColumn)
    .comp <- inst.comparison.id()
    instance.timings() %>%
      dplyr::filter(id.name == .comp) %>%
      dplyr::arrange( !! by)
  })

  inst.frontier.timings <- reactive({
    ids <- instances.plot.frontier()$id
    instance.timings() %>% dplyr::filter(id %in% ids)
  })

  inst.frontier <- reactive({
    timings <- inst.frontier.timings()$id.name
    instSet() %>% dplyr::filter(id %in% timings)
  })

  instance.timings.plot <- reactive({
    times <- instance.timings()
    if (input$instances.plot.display.frontier.only) {
      frontier <- ui.calc.frontier(times,
                                    x = input$instances.plot.x,
                                    y = input$instances.plot.y,
                                    id = "id",
                                    quadrant = input$instances.plot.frontier.quadrant)

      times <- times %>% dplyr::filter(id %in% frontier$id | id.name == inst.comparison.id())
    }
    times
  })

  inst.recommended.text <- reactive({
    paste(inst.recommended.id(), collapse = ", ")
  })

  ## -------------------------------------------------------------------------------------- ##
                                        # READ ACCESS DISTRIBUTION #
  ## -------------------------------------------------------------------------------------- ##

  output$distributionPlotRecommended <- renderPlotly({
    ui.plot.distribution(head(inst.recommended(), n = 1), distribution.recommended())
  })

  output$distributionPlotComparison <- renderPlotly({
    selected <- inst.comparison()
    validate(
      need(nrow(selected) >= 1, "Please select a comparison instance")
    )
    ui.plot.distribution(selected, distribution.comparison())
  })

  output$distribution.comparison.msg <- renderUI({
    inst <- inst.comparison.timings()
    if(nrow(inst) == 0) {
      return("")
    }
    best <- head(inst, n = 1)
    if(input$comparison.count == best$count) {
      return("")
    }
    shiny::wellPanel(
             class = "warning-msg top-margin",
             paste("The selected count is not the recommended count (", best$count, ")", sep="")
           )
  })

  ## -------------------------------------------------------------------------------------- ##
                                        # HEADER TEXT & SETTINGS #
  ## -------------------------------------------------------------------------------------- ##

  output$instance.comparison.selection <- renderUI({
    selectInput("instance",
                shiny::span("Instance type to", shiny::span("compare", style="color:blue;"), "to"),
                c(Choose='', sort(instSet()$id)),
                selectize=TRUE)
  })

  output$instance.recommended.text <- renderText(
    inst.recommended.text()
  )

  output$instance.comparison.texts <- renderUI({
    by <- input$recommendationColumn
    tcol <- "stat.time.sum"
    s <- inst.comparison.timings() %>% dplyr::filter(count == input$comparison.count) %>% head(n = 1)
    s.rec <- inst.comparison.timings() %>% head(n = 1)
    r <- inst.recommendation() %>% head(n = 1)

    if(nrow(s) == 0 | nrow(r) == 0) {
      ""
    } else {
      mktext <- function(pre, post, ...) {
        shiny::span(style="padding-left: 3px",
                    pre,
                    shiny::span(style = "color: blue;", ...),
                    post)
      }
      perc.price <- signif(100 * (s[[by]] / r[[by]] - 1), 4)
      if(s$stat.time.sum >= r$stat.time.sum) {
        perc.time <- signif(100 * (s[[tcol]] / r[[tcol]] - 1), 4)
        time.text <- mktext("The recommended configuration is ", " faster.", perc.time, "%")
      } else {
        perc.time <- signif(100 * (r[[tcol]] / s[[tcol]] - 1), 4)
        time.text <- mktext("The comparison configuration is ", " faster.", perc.time, "%")
      }
      shiny::div(
               mktext("The best count for the comparison instance is ", ".", s.rec$count),
               shiny::br(),
               mktext("The comparison configuration is ", " more expensive.", perc.price, "%"),
               shiny::br(),
               time.text
             )
    }
  })

  observeEvent(input$instance.count.max, {
    count.com <- input$comparison.count
    count.max <- input$instance.count.max
    updateSliderInput(session, "comparison.count", max=count.max, value=min(count.com, count.max))
  })

  ## -------------------------------------------------------------------------------------- ##
                                        # MAIN INSTANCES PLOT #
  ## -------------------------------------------------------------------------------------- ##

  instances.plot.frontier <- reactive({
    ui.calc.frontier(instance.timings.plot(),
                      x = input$instances.plot.x,
                      y = input$instances.plot.y,
                      id = "id",
                      quadrant = input$instances.plot.frontier.quadrant)
  })

  instance.colors <- reactive({
    timings <- instance.timings.all()
    frontier <- instances.plot.frontier()
    .com.id <- inst.comparison.id()
    .com.cnt <- input$comparison.count
    .rec.id <- inst.recommended.id()

    colors <- rep("black", nrow(timings))
    names(colors) = timings$id

    if(input$instances.plot.display.frontier) {
      colors[timings$id %in% frontier$id] <- "darkgreen"
    }
    colors[timings$id.name == .com.id] <- "skyblue"
    colors[timings$id.name == .com.id & timings$count == .com.cnt] <- "blue"
    colors[timings$id %in% .rec.id] <- "red"
    colors
  })

  output$instances.plot <- renderPlotly({
    timings <- instance.timings.plot()
    time.period <- query.time.period.seconds()

    scaleFn.x <- ui.instance.plot.scalings[[input$instances.plot.scale.x]][[1]]
    scaleFn.y <- ui.instance.plot.scalings[[input$instances.plot.scale.y]][[2]]
    colors <- instance.colors()[timings$id]

    plot <- ggplot(timings, aes(y = get(input$instances.plot.y),
                                x = get(input$instances.plot.x),
                                label = id)) +
      scaleFn.x() +
      scaleFn.y() +
      geom_point(shape = 3, color = colors) +
      ## geom_text_repel(color = colors) +
      labs(x = input$instances.plot.x, y = input$instances.plot.y)

    if(input$instances.plot.display.frontier) {
      frontier <- instances.plot.frontier()
      plot <- plot + geom_line(data = frontier, color = "green")
    }
    if(str_detect(input$instances.plot.x, "time") &
       max(timings[[input$instances.plot.x]]) >= (0.8 * time.period)) {
      plot <- plot + geom_vline(aes(xintercept = time.period),
                                colour = "purple", alpha = 0.4)
    }
    ggplotly(plot)
  })

  output$instances.plot.queriesPerDollar <- renderPlotly({
    timings <- instance.timings.plot()
    colors <- instance.colors()[timings$id]
    doFlip <- nrow(timings) > 50
    plot <- if (doFlip) {
              ggplot(timings, aes(y = col.recom.inv, label = id, x = reorder(id, col.recom.inv))) +
                coord_flip() +
                scale_y_continuous(position = "right")
            } else {
              ggplot(timings, aes(y = col.recom.inv, label = id, x = reorder(id, -col.recom.inv)))
            }
    plot <- plot +
      geom_bar(fill = colors, position="dodge", stat="identity", alpha = 0.7, width = 0.6) +
      labs(x = "Instance Type", y = "Queries per Dollar") +
      theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.4, size = 12))
    ggplotly(plot, dynamicTicks = TRUE)
  })

  output$instances.plot.queriesPerDollar.ui <- renderUI({
    cnt <- nrow(instance.timings.plot())
    if (cnt > 50) {
      plotlyOutput("instances.plot.queriesPerDollar", height = 20 * cnt)
    } else {
      plotlyOutput("instances.plot.queriesPerDollar")
    }
  })

  ## -------------------------------------------------------------------------------------- ##
                                        # SPECS AND TIMINGS TABLES #
  ## -------------------------------------------------------------------------------------- ##

  view.table.column.instance.type <- function(c, p, r) {
    data.frame(type = c(rep("Recommended", nrow(r)),
                        rep("Frontier", nrow(p)),
                        rep("Comparison", nrow(c))))
  }

  instances.specs.significant <- reactive({
    r <- inst.recommended()
    s <- inst.comparison()
    p <- inst.frontier() %>% dplyr::filter(!(id %in% r$id))
    rbind(r, p, s) %>%
      dplyr::mutate(network.Gbps = ifelse(network.is.steady,
                                          paste(network.Gbps),
                                          paste("<", network.Gbps))) %>%
      dplyr::select(-starts_with("id."), -network.is.steady) %>%
      cbind(view.table.column.instance.type(s, p, r), .)
  })

  instances.table.times.base <- reactive({
    r <- inst.recommended.timings()
    s <- inst.comparison.timings()
    p <- inst.frontier.timings() %>% dplyr::filter(!(id %in% r$id))
    rbind(r, p, s) %>%
      dplyr::select(-id.name, -count) %>%
      cbind(view.table.column.instance.type(s, p, r), .)
  })

  output$instances.messages <- renderTable({
    instances.specs.significant() %>%
      dplyr::select(id, loading.comment) %>%
      dplyr::filter(loading.comment != "")
  })

  output$instances.messages.ui <- renderUI({
    inst <- instances.specs.significant()
    if (any(inst$loading.comment != "")) {
      shiny::wellPanel(
               class = "warning-msg top-margin",
               h3("Warnings"),
               helpText("The following warnings were produced for instances in the tables above."),
               tableOutput("instances.messages")
             )
    } else {
      ""
    }
  })

  output$instances.specs <- renderDT({
    instances.specs.significant() %>%
      dplyr::select(-starts_with("loading.")) %>%
      ui.as.dt.formatted()
  })

  output$instance.specs.ec2instances.info.link <- renderUI({
    region <- instSet.long() %>% head(n = 1) %>% .$meta.region.name
    inst <- instances.specs.significant()$id
    if (length(region) != 0 & length(inst) != 0) {
      uri <- paste(sep="",
                   "https://ec2instances.info/",
                   "?region=", region,
                   "&compare_on=true",
                   "&selected=", paste(inst, collapse=",")
                   )
      shiny::a(href=uri, target="_new",
               class="btn btn-default top-margin",
               "Compare on ec2instances.info")
    } else {
      ""
    }
  })

  output$instances.metadata <- renderDT({
    all <- instSet.long()
    signif <- instances.specs.significant()
    dplyr::inner_join(signif, all, by = c("id")) %>%
      dplyr::select(type, id, starts_with("meta.")) %>%
      ui.as.dt.formatted()
  })

  output$instances.reads.gib <- renderDT({
    instances.table.times.base() %>%
      dplyr::select(type, id, starts_with("read."), starts_with("stat.read")) %>%
      ui.as.dt.formatted()
  })

  output$instances.times <- renderDT({
    instances.table.times.base() %>%
      dplyr::select(type, id, starts_with("time."), starts_with("stat.time.")) %>%
      ui.as.dt.formatted()
  })

  output$instances.prices <- renderDT({
    instances.table.times.base() %>%
      dplyr::select(type, id, cost.usdph, starts_with("stat.price"), starts_with("col.recom.")) %>%
      ui.as.dt.formatted()
  })

  ## -------------------------------------------------------------------------------------- ##
                                        # BUDGET CALCULATOR #
  ## -------------------------------------------------------------------------------------- ##

  timings.plot.budget.limits <- reactive({
    times <- instance.timings()
    col.name <- input$timings.plot.budget.col.cost
    val.min <- min(times[[col.name]])
    val.max <- max(times[[col.name]])
    length <- input$timings.plot.budget.step
    lim <- if(input$timings.plot.budget.limits.logarithmic) {
             exp(seq(if_else(val.min == 0, 0, log(val.min)), if_else(val.max == 0, 0, log(val.max)), length.out = length))
           } else {
             seq(val.min, val.max, length.out = length)
           }
    lim.rounded <- purrr::map_dbl(lim, function(x) signif(x, digits = input$timings.plot.budget.step.digits))
    unique(lim.rounded)
  })

  timings.plot.budget.df <- reactive({
    timings <- instance.timings.all()
    budgets <- timings.plot.budget.limits()
    .col.cost <- input$timings.plot.budget.col.cost
    .col.opti <- input$timings.plot.budget.col.optim

    df <- model.budgets.discrete(
      timings, budgets,
      .cost.fn  = function(df) { df[[.col.cost]] },
      .optim.fn = function(df) { df[[.col.opti]] }
    ) %>%
      dplyr::mutate(
               budget.optim.txt = format(budget.optim, digits = 4),
               budget.cost.txt = paste(" ", id,
                                       "using",
                                       format(budget.cost, digits = 3),
                                       "of",
                                       format(budget.limit, digits = 3)
                                       ),
               color = instance.colors()[id]
             )
    if (input$timings.plot.budget.duplicates.filter) {
      df <- df %>% dplyr::distinct(id, .keep_all = TRUE)
    }
    df
  })

  output$timings.plot.budget <- renderPlotly({
    df <- timings.plot.budget.df()
    plot <- ggplot(df, aes(x=budget.cost, y=budget.optim)) +
      suppressWarnings( # Hack for getting the plotly tooltip to display more values
        geom_point(aes(label = id, limit = budget.limit, description = budget.cost.txt),
                   alpha=0.8, width=0.6, shape = 3, color = df$color)
      ) +
      labs(x = paste("Budget (", input$timings.plot.budget.col.cost, ")"),
           y = paste("Optimized Value (", input$timings.plot.budget.col.optim, ")")) +
      theme(axis.text.x = element_text(hjust = 1, size = 12, angle = 90),
            axis.text.y = element_text(hjust = 1, size = 12))
    if(input$timings.plot.budget.ticks.at.limits) {
      plot <- plot + scale_x_continuous(breaks = df$budget.limit)
    }
    if(input$timings.plot.budget.limits.display) {
      plot <- plot + geom_vline(aes(xintercept=budget.limit), alpha = 0.4, colour = "skyblue")
    }
    plot
  })

  output$timings.plot.budget.ui <- renderUI({
    plotlyOutput("timings.plot.budget")
  })

  ## -------------------------------------------------------------------------------------- ##
                                        # SCALING EFFICIENCY #
  ## -------------------------------------------------------------------------------------- ##

  output$scaling.efficiency.formula <- renderUI({
    formula <- ui.scaling.def$formula
    withMathJax(
      helpText(formula)
    )
  })

  scaling.efficiency.params <- reactive({
    .pdef <- ui.scaling.def$params
    .params <- purrr::lmap(.pdef, function(p) {
      fullname <- paste("scaling.efficiency.param.", names(p))
      result <- list()
      result[[names(p)]] <- input[[fullname]]
      result
    })
    .params
  })

  output$scaling.efficiency.sliders <- renderUI({
    .params <- ui.scaling.def$params

    .inputs <- lapply(names(.params), function(pname) {
      .pdef <- .params[[pname]]

      .value <- .pdef$default
      .min  <- ifelse(is.null(.pdef$min),  0.1,  .pdef$min)
      .max  <- ifelse(is.null(.pdef$max),  1,    .pdef$max)
      .step <- ifelse(is.null(.pdef$step), 0.01, .pdef$step)
      shiny::div(
               sliderInput(paste("scaling.efficiency.param.", pname),
                           paste("Efficiency Parameter:", pname),
                           min=.min, max=.max, value=.value,
                           step=.step),
               withMathJax(helpText(.pdef$description)),
               hr()
             )
    })
    do.call(tagList, .inputs)
  })

  scaling.efficiency.plot.n.colors <- reactive({
    t.comp <- inst.comparison.timings() %>% head(n = 1)
    t.recm <- inst.recommended.timings()
    colors <- rep("black", ui.instance.opts.count.max)
    colors[t.comp$count] = "blue"
    colors[input$comparison.count] = "blue"
    colors[t.recm$count] = "red"
    colors
  })

  output$scaling.efficiency.plot <- renderPlotly({
    .scale.fn <- ui.scaling.def$fn(scaling.efficiency.params())
    .colors <- scaling.efficiency.plot.n.colors()

    ns <- 2:ui.instance.opts.count.max
    df <- data.frame(x = ns, y = 1 / .scale.fn(ns))
    df <- rbind(data.frame(x = 1, y = 1), df)
    df$color <- .colors
    labels <- df[df$color != "black",]
    plot <- ggplot(df, aes(x = x, y = y)) +
      geom_line() +
      geom_point(color = df$color) +
      geom_vline(data = labels, aes(xintercept=x), colour=labels$color, alpha = 0.2) +
      labs(x = "Number of Instances Running", y = "Speedup")
    ggplotly(plot, dynamicTicks = TRUE)
  })

  output$scaling.efficiency.plot.time <- renderPlotly({
    .scale.fn <- ui.scaling.def$fn(scaling.efficiency.params())
    time.rec <- inst.recommended.timings()
    time.com <- inst.comparison.timings()
    time.fro <- inst.frontier.timings() %>%
      dplyr::filter(!(id %in% time.rec$id) & !(id %in% time.com$id)) %>%
      dplyr::filter(!(count %in% time.rec$count) & !(count %in% time.com$count))

    times <- rbind(
      dplyr::mutate(time.rec, group = "Recommended"),
      dplyr::mutate(time.com, group = "Comparison"),
      dplyr::mutate(time.fro, group = "Frontier")
    ) %>%
      dplyr::group_by(group, count) %>%
      dplyr::top_n(-1, wt = stat.time.sum)

    df <- dplyr::mutate(times, x = count, y = stat.time.sum)

    plot <- ggplot(df, aes(x = x, y = y)) +
      geom_line() +
      suppressWarnings(geom_point(aes(color = group, id = id, time = stat.time.sum, cost = stat.price.sum, perf = col.recom.inv))) +
      scale_color_manual(values = c("Recommended" = "red", "Comparison" = "blue", "Frontier" = "green"),
                         aesthetics = c("color")) +
      # geom_vline(aes(xintercept = count, id = id, time = stat.time.sum), color=df$color, alpha = 0.2) +
      labs(
        x = "Number of Instances Running",
        y = "Query Time"
      )
    ggplotly(plot, dynamicTicks = TRUE)
  })

  ## -------------------------------------------------------------------------------------- ##
                                        # HISTORICAL COMPARISON #
  ## -------------------------------------------------------------------------------------- ##

  history.instances.times <- reactive({
    .filter.fn <- ui.instance.filter.fn.get(input$instanceFilter)
    .col.rec <- input$recommendationColumn
    .scale.par <- scaling.efficiency.params()
    .timing.fn <- model.make.timing.fn(distribution.precomputed(),
                                       input$instance.count.max,
                                       ui.scaling.def$fn(.scale.par),
                                       model.distr.split.fn(input$distribution.load.first),
                                       query.time.period.seconds())
    .query <- query.data.frame()
    furrr::future_map_dfr(ui.instance.sets, function(set) {
      filtered <- set %>% .filter.fn() %>% dplyr::filter(complete.cases(.))
      time <- model.calc.costs(query = .query, inst = filtered, timing.fn = .timing.fn)
      joined <- ui.data.timing.enrich(time, .col.rec) %>%
        dplyr::inner_join(set, benchmark,
                          by = c("id.name" = "id"),
                          suffix = c(".from.timing", "")) %>%
        dplyr::mutate(date = readr::parse_date(commit.date, "%Y-%m-%d"))
      return(joined)
    })
  })

  output$history.instances.recommended <- renderPlotly({
    .all.times <- history.instances.times()
    .by <- as.name(input$recommendationColumn)
    .comp.id <- inst.comparison.id()
    .comp.cnt <- input$comparison.count
    recom <- .all.times %>%
      dplyr::group_by(meta.group) %>%
      dplyr::top_n(-1, wt = !! .by) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(color = "red")
    comp <- .all.times %>%
      dplyr::filter(id.name == .comp.id & count == .comp.cnt) %>%
      dplyr::mutate(color = "blue")

    df <- rbind(recom, comp)
    plot <- ggplot(df, aes(x = date, y = col.recom.inv)) +
      suppressWarnings(
        geom_point(aes_all(colnames(ui.instance.sets[[1]])), color = df$color)
      )
    ggplotly(plot)
  })

  ## -------------------------------------------------------------------------------------- ##
                                        # DATASET INFORMATION #
  ## -------------------------------------------------------------------------------------- ##

  output$instSet.metadata <- renderTable({
    all <- instSet.all()
    complete <- all %>% dplyr::filter(complete.cases(.))
    instSet.long() %>%
      head(n = 1) %>%
      dplyr::transmute(
               "Nr." = as.integer(meta.join.entry),
               "Origin" = meta.origin,
               "Region" = meta.region.name,
               "Date" = commit.date,
               "Commit" = commit.hash,
               "Message" = commit.msg,
               "Instance Count" = nrow(all),
               "Complete Cases" = nrow(complete),
               "Modified Cases" = nrow(complete %>% dplyr::filter(loading.comment != ""))
             )
  })

  output$instSet.ids <- renderDT({
    all <- instSet.all()
    all %>%
      dplyr::transmute(
               id = id,
               "Is Complete" = ifelse(complete.cases(.), "Yes", "No"),
               "Modification" = loading.comment
             )
  })

  observeEvent(input$instSet.details.show, {
    showModal(modalDialog(
      title = "Details of the selected instance set",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      h3("Instance Set Metadata"),
      tableOutput("instSet.metadata"),
      h3("Included instances"),
      helpText("The instances included in the selected set before any filtering."),
      shiny::strong(helpText("Instances with incomplete data are always filtered out.")),
      DTOutput("instSet.ids")
    ))
  })

  output$instSet.filtered.ids <- renderDT({
    instSet() %>% dplyr::transmute(id = id)
  })

  observeEvent(input$instFilter.details.show, {
    cnt.all <- nrow(instSet.all() %>% dplyr::filter(complete.cases(.)))
    cnt.filtered <- nrow(instSet())
    showModal(modalDialog(
      title = "Details of the selected filters",
      easyClose = TRUE,
      footer = NULL,
      size = "l",
      shiny::p("Instances included before filtering: ",
               shiny::strong(toString(cnt.all) ),
               shiny::br(),
               "Instances included after filtering: ",
               shiny::strong(toString(cnt.filtered))),
      h3("Included instances"),
      helpText("The instances included in the selected set after filtering using the selected methods."),
      shiny::strong(helpText("Instances with incomplete data are always filtered out.")),
      DTOutput("instSet.filtered.ids")
    ))
  })

  ## -------------------------------------------------------------------------------------- ##
                                        # CONFIGURATION OPTIONS #
  ## -------------------------------------------------------------------------------------- ##

  config.maxvals <- reactive({
    list(
      time.cpu  = input$time.cpu.maxval  %||% 1000,
      data.read = input$data.read.maxval %||% 5000,
      data.xchg = input$data.xchg.maxval %||% 5000
    )
  })

  output$time.cpu.slider <- renderUI({
    sliderInput("time.cpu", "CPUh", min=0, step=1,
                max=config.maxvals()$time.cpu,
                value=20)
  })

  output$data.read.slider <- renderUI({
    sliderInput("data.read", "Sum of Reads", min=ui.instance.opts.count.max, step=10,
                max=config.maxvals()$data.read,
                value=800)
  })

  output$data.xchg.slider <- renderUI({
    sliderInput("data.xchg", "Sum of Exchange Data", min=0, step=10,
                max=config.maxvals()$data.xchg,
                value=200)
  })

  observeEvent(input$config.options.show, {
    maxvals <- config.maxvals()

    showModal(modalDialog(
      title = "Configuration Options",
      easyClose = TRUE,
      footer = shiny::modalButton("Done"),
      size = "l",
      h2("Slider Ranges"),
      helpText("Set the slider ranges for the input configuration values here."),
      shiny::numericInput("time.cpu.maxval", "Max CPU Operations", value=maxvals$time.cpu),
      shiny::numericInput("data.read.maxval", "Max Sum of Reads", value=maxvals$data.read),
      shiny::numericInput("data.xchg.maxval", "Max Exchange Data", value=maxvals$data.xchg)
    ))
  })
}
## ---------------------------------------------------------------------------------------------- ##
                                        # Shiny Client
## ---------------------------------------------------------------------------------------------- ##
client <- function(request) {
  fluidPage(
    tags$head(tags$style(HTML("
            body { padding-top: 10px; }
            .padded-col { padding-top: 2.44rem; }
            .top-margin { margin-top: 20px; }
            .warning-msg { color: #9F6000; background-color: #FEEFB3; }
            .warning-msg:before { content: '⚠️ '; }
            .icon-action-btn { font-size: 1.4em; color: #333; }
            .icon-action-btn:hover {  }
            .shiny-output-error-validation {
                color: green;
                font-weight: bold;
            } ")
            )),
    ## ---------------------------------------------------------------------------------- ##
                                        # HEADER & DATASETS #
    ## ---------------------------------------------------------------------------------- ##
    fluidRow(
      shiny::column(4, uiOutput("app.title")),
      shiny::column(2, selectInput("instanceSet", label = "Use Dataset", names(ui.instance.sets))),
      shiny::column(2, class = "padded-col", shiny::actionButton("instSet.details.show", "Show Instance Set Details")),
      shiny::column(2, selectInput("instanceFilter", label = "Apply Filters", names(ui.instance.filters),
                                   selected = ui.instance.filter.initial, multiple = TRUE)),
      shiny::column(2, class = "padded-col", shiny::actionButton("instFilter.details.show", "Show Filter Details")),
      ),
    sidebarLayout(
      sidebarPanel(
        ## -------------------------------------------------------------------------- ##
                                        # QUERY CONFIGURATION #
        ## -------------------------------------------------------------------------- ##
        shiny::fluidRow(
                 shiny::column(10, h2("Workload Parameters")),
                 shiny::column(2, class="padded-col",
                               shiny::actionLink("config.options.show",
                                                 shiny::icon("cog", lib = "glyphicon"),
                                                 class="icon-action-btn"))
               ),
        helpText("Configure the simulated workload that the optimal instance should be calculated for."),
        hr(),
        uiOutput("time.cpu.slider"),
        helpText("Amount of CPUh required to fulfill this query."),
        hr(),
        uiOutput("data.read.slider"),
        helpText("Sum of Reads [GiB] required to fulfill this query (Mem/SSD/S3)."),
        hr(),
        uiOutput("data.xchg.slider"),
        helpText("Exchange Data [GiB] between instances required to fulfill this query. Is 0 for single instances."),
        hr(),
        sliderInput("locality", "Memory Access Distribution Factor", min=1e-8, max=1 - 1e-8, value=0.1),
        helpText("Zipf distribution factor for simulating read locality. Higher values produce higher locality."),
        hr(),
        checkboxInput("distribution.load.first", "First Read is from S3", value = FALSE),
        helpText("Whether the first read in the access distribution is a read from S3 (vs an optimal read)"),
        hr(),
        ##
        shiny::fluidRow(
                 shiny::column(8,
                               numericInput("time.period.num",
                                            shiny::span("Workload Period (T)", style = "color: purple"),
                                            value = 1, step = 1, min = 1)
                               ),
                 shiny::column(4,
                               selectInput("time.period.unit",
                                           shiny::span("Unit", style = "color: purple"),
                                           selected = "Weeks", choices = names(ui.time.units))
                               )
               ),
        helpText("In which time period",
                 shiny::span("T", style = "color:purple;font-weight:bold;"),
                 "the workload is executed periodically."),
        helpText("Configurations which take longer than",
                 shiny::span("T", style = "color:purple;font-weight:bold;"),
                 "to execute will not be",
                 shiny::span("recommended", style = "color:red;"),
                 "."),
        hr(),
        ##
        h2("Scaling Efficiency"),
        helpText("With Ahmdals law, the overall formula for the scaling of cpu, reads and instance network data is"),
        uiOutput("scaling.efficiency.formula"),
        helpText("Where W is the overall workload, and w is the workload required per instance."),
        helpText("Set the parameters with these sliders:"),
        uiOutput("scaling.efficiency.sliders"),
        hr(),
        ## -------------------------------------------------------------------------- ##
                                        # INSTANCE SELECTION CONFIGURATION #
        ## -------------------------------------------------------------------------- ##
        h2("Instance Parameters"),
        helpText("Configure how the optimal instance setup is to be determined."),
        hr(),
        sliderInput("instance.count.max", "Maximum Instance Count", min=1,
                    max=ui.instance.opts.count.max, value=ui.instance.opts.count.initial),
        helpText("The algorithm will only consider configurations with this number of instances or less."),
        hr(),
        ##
        selectInput("instance.type.opt.include", "Include counts per instance type", ui.instance.opts.type.inclusion),
        helpText("Select which counts for each instance type are included in plot outputs."),
        hr(),
        ##
        selectInput("recommendationColumn",
                    shiny::span("Column to use for", shiny::span("recommendation", style="color:red;")),
                    ui.instance.plot.colnames.recom,
                    selected="stat.price.sum"),
        helpText("The algorithm will recommend the instance with a minimal value in this column."),
        hr(),
        ##
        h3("Save configuration"),
        helpText("Save the configuration as a link that can be bookmarked. You can add personal notes in the text area below."),
        textAreaInput("user.notes", "Personal Notes"),
        bookmarkButton()
      ),
      mainPanel(
        ## -------------------------------------------------------------------------- ##
                                        # RECOMMENDATION HEADER & COMPARISON #
        ## -------------------------------------------------------------------------- ##
        shiny::wellPanel(
                 fluidRow(
                   shiny::column(6,
                                 h3(style = "padding-top:45px; border-right: 1px solid #DDD;",
                                    "The recommended instance is ",
                                    shiny::span(style = "color: red;",
                                                textOutput("instance.recommended.text",
                                                           inline=TRUE)),
                                    shiny::br()
                                    ),
                                 uiOutput("instance.comparison.texts")
                                 ),
                   shiny::column(6,
                                 uiOutput("instance.comparison.selection"),
                                 helpText("Compare the selected instance with the recommended one in plots and tables."),
                                 sliderInput("comparison.count", "Comparison Instance Count",
                                             value=1, min=1, step=1, max=ui.instance.opts.count.initial)
                                 )
                 )
               ),
        ##
        tabsetPanel(
          type = "tabs",
          ## ---------------------------------------------------------------------- ##
                                        # GRAPHS TAB #
          ## ---------------------------------------------------------------------- ##
          tabPanel(
            "Graphs",
            ##
            h3("Instances Plot"),
            helpText("Select the x- and y-Axes. The recommended instance is red, comparison instance is blue."),
            shiny::wellPanel(
                     fluidRow(
                       shiny::column(6,
                                     selectInput("instances.plot.x",
                                                 "Plot X Axis Column",
                                                 ui.instance.plot.colnames,
                                                 selected="stat.time.sum"),
                                     selectInput("instances.plot.y",
                                                 "Plot Y Axis Column",
                                                 ui.instance.plot.colnames,
                                                 selected="col.recom.inv")
                                     ),
                       shiny::column(6,
                                     selectInput("instances.plot.scale.x",
                                                 "Plot X Axis Scale",
                                                 ui.instance.plot.scale),
                                     selectInput("instances.plot.scale.y",
                                                 "Plot Y Axis Scale",
                                                 ui.instance.plot.scale)
                                     )
                     ),
                     fluidRow(
                       shiny::column(6,
                                     checkboxInput("instances.plot.display.frontier",
                                                   shiny::span("Display",
                                                               shiny::span("pareto frontier line", style="color:green;")),
                                                   value=TRUE),
                                     checkboxInput("instances.plot.display.frontier.only",
                                                   shiny::span("Only display points in",
                                                               shiny::span("pareto frontier", style="color:green")),
                                                   value=TRUE)
                                     ),
                       shiny::column(6,
                                     selectInput("instances.plot.frontier.quadrant",
                                                 shiny::span(shiny::span("Frontier", style="color:green;"),
                                                             "Quadrant"),
                                                 ui.instance.plot.pareto.quadrants)
                                     )
                     )
                   ),
            plotlyOutput("instances.plot"),
            helpText("Select an area in the plot to zoom into, double click to reset."),
            hr(),
            ##
            h3("Price vs. Queries per Dollar"),
            helpText("How many queries do you get per dollar for each instance?"),
            uiOutput("instances.plot.queriesPerDollar.ui"),
            hr(),
            ##
            h3("Budget Calculator"),
            shiny::wellPanel(
                     fluidRow(
                       shiny::column(6,
                                     selectInput("timings.plot.budget.col.cost",
                                                 "Cost Function",
                                                 ui.instance.plot.colnames,
                                                 selected="stat.price.sum"),
                                     helpText("Determines where instances are placed on the X axis, e.g what determines the budget.")
                                     ),
                       shiny::column(6,
                                     selectInput("timings.plot.budget.col.optim",
                                                 "Optimization Function",
                                                 ui.instance.plot.colnames,
                                                 selected="stat.time.sum"),
                                     helpText("Determines what is optimized for, e.g the Y Axis.")
                                     )
                     ),
                     fluidRow(
                       shiny::column(6,
                                     numericInput("timings.plot.budget.step",
                                                  "Budget Step Count",
                                                  step=1,
                                                  value=100),
                                     helpText("Into how many slices should the price range be divided to get the budget limits?")
                                     ),
                       shiny::column(6,
                                     numericInput("timings.plot.budget.step.digits",
                                                  "Budget Limits Significant Digits",
                                                  step=1,
                                                  value=3),
                                     helpText("To how many significant digits should the budget steps be rounded?")
                                     ),
                       ),
                     fluidRow(
                       shiny::column(3,
                                     checkboxInput("timings.plot.budget.limits.logarithmic",
                                                   "Logarithmic budget steps",
                                                   value = TRUE)
                                     ),
                       shiny::column(3,
                                     checkboxInput("timings.plot.budget.duplicates.filter",
                                                   "Filter duplicate instance configurations",
                                                   value = TRUE)
                                     ),
                       shiny::column(3,
                                     checkboxInput("timings.plot.budget.limits.display",
                                                   "Display budget limits as vertical lines",
                                                   value = TRUE)
                                     ),
                       shiny::column(3,
                                     checkboxInput("timings.plot.budget.ticks.at.limits",
                                                   "Display axis ticks at budget limits",
                                                   value = TRUE)
                                     ),
                       )
                   ),
            uiOutput("timings.plot.budget.ui")
          ),
          ## ---------------------------------------------------------------------- ##
                                        # SPEC AND TIMING TABLES #
          ## ---------------------------------------------------------------------- ##
          tabPanel(
            "Tables",
            ##
            shiny::fluidRow(
                     shiny::column(4,
                                   h3("Specifications"),
                                   helpText("Specifications of the recommended and comparison instance."),
                                   ),
                     shiny::column(3,
                                   uiOutput("instance.specs.ec2instances.info.link"),
                                   )
                   ),
            DTOutput("instances.specs"),
            hr(),
            ##
            h3("Metadata"),
            helpText("Metadata fields (starting with meta.*) on the recommended and comparison instances."),
            DTOutput("instances.metadata"),
            hr(),
            ##
            h3("Calculated Read Operations"),
            helpText("Calculated GiB reads of the recommended and comparison instance."),
            DTOutput("instances.reads.gib"),
            hr(),
            ##
            h3("Calculated Times"),
            helpText("Calculated Times of the recommended and comparison instance."),
            DTOutput("instances.times"),
            hr(),
            ##
            h3("Calculated Prices"),
            helpText("Calculated Prices of the recommended and comparison instance."),
            DTOutput("instances.prices"),
            ##
            uiOutput("instances.messages.ui")
          ),
          ## ---------------------------------------------------------------------- ##
                                        # READ ACCESS DISTRIBUTION #
          ## ---------------------------------------------------------------------- ##
          tabPanel(
            "Access Distribution",
            style = "padding-top: 10px",
            ##
            h3("Recommended Instance"),
            helpText("Memory Access Distribution for the recommended instance."),
            plotlyOutput("distributionPlotRecommended"),
            hr(),
            ##
            shiny::fluidRow(
                     shiny::column(6,
                                   h3("Comparison Instance"),
                                   helpText("Memory Access Distribution for the comparison instance.")
                                   ),
                     shiny::column(6, uiOutput("distribution.comparison.msg"))
                   ),
            plotlyOutput("distributionPlotComparison")
          ),
          ## ---------------------------------------------------------------------- ##
                                        # SCALING EFFICIENCY GRAPHS #
          ## ---------------------------------------------------------------------- ##
          tabPanel(
            "Scaling Efficiency",
            h3("Speedup"),
            plotlyOutput("scaling.efficiency.plot"),
            ##
            h3("Best time per instance count"),
            plotlyOutput("scaling.efficiency.plot.time"),
          ),
          ## ---------------------------------------------------------------------- ##
                                        # HISTORICAL COMPARISON #
          ## ---------------------------------------------------------------------- ##
          tabPanel(
            "Historical Comparison",
            h3("Recommended Instances"),
            helpText("Recommended Instances in each dataset"),
            plotlyOutput("history.instances.recommended")
          )
        )
      )
    )
  )
}


## ---------------------------------------------------------------------------------------------- ##
                                        # Run Program
## ---------------------------------------------------------------------------------------------- ##


ui.run <- function() {
  ## library(profvis)
  ## profvis({
  runApp(
    shinyApp(client, server, enableBookmarking = "url",
             options = list(
               host = "0.0.0.0",
               port = 3030
             ))
  )
  ##})
}


ui.run()
