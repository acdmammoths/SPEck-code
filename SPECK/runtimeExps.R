args = commandArgs(trailingOnly=TRUE)
simulatedData = FALSE
if (length(args) > 0) {
  if(args[1] == "-sim") {
    simulatedData = TRUE
  }
}

library(ggplot2)
library(tidyverse)


options(scipen = 999)

colors = c("\u03B5-AUS"="#828282", "EUS"="#cfcfcf")

runtimes <- read.csv("results/csv/runtimes.csv")%>%
  filter(type == "generation")%>%
  mutate(model = case_when(strategy %in% c("itemsetsSwaps", "completePerm") ~ 1,
                           strategy %in% c("sameSizeSwaps", "sameSizePerm") ~ 2,
                           TRUE ~ 0))

if (simulatedData) {
  runtimes <- runtimes %>%
    filter(str_detect(dataset, "sim_")) %>%
    mutate(dataset = numTransactions)
} else {
  runtimes <- runtimes %>%
    filter(!str_detect(dataset, "sim_"))
}

#get relative runtimes for strategies with same null model
#faster / slower
relative <- runtimes%>%
  group_by(dataset, strategy, model)%>%
  summarize(base_runtime = median(time.ns.))%>%
  ungroup()%>%
  filter(strategy %in% c("itemsetsSwaps", "sameSizeSwaps", "sameFreqSwaps")) %>%
  select(-strategy)%>%
  inner_join(runtimes, by = c("dataset", "model"))%>%
  mutate(relative = time.ns. / base_runtime)

f <- function(x) {
  r <- quantile(x, probs = c(0.00, 0.25, 0.5, 0.75, 1.00))
  names(r) <- c("ymin", "lower", "middle", "upper", "ymax")
  r
}

vals <<- list()
vals$nm <- 0

labeller <- function(variable, value) {
  d <- runtimes%>%
    group_by(dataset, strategy, model)%>%
    summarize(base_runtime = round(median(time.ns.)/1000000, 2))%>%
    ungroup() %>%
    filter(strategy == vals$nm, dataset %in% value)
  str <- paste0(value, "\n(", d$base_runtime," ms)")
  str
}
relativeFigure <- function(strategies, out) {
  vals$nm <<- strategies[1]
  
  nm = case_when(
    strategies[1] == "completePerm"~1,
    strategies[1] == "itemsetsSwaps"~1,
    strategies[1] == "sameSizePerm"~2,
    strategies[1] == "sameSizeSwaps"~2,
    strategies[1] == "sameFreqSwaps"~3
  )
  runtimesPlot <- relative %>%
    filter(strategy %in% strategies)%>%
    mutate(strategy = case_when(
      strategy == "sameSizePerm" ~ "EUS",
      strategy == "completePerm" ~ "EUS",
      TRUE ~ "\u03B5-AUS",
    )) %>%
    ggplot(mapping = aes(x = strategy, y = relative, color = strategy))+
    stat_boxplot(geom='errorbar',coef=10000, width = 0.3) +
    stat_summary(fun.data=f, geom="boxplot", lwd = .8) +
    facet_wrap(~dataset, ncol = length(unique(relative$dataset)), 
               strip.position = "bottom", labeller = labeller)+
    labs(y = "Relative Runtime", color="Method")+
    scale_colour_manual(values = colors) +
    ylim(0,2)+ #omit outlier in SIGN 
    theme(axis.text.x = element_blank(),
          axis.ticks.x = element_blank(),
          axis.title.x = element_blank(),
          legend.position = c(.05, .87),
          panel.background = element_blank(),
          panel.grid.major.y = element_line(color = "grey"),
          panel.grid.minor.y= element_blank(),
          panel.grid.major.x = element_blank(),
          panel.grid.minor.x = element_blank(),
          strip.background = element_blank(),
          legend.key = element_blank(),
          legend.direction = "horizontal",
          legend.justification = "left",
          legend.background = element_blank(),
          plot.title = element_text(hjust = 0.5),
          text = element_text(size = 15))
  ggsave(out, plot = runtimesPlot, path = "./plots", width = 20, height = 10, units="cm")
  runtimesPlot
}

ribbonGraph <- function(strategies, out) {
  vals$nm <- strategies[1]
  nm = case_when(
    strategies[1] == "completePerm"~1,
    strategies[1] == "itemsetsSwaps"~1,
    strategies[1] == "sameSizePerm"~2,
    strategies[1] == "sameSizeSwaps"~2,
    strategies[1] == "sameFreqSwaps"~3
  )
  #tick marks on x axis
  
  mx <- as.vector(unique(runtimes$numItemsets))
  
  ribbon_data <- runtimes %>%
    rename(time = "time.ns.")%>% #time is in nanos
    mutate(time = time/1000000)%>%
    group_by(dataset, strategy, numItemsets, numTransactions)%>%
    summarize(min = min(time), mean = mean(time), median = median(time), max = max(time)) %>%
    filter(strategy %in% strategies) %>%
    ungroup(strategy)%>%
    mutate(strategy = case_when(
      strategy == "sameSizePerm" ~ "EUS",
      strategy == "completePerm" ~ "EUS",
      TRUE ~ "\u03B5-AUS",
    ))
  
  ribbonPlot<- ggplot(ribbon_data,
                      mapping = aes(x = numItemsets, y = median, 
                                    ymin = min, ymax = max, fill = strategy))+
    geom_ribbon()+
    geom_line(aes(lty = ""))+
    scale_linetype_manual(values = c("dashed")) +
    geom_point(aes(x=numItemsets, y=min), size=1) +
    #geom_point(aes(x=numItemsets, y=median), size=1) +
    geom_point(aes(x=numItemsets, y=max), size=1) +
    scale_x_continuous(breaks = mx, trans = "log10")+
    scale_y_continuous(trans = "log10") + 
    labs(x = expression(paste("Number ", italic("m")," of itemsets (log scale)")), 
         y = "Runtime (ms) (log scale)" ,
         fill = "Method",
         lty="Median") +
    scale_fill_manual(values = colors) +
    theme(axis.text.x = element_text(angle = 60, vjust = 0.5, hjust = 0.5),
          text = element_text(size = 15),
          panel.background = element_blank(),
          panel.grid.major.y = element_line(color="gray"),
          legend.position = c(.03, .81),
          legend.key = element_blank(),
          legend.direction="horizontal",
          legend.justification = "left",
          legend.background = element_blank(),
          legend.spacing.y = unit(.05, 'cm'),
          legend.text = element_text(size=13),
          plot.title = element_text(hjust=0.5),
          axis.line = element_line(color="black"))
  ggsave(out, plot = ribbonPlot, path = "./plots", width = 20, height = 10, units="cm")
  ribbonPlot
}

if (simulatedData) {
  same_size_ribbon <- ribbonGraph(c("sameSizePerm", "sameSizeSwaps"), "sameSizeRuntimesRibbonSim.png")
  perm_swaps_ribbon <- ribbonGraph(c("completePerm", "itemsetsSwaps"), 
                                   "permSwapsRuntimesRibbonSim.png")
  same_freq_ribbon <- ribbonGraph(c("sameFreqSwaps"), "sameFreqRuntimesRibbonSim.png")
  
} else {
  perm_swaps_rel <- relativeFigure(c("completePerm", "itemsetsSwaps"), "permSwapsRuntimesRel.png")
  same_size_rel <- relativeFigure(c("sameSizePerm", "sameSizeSwaps"), "sameSizeRuntimesRel.png")
  same_freq_rel <- relativeFigure(c("sameFreqSwaps"), "sameFreqRuntimesRel.png")
}


