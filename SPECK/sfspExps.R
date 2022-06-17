args = commandArgs(trailingOnly=TRUE)

if (length(args) == 1) {
  outfile = paste0("plots/", args[1])
} else {
  outfile = "plots/sfspTable.csv"
}

packages <- c("tidyverse")
new_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
if(length(new.packages) > 0) install.packages(new_packages)
lapply(packages, require, character.only = TRUE)

sfsp <- read.csv("results/csv/sfspData.csv")

table <- sfsp%>%
  rename(time = time.ms.)%>%
  group_by(dataset, strategy, theta, p, t, procs)%>%
  summarize(FSP = first(numFsp), SFSP = median(numSfsp))%>%
  mutate(FSP = ifelse(FSP == "unknown", "-", FSP))%>%
  ungroup()%>%
  select(dataset, strategy, FSP, SFSP)
write.csv(table, outfile)
