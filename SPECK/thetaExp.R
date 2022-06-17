library(ggplot2)
library(tidyverse)
data <- read.csv("./SPECK/results/csv/thetaExperiments.csv")



p <- data %>%
  mutate(time = time/ 1000,
         strategy = ifelse(strategy=="completePerm",
                           "Null model #1 - EUS",
                           "Null model #2 - EUS")) %>%
  ggplot(mapping = aes(x =theta, y = time, group = theta)) +
  stat_boxplot(geom="errorbar", coef=1000, lwd =.8) +
  geom_boxplot(outlier.alpha = 0, lwd=.8) +
  facet_wrap(~strategy) +
  labs(x = expression(paste("Minimum support threshold ", theta)),
       y = "Runtime (Seconds)") +
  theme(
        text = element_text(size = 15),
        panel.background = element_blank(),
        panel.grid.major.x = element_blank(),
        panel.grid.major.y = element_line(color="gray"),
        legend.position = c(.03, .81),
        legend.key = element_blank(),
        legend.direction="horizontal",
        legend.justification = "left",
        legend.background = element_blank(),
        legend.spacing.y = unit(.05, 'cm'),
        legend.text = element_text(size=13),
        plot.title = element_text(hjust=0.5))
p
ggsave("thetaExps.png", plot = p, path = "./SPECK/plots", width = 20, height = 10, units="cm")


