# ========================================================
# Author: Maria Juliana Duran
# Date: June 2025
# Goal: This code execute descriptive statistics
# ========================================================

# -------- SET UP
# ========================================================
library(pacman)
p_load(purrr, tidyverse, arrow, hrbrthemes, ggplot2)

geih <- read_parquet("~/Documents/git/Venezuelan-migrants/merge/output/geih_bogota.parquet")
output_path <- "~/Documents/git/Venezuelan-migrants/descriptive_statistics/output/"

# -------- 1. Grafica temporal de llegada de venezolanos
# ========================================================

migrantes <- geih %>% filter(nacionalidad == "Venezuelan")

migrantes_year <- migrantes %>% 
  group_by(year_llego_col) %>% 
  summarise(n = n()) %>% 
  mutate(year_llego_col = ifelse(year_llego_col == 98, 1998, year_llego_col))

migrantes_year %>%
  filter(year_llego_col > 1999) %>% 
  ggplot(aes(x = year_llego_col, y = n)) +
  geom_line(color="#8DB6CD", linewidth = 0.85) +
  scale_x_continuous(breaks = seq(2000, 2024, by = 2)) +
  scale_y_continuous(breaks = seq(0, 1900, by = 250)) +
  labs(title = "Venezuelan migration to Bogotá",
       subtitle = "1965-2024",
       x = "Year",
       y = "Number of inmigrants") +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 20),
        plot.subtitle = element_text(hjust = 0.5, size = 17),
        axis.title = element_text(size = 15),
        axis.text = element_text(size = 13),
        panel.background = element_blank(),
        panel.grid.major = element_line(color = "grey95", linewidth = 0.5))

ggsave(paste0(output_path,"migration_years.jpeg"), width = 15, height = 11)

# -------- 2. Grafica comparación de edad
# ========================================================

data_piramide <- geih %>%
  filter(nacionalidad != "otro") %>% 
  mutate(grupo_edad = cut(edad, breaks = seq(0, 80, by = 5), right = FALSE)) %>%
  group_by(nacionalidad, grupo_edad) %>%
  summarise(n = n(), .groups = "drop") %>%
  group_by(nacionalidad) %>%
  mutate(pct = n / sum(n) * 100) %>%
  ungroup() %>%
  mutate(pct = ifelse(nacionalidad == "Venezuelan", pct, -pct)) %>% 
  filter(!is.na(grupo_edad))

ggplot(data_piramide, aes(x = grupo_edad, y = pct, fill = nacionalidad)) +
  geom_bar(stat = "identity", width = 0.95, color = "white") +
  coord_flip() +
  scale_y_continuous(labels = abs) +
  scale_fill_manual(values = c("Venezuelan" = "#6B8E23", "Colombian" = "#3288bd")) +
  labs(title = "Age structure comparison between Venezuelan migrants and Colombians",
       subtitle = "Percentage within each nationality group",
       x = "Age group",
       y = "Percentage (%)",
       fill = "Nacionality") +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 20),
        plot.subtitle = element_text(hjust = 0.5, size = 17),
        axis.title = element_text(size = 15),
        axis.text = element_text(size = 14),
        legend.title = element_text(size = 15),
        legend.text = element_text(size = 14),
        legend.position = "bottom")

ggsave(paste0(output_path,"age_groups_comparison.jpeg"), width = 15, height = 11)

# -------- 3. Informalidad/sexo/nacionalidad
# ========================================================

informalidad_grouped <- geih %>%
  group_by(nacionalidad, sexo) %>%
  summarise(total = n(),
            informales = sum(informal == 1, na.rm = TRUE),
            tasa_informalidad = informales / total * 100) %>% 
  filter(nacionalidad != "otro") %>% 
  mutate(sexo = case_when(
    sexo == 1 ~ "Male",
    sexo == 2 ~ "Female",
    TRUE ~ as.character(sexo)))

ggplot(informalidad_grouped, aes(x = sexo, y = tasa_informalidad, fill = nacionalidad)) +
  geom_bar(stat = "identity", position = position_dodge(width = 0.6), width = 0.5, , color = NA) +
  scale_fill_manual(values = c("Venezuelan" = "#6B8E23", "Colombian" = "#3288bd")) +
  labs(title = "Informality rate by sex and nationality",
       x = "Sex",
       y = "Informality rate (%)",
       fill = "Nationality") +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 20),
        plot.subtitle = element_text(hjust = 0.5, size = 17),
        axis.title = element_text(size = 17),
        axis.text = element_text(size = 16),
        legend.title = element_text(size = 16),
        legend.text = element_text(size = 15),
        legend.position = "bottom")

ggsave(paste0(output_path,"informality_sex.jpeg"), width = 15, height = 11)

# -------- 4. Informalidad/regularización
# ========================================================

regularizados <- geih %>% 
  filter(!is.na(regularizado)) %>% 
  group_by(regularizado) %>%
  summarise(total = n(),
            informales = sum(informal == 1, na.rm = TRUE),
            tasa_informalidad = informales / total * 100) %>% 
  mutate(regularizado = case_when(
    regularizado == 1 ~ "Regularized",
    regularizado == 0 ~ "Not Regularized",
    TRUE ~ as.character(regularizado)))

ggplot(regularizados, aes(x = regularizado, y = tasa_informalidad, fill = regularizado)) +
  geom_bar(stat = "identity", position = position_dodge(width = 0.6), width = 0.5, , color = NA) +
  geom_text(aes(label = paste0(round(tasa_informalidad, 1), "%")), 
            vjust = -0.5, size = 5, color = "black") +
  scale_fill_manual(values = c("Regularized" = "#CDB5CD", "Not Regularized" = "#CD5555")) +
  labs(title = "Informality rate by migration status",
       x = "Migration status",
       y = "Informality rate (%)",
       fill = "Status") +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 20),
        plot.subtitle = element_text(hjust = 0.5, size = 17),
        axis.title = element_text(size = 17),
        axis.text = element_text(size = 16),
        legend.title = element_text(size = 16),
        legend.text = element_text(size = 15),
        legend.position = "none")

ggsave(paste0(output_path,"informality_regularized.jpeg"), width = 15, height = 11)

