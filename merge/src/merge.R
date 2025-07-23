# ========================================================
# Author: Maria Juliana Duran
# Date: June 2025
# Goal: This code merges all modules and tests the merge 
# ========================================================

# -------- SET UP
# ========================================================
library(pacman)
p_load(purrr, readr, tidyverse, janitor, stringr, arrow, tools)

files <- list.files("~/Documents/git/Venezuelan-migrants/cleaning/output_bogota", full.names = T)

# -------- 1. FUNCTIONS
# ========================================================

strip_suffixes <- function(name) {
  str_remove(name, "(\\.x|\\.y|\\.x\\.x|\\.y\\.y|\\.x\\.x\\.x|\\.y\\.y\\.y|\\.x\\.x\\.x\\.x|\\.y\\.y\\.y\\.y)+$")
}

read_data <- function(path) {
  
  df <- read_parquet(path) %>% 
    select(-dpto)
  
}

check_conflict_free <- function(data, base) {
  cols <- names(data)[str_detect(names(data), paste0("^", base, "(\\.|$)"))]
  
  if (length(cols) < 2) return(TRUE)
  
  cols_data <- map(cols, ~ as.character(data[[.x]]))
  
  has_conflict <- pmap_lgl(cols_data, function(...) {
    vals <- c(...)
    vals <- vals[!is.na(vals)]
    length(unique(vals)) > 1
  })
  
  !any(has_conflict)
}

unify_columns <- function(data, base) {
  cols <- names(data)[str_detect(names(data), paste0("^", base, "(\\.|$)"))]
  
  if (!(base %in% cols) && base %in% names(data)) {
    cols <- c(base, cols)
  }
  
  data %>%
    mutate("{base}" := coalesce(!!!syms(cols))) %>% 
    select(-setdiff(cols, base)) 
}

# -------- 2. READ DATA
# ========================================================

datasets <- set_names(map(files, read_data),
                      file_path_sans_ext(basename(files)))

df <- datasets[names(datasets) != "hogares"]
hogares <- datasets[["hogares"]]

hogares_persona <- hogares %>% filter(!is.na(persona))
hogares_no_persona <- hogares %>% filter(is.na(persona)) %>% 
  select(-c(id,persona))

# -------- 3. MERGE PERSONAS
# ========================================================

personas_merged <- reduce(df,
                          ~ full_join(.x, .y, by = c("id", "year")))

personas_merged <- personas_merged %>%
  left_join(hogares_persona, by = c("id", "year"))

# -------- 4. TESTS PERSONAS
# ========================================================

# 1. Identify duplicates
base_vars <- names(personas_merged) %>%
  tibble(original = .) %>%
  mutate(base = strip_suffixes(original)) %>%
  group_by(base) %>%
  filter(n() > 1) %>%
  summarise(vars = list(original), .groups = "drop")

# 2. Compare row to row
comparison_results <- base_vars %>%
  mutate(are_all_identical = map_lgl(vars, function(cols) {
    cols_data <- map(cols, ~ as.character(personas_merged[[.x]]))
    first_col <- cols_data[[1]]
    all(map_lgl(cols_data[-1], ~ all(coalesce(.x, "__NA__") == coalesce(first_col, "__NA__"))))
  }))

comparison_results

# 3. Test if they have conflicts
results <- base_vars %>%
  mutate(no_conflict = map_lgl(base, ~ check_conflict_free(personas_merged, .x)))

results

no_conflict <- results %>%
  filter(no_conflict) %>%
  pull(base)

for (base in no_conflict) {
  personas_merged <- unify_columns(personas_merged, base)
}

# -------- 3. MERGE HOGARES
# ========================================================
hogar_vars <- c("tipo_vivienda", "cuantos_hogares", "propia_vivienda", "propia_cual",
                "corriente", "ahorros", "cdt", "prestamo_vivienda", "prestamo_vehiculo",
                "prestamo_libre", "tarjeta_credito", "otro", "ningun_prestamo", "no_sabe")

missing_household <- personas_merged %>%
  filter(if_all(all_of(hogar_vars), is.na))

recovered <- missing_household %>%
  select(-all_of(hogar_vars)) %>%
  left_join(hogares_no_persona, by = c("vivienda", "hogar", "year"))

successful_merge <- personas_merged %>%
  filter(!if_all(all_of(hogar_vars), is.na))

final <- bind_rows(personas_merged, recovered) %>% 
  select(id, vivienda, hogar, persona, year, nacio_otro_pais, nacionalidad, 
         siempre_ha_vivido, donde_nacio, nacio_otro_dpto, year_llego_col,
         everything())

final <- final %>% 
  mutate(nacionalidad = ifelse(is.na(nacio_otro_pais), "Colombian", NA),
         nacionalidad = ifelse(is.na(nacionalidad) & nacio_otro_pais == 3, "Venezuelan", nacionalidad),
         nacionalidad = ifelse(is.na(nacionalidad), "otro", nacionalidad),
         informal = ifelse(tiene_contrato == 2, 1, NA),
         informal = ifelse(tiene_contrato == 1 & quien_paga_pension == 2, 1, informal),
         informal = ifelse(tiene_contrato == 1 & quien_paga_pension == 4, 1, informal),
         informal = ifelse(cotizando_pensiones == 2, 1, informal),
         informal = ifelse(afiliado_seguridad_s == 2, 1, informal),
         informal = ifelse(is.na(informal) & recibe_remuneracion == 2, 1, informal),
         informal = ifelse(is.na(informal) & otro_trabajo_informal == 1, 1, informal),
         informal = ifelse(is.na(informal) & aseguradora_riesgos_prof == 2, 1, informal),
         informal = ifelse(is.na(informal) & donde_realiza_trabajo < 6, 1, informal),
         informal = ifelse(is.na(informal) & negocio_regustrado_independiente != 1, 1, informal),
         informal = ifelse(is.na(informal) & oficio_trabajo1 < 99, 1, informal),
         informal = ifelse(is.na(informal) & oficio_trabajo2 < 99, 1, informal),
         informal = ifelse(is.na(informal) & otro_trabajo_informal == 1, 1, informal),
         informal = ifelse(is.na(informal) & tiene_contrato == 2, 1, informal),
         informal = ifelse(is.na(informal) & tiene_contrato == 1 & verbal_escrtio == 1, 1, informal),
         informal = ifelse(is.na(informal) & formas_trabajo == 6, 1, informal),
         informal = ifelse(is.na(informal) & trabajo_tipo == 1, 1, informal),
         informal = ifelse(is.na(informal) & !is.na(por_que_independiente), 1, informal),
         informal = ifelse(is.na(informal), 0, informal),
         regularizado = ifelse(year_llego_col >= 2021, 0, NA),
         regularizado = ifelse(year_llego_col >= 2017 & year_llego_col < 2021, 1, regularizado))

final <- final %>% select(id, vivienda, hogar, persona, year, nacionalidad, edad, sexo, year_llego_col, informal, regularizado, everything())

write_parquet(final, "~/Documents/git/Venezuelan-migrants/merge/output/geih_bogota.parquet")



