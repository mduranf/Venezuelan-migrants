# ========================================================
# Author: Maria Juliana Duran
# Date: June 2025
# Goal: This code processes all of the modules separately 
# ========================================================

# -------- SET UP
# ========================================================
library(pacman)
p_load(purrr, readr, tidyverse, janitor, stringr, data.table, arrow, tools)

path_generales <- list.files(path = "~/Documents/git/Venezuelan-migrants/cleaning/input/generales",
                             full.names = T)

path_hogares <- list.files(path = "~/Documents/git/Venezuelan-migrants/cleaning/input/hogar_vivienda", 
                           full.names = T)

path_migracion <- list.files(path = "~/Documents/git/Venezuelan-migrants/cleaning/input/migracion", 
                             full.names = T)

path_fuerza <- list.files(path = "~/Documents/git/Venezuelan-migrants/cleaning/input/fuerza_trabajo",
                          full.names = T)

path_ocupados <- list.files(path = "~/Documents/git/Venezuelan-migrants/cleaning/input/ocupados",
                            full.names = T)

path_desocupados <- list.files(path = "~/Documents/git/Venezuelan-migrants/cleaning/input/desocupados",
                               full.names = T)

path_inactivos <- list.files(path = "~/Documents/git/Venezuelan-migrants/cleaning/input/inactivos",
                             full.names = T)

path_otras <- list.files(path = "~/Documents/git/Venezuelan-migrants/cleaning/input/otras_actividades",
                         full.names = T)

path_otro_ingreso <- list.files(path = "~/Documents/git/Venezuelan-migrants/cleaning/input/otro_ingreso",
                                full.names = T)

path_mpft <- list.files(path = "~/Documents/git/Venezuelan-migrants/cleaning/input/mpft",
                                full.names = T)

path_infantil <- list.files(path = "~/Documents/git/Venezuelan-migrants/cleaning/input/infantil",
                            full.names = T)

# -------- 1. FUNCTIONS
# ========================================================

detect_delim <- function(path) {
  first_line <- readLines(path, n = 1, warn = FALSE)
  commas <- stringr::str_count(first_line, ",")
  semicolons <- stringr::str_count(first_line, ";")
  
  if (semicolons > commas) {
    return(";")
  } else {
    return(",")
  }
}

read_data_generales <- function(path) {
  year <- str_extract(path, "\\d{4}") %>% as.numeric()
  month <- str_extract(path, "enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre")
  
  # Exception: whitespace-delimited file
  if (str_detect(basename(path), "septiembre_2021")) {
    data <- read_table(path, show_col_types = FALSE) %>% clean_names()
    
  } else {
    # Auto-detect delimiter
    delim <- detect_delim(path)
    
    if (delim == ",") {
      data <- read_csv(path, locale = locale(encoding = "Latin1")) %>% clean_names()
    } else {
      data <- read_delim(path, delim = ";", locale = locale(encoding = "Latin1"), escape_double = FALSE) %>% clean_names()
    }
  }
  
  # Clean data
  data <- data %>%
    mutate(across(where(is.character), ~ as.numeric(.))) %>%
    mutate(year = year, month = month)
  
  return(data)
}

read_data_migracion <- function(path) {
  year <- str_extract(path, "\\d{4}") %>% as.numeric()
  month <- str_extract(path, "enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre")
  
  if (year == 2021 && month %in% c("julio", "agosto", "septiembre", "octubre", "noviembre")) {
    data <- read_csv(path)[, -1] %>% clean_names()
  } else if (year == 2021 && month == "diciembre") {
    data <- read_csv(path) %>% clean_names()
  } else {
    data <- read_delim(path, delim = ";", escape_double = FALSE) %>% clean_names()
  }
  
  # Data cleaning
  data <- data %>%
    mutate(across(everything(), ~ ifelse(. == ".", NA, .))) %>%
    mutate(across(everything(), ~ as.numeric(.))) %>%
    mutate(year = year)
  
  return(data)
}

filter_dpto <- function(input_path, 
                        output_path = "~/Documents/git/Venezuelan-migrants/cleaning/output_bogota",
                        dpto_value = 11) {
  
  parquet_files <- list.files(input_path, pattern = "\\.parquet$", full.names = TRUE)
  
  
  for (file in parquet_files) {
    cat("Processing:", file, "\n")
    
    file_name <- basename(file)
    out_file <- file.path(output_path, file_name)
    
    tryCatch({
      df <- read_parquet(file)
      
      if (!"dpto" %in% names(df)) {
        warning(paste("Skipping", file_name, "- 'dpto' column not found"))
      } else {
        filtered <- df %>% filter(dpto == dpto_value)
        write_parquet(filtered, out_file)
        cat("Saved:", new_name, "\n")
      }
    }, error = function(e) {
      warning(paste("Failed to process", file_name, ":", e$message))
    })
  }
}


# -------- 2. GENERALES
# ========================================================
generales <- purrr::map_dfr(path_generales, read_data_generales)
prueba <- generales %>% filter(is.na(directorio)) %>% select(year, ano, month)

generales <- generales %>% 
  select(directorio, secuencia_p, orden, year, dpto, hogar, p6040,p6081,
         p6083, p6090, p6100, p6110, p6160, p6170, p3271, p3042, p3038, p3039,
         discapacidad, esc, p6020, p6210) %>% 
  rename(vivienda = directorio,
         hogar2 = secuencia_p,
         persona = orden, 
         edad = p6040,
         padre_reside = p6081,
         madre_reside = p6083,
         afiliado_seguridad_s = p6090,
         cual_regimen = p6100,
         quien_paga = p6110,
         sabe_leer_escribir = p6160,
         asiste_educacion = p6170,
         sexo1 = p3271,
         sexo2 = p6020,
         ultimo_nivel_educativo1 = p3042,
         ultimo_nivel_educativo2 = p6210,
         orientacion_sexual = p3038, 
         genero = p3039,
         escolaridad = esc)

generales <- generales %>%
  mutate(hogar = if_else(!is.na(hogar), hogar, hogar2),
         sexo = if_else(!is.na(sexo1), sexo1, sexo2),
         ultimo_nivel_educativo = if_else(!is.na(ultimo_nivel_educativo1), ultimo_nivel_educativo1, ultimo_nivel_educativo2),
         id = as.numeric(str_c(vivienda, hogar, persona))) %>% 
  select(vivienda, hogar, persona, id, year, dpto, hogar, discapacidad, edad, sexo, padre_reside,
         madre_reside, afiliado_seguridad_s, cual_regimen, quien_paga, sabe_leer_escribir, 
         asiste_educacion, ultimo_nivel_educativo, orientacion_sexual, genero, escolaridad)

write_parquet(generales, "~/Documents/git/Venezuelan-migrants/cleaning/output/generales.parquet")


# -------- 3. HOGARES
# ========================================================
hogares <- purrr::map_dfr(path_hogares, read_data_generales)
prueba <- hogares %>% filter(is.na(directorio)) %>% select(year, ano, month)

hogares <- hogares %>% 
  select(directorio, secuencia_p, orden, year, dpto, p4000, p70, p5090,
         p5090s1, p5222s1, p5222s2, p5222s3, p5222s4, p5222s5, p5222s6, p5222s7, 
         p5222s8, p5222s9, p5222s10) %>% 
  rename(vivienda = directorio,
         hogar = secuencia_p,
         persona = orden, 
         tipo_vivienda = p4000,
         cuantos_hogares = p70, 
         propia_vivienda = p5090, 
         propia_cual = p5090s1, 
         corriente = p5222s1, 
         ahorros = p5222s2, 
         cdt = p5222s3, 
         prestamo_vivienda = p5222s4, 
         prestamo_vehiculo = p5222s5, 
         prestamo_libre = p5222s6, 
         tarjeta_credito = p5222s7, 
         otro = p5222s8, 
         ningun_prestamo = p5222s9, 
         no_sabe = p5222s10) %>% 
  filter(!is.na(persona))

hogares <- hogares %>% 
  mutate(id = as.numeric(str_c(vivienda, hogar, persona)))

write_parquet(hogares, "~/Documents/git/Venezuelan-migrants/cleaning/output/hogares.parquet")

# - Observaciones datos 
# 1. Instrumentos financieros solo se incluyeron en 2022.  

# -------- 4. MIGRACION
# ========================================================
migration <- purrr::map_dfr(path_migracion, read_data_migracion)

migration <- migration %>% 
  rename(vivienda = directorio,
         hogar1 = secuencia_p,
         hogar2 = hogar,
         persona = orden,
         siempre_ha_vivido = p6074,
         donde_nacio1 = p756,
         nacio_otro_dpto1 = p756s1,
         nacio_otro_pais1 = p756s3,
         donde_5_years1 = p755,
         vivia5_otro_dpto1 = p755s1,
         vivia5_otro_pais1 = p755s3,
         vivia5_cabecera1 = p754,
         donde_1_year1 = p753,
         vivia1_otro_dpto1 = p753s1,
         vivia1_otro_pais1 = p753s3, 
         vivia1_cabecera1 = p752,
         motivo1 = p1662, 
         donde_nacio2 = p3373,
         nacio_otro_dpto2 = p3373s1,
         nacio_otro_pais2 = p3373s3,
         year_llego_col = p3373s3a1,
         mes_llego_col = p3373s3a2,
         nacionalidad = p3374,
         pais_nacionalidad1 = p3374s1,
         pais_nacionalidad2 = p3374s2,
         year_volvio_col = p3381,
         donde_5_years2 = p3382,
         vivia5_otro_dpto2 = p3382s1,
         vivia5_otro_pais2 = p3382s3,
         vivia5_cabecera2 = p3383,
         donde_1_year2 = p3384,
         vivia1_otro_dpto2 = p3384s1,
         vivia1_otro_pais2 = p3384s3, 
         vivia1_cabecera2 = p3385,
         motivo2 = p3386,
         intencion_quedarse = p3375,
         cuantos_meses_quedarse = p3375s1) %>% 
  select(vivienda, hogar1, hogar2, persona, year, dpto, siempre_ha_vivido, donde_nacio1, 
         donde_nacio2, nacionalidad, nacio_otro_dpto1, nacio_otro_dpto2, nacio_otro_pais1, 
         nacio_otro_pais2, donde_5_years1, donde_5_years2, vivia5_otro_dpto1, 
         vivia5_otro_dpto2, vivia5_otro_pais1, vivia5_otro_pais2, vivia5_cabecera2,
         vivia5_cabecera1, donde_1_year1, donde_1_year2, vivia1_otro_dpto1, vivia1_otro_dpto2, 
         vivia1_otro_pais1, vivia1_otro_pais2, vivia1_cabecera1, vivia1_cabecera2, 
         motivo1, motivo2, year_llego_col, mes_llego_col, pais_nacionalidad1, pais_nacionalidad2, 
         year_volvio_col, motivo1, motivo2, intencion_quedarse, cuantos_meses_quedarse)

migration <- migration %>%
  mutate(hogar = if_else(!is.na(hogar1), hogar1, hogar2),
         id = as.numeric(str_c(vivienda, hogar, persona)),
         donde_nacio = if_else(!is.na(donde_nacio1), donde_nacio1, donde_nacio2),
         nacio_otro_dpto = if_else(!is.na(nacio_otro_dpto1), nacio_otro_dpto1, nacio_otro_dpto2),
         nacio_otro_pais = if_else(!is.na(nacio_otro_pais1), nacio_otro_pais1, nacio_otro_pais2),
         donde_5_years = if_else(!is.na(donde_5_years1), donde_5_years1, donde_5_years2),
         vivia5_otro_dpto = if_else(!is.na(vivia5_otro_dpto1), vivia5_otro_dpto1, vivia5_otro_dpto2),
         vivia5_otro_pais = if_else(!is.na(vivia5_otro_pais1), vivia5_otro_pais1, vivia5_otro_pais2),
         vivia5_cabecera = if_else(!is.na(vivia5_cabecera1), vivia5_cabecera1, vivia5_cabecera2),
         donde_1_year = if_else(!is.na(donde_1_year1), donde_1_year1, donde_1_year2),
         vivia1_otro_dpto = if_else(!is.na(vivia1_otro_dpto1), vivia1_otro_dpto1, vivia1_otro_dpto2),
         vivia1_otro_pais = if_else(!is.na(vivia1_otro_pais1), vivia1_otro_pais1, vivia1_otro_pais2),
         vivia1_cabecera = if_else(!is.na(vivia1_cabecera1), vivia1_cabecera1, vivia1_cabecera2),
         motivo = if_else(!is.na(motivo1), motivo1, motivo2),
         nacio_otro_pais = if_else(nacio_otro_pais == 862, 3, nacio_otro_pais)) %>% 
  select(id, year, dpto, siempre_ha_vivido, donde_nacio, nacio_otro_dpto, 
         nacio_otro_pais, nacionalidad, donde_5_years, vivia5_otro_dpto, 
         vivia5_otro_pais, vivia5_cabecera, donde_1_year, vivia1_otro_dpto, 
         vivia1_otro_pais, vivia1_cabecera, motivo, year_llego_col, mes_llego_col,
         pais_nacionalidad1, pais_nacionalidad2, year_volvio_col, intencion_quedarse, 
         cuantos_meses_quedarse)

write_parquet(migration, "~/Documents/git/Venezuelan-migrants/cleaning/output/migration.parquet")

# - Observaciones datos 
# 1. Nacionalidad solo se incluyó en el 2022. 
# 2. Siempre ha vivido se quitó en el 2022. 

# -------- 5. FUERZA LABORAL
# ========================================================
fuerza <- purrr::map_dfr(path_fuerza, read_data_generales)
prueba <- fuerza %>% filter(is.na(directorio)) %>% select(year, ano, month)

fuerza <- fuerza %>% 
  select(directorio, secuencia_p, orden, hogar, year, dpto,
         p6240, p6250, p6300, p6320, p6340, p3362s1, p3362s2, p3362s3, 
         p3362s4, p3362s5, p3362s6, p3362s7, p6290, p6240s2) %>% 
  rename(vivienda = directorio,
         hogar2 = secuencia_p,
         persona = orden,
         que_hizo_conseguir_trabajo = p6290,
         que_actividad_mayor_parte = p6240,
         no_trabajo_pero_ingresos_negocio = p6250,
         desea_trabajo = p6300,
         minimo_2_semanas_ultimo_year = p6320,
         diligencia_ultimo_year = p6340,
         ayuda_familiares = p3362s1, 
         visito_cv_empresas = p3362s2, 
         agencia_empleo = p3362s3, 
         clasificados = p3362s4, 
         convocatorias = p3362s5, 
         preparativos_negocio = p3362s6, 
         otro_trabajo_informal = p3362s7,
         recibe_remuneracion = p6240s2) %>% 
  mutate(hogar = if_else(!is.na(hogar), hogar, hogar2),
         id = as.numeric(str_c(vivienda, hogar, persona))) %>% 
  select(-hogar2)

write_parquet(fuerza, "~/Documents/git/Venezuelan-migrants/cleaning/output/fuerza.parquet")

# -------- 6. OCUPADOS
# ========================================================
ocupados <- purrr::map_dfr(path_ocupados, read_data_generales)
prueba <- ocupados %>% filter(is.na(directorio)) %>% select(year, ano, month)

ocupados <- ocupados %>% 
  select(directorio, secuencia_p, orden, hogar, year, dpto,
         p6440, p6450, p6422, p6424s1, p6765, p6780, p6790, p1879,
         p1805, p6880, p6915, p6920, p6930, p6940, p6960, p6990,
         p760, p7130, p7140s4, p7240, inglabo, p6410s1, p6424s5,
         p6480, p6980, p6980s1, p6980s2, p6980s3, p6980s4, p6980s5, 
         p6980s6, p6980s7, p6980s8,p6772) %>% 
  rename(vivienda = directorio,
         hogar2 = secuencia_p,
         persona = orden,
         tiene_contrato = p6440,
         verbal_escrtio = p6450, 
         esta_conforme_contrato = p6422,
         vacaciones_sueldo = p6424s1,
         formas_trabajo = p6765,
         trabajo_tipo = p6780,
         meses_trabajados_ultimo_year = p6790, 
         por_que_independiente = p1879, 
         aceptaria_asalariado = p1805,
         donde_realiza_trabajo = p6880,
         costos_enfermedad = p6915,
         cotizando_pensiones = p6920, 
         regimen_pension = p6930,
         quien_paga_pension = p6940,
         years_afiliado = p6960,
         aseguradora_riesgos_prof = p6990, 
         meses_entre_trabajo = p760, 
         desea_cambiar_trabajo = p7130, 
         por_que_temporal = p7140s4, 
         de_donde_recursos_sin_trabajo = p7240, 
         ingresos_laborales = inglabo,
         licencia_enfermedad = p6424s5,
         vacaciones_sueldo2 = p6410s1, 
         por_que_medio_trabajo = p6480,
         que_esta_haciendo_vejez= p6980,
         pensiones_obligatorias_vejez = p6980s1,
         pensiones_voluntarias_vejez = p6980s2,
         ahorrando_vejez = p6980s3,
         inversiones_vejez = p6980s4,
         seguro_vejez = p6980s5, 
         hijos_vejez = p6980s6, 
         nada_vejez = p6980s8,
         negocio_regustrado_independiente = p6772) %>% 
  mutate(hogar = if_else(!is.na(hogar), hogar, hogar2),
         vacaciones_sueldo = if_else(!is.na(vacaciones_sueldo), vacaciones_sueldo, vacaciones_sueldo2),
         id = as.numeric(str_c(vivienda, hogar, persona))) %>%  
  select(-hogar2, -vacaciones_sueldo2)

write_parquet(ocupados, "~/Documents/git/Venezuelan-migrants/cleaning/output/ocupados.parquet")


# -------- 7. DESOCUPADOS
# ========================================================
desocupados <- purrr::map_dfr(path_desocupados, read_data_generales)
prueba <- desocupados %>% filter(is.na(directorio)) %>% select(year, ano, month)

desocupados <- desocupados %>% 
  select(directorio, secuencia_p, orden, hogar, year, dpto,
         p7250, p7280, p7310, p7320, p7350, p9460, p7390, p7420s1,
         p7420s2, p7420s3, p7420s4, p7420s5, p7420s6, p7420s8,
         p7422, p1519, oficio1, oficio2, p744, p7440, p1883) %>% 
  rename(vivienda = directorio,
         hogar2 = secuencia_p,
         persona = orden,
         horas_buscando_trabajo = p7250,
         buscado_trabajo_como = p7280,
         buscado_trabajo_2semanas = p7310,
         hace_cuanto_no_trabaja = p7320,
         ultimo_trabajo_era = p7350,
         subsidio_desempleo = p9460, 
         como_enfermedad_desocupado = p7390,
         aportar_pension_obligatoria_desocupado = p7420s1,
         aportar_pension_voluntaria_desocupado = p7420s2,
         ahorrando_vejez_desocupado = p7420s3,
         inversiones_vejez_desocupado = p7420s4,
         seguro_vejez_desocupado = p7420s5, 
         hijos_vejez_desocupado = p7420s6, 
         nada_vejez_desocupado = p7420s8,
         recibio_ingresos_desocupados = p7422,
         cotizando_fondo_pensiones_desocupados = p1519, 
         oficio_trabajo1 = oficio1,
         oficio_trabajo2 = oficio2, 
         hace_cuanto_no_trabaja_inactivos = p744,
         hace_cuanto_no_trabaja_inactivos2 = p7440, 
         tipo_fondo_pensiones_desocupados = p1883) %>% 
  mutate(hogar = if_else(!is.na(hogar), hogar, hogar2),
         id = as.numeric(str_c(vivienda, hogar, persona))) %>%
  select(-hogar2)

write_parquet(desocupados, "~/Documents/git/Venezuelan-migrants/cleaning/output/desocupados.parquet")


# -------- 8. INACTIVOS
# ========================================================
inactivos <- purrr::map_dfr(path_inactivos, read_data_generales)
prueba <- inactivos %>% filter(is.na(directorio)) %>% select(year, ano, month)


inactivos <- inactivos %>% 
  select(directorio, secuencia_p, orden, hogar, year, dpto,
         p7440, p7460, p7470, p6921) %>% 
  rename(vivienda = directorio,
         hogar2 = secuencia_p,
         persona = orden,
         hace_cuanto_no_trabaja_inactivos = p7440,
         cotizando_fondo_pensiones_inactivos = p7460, 
         tipo_fondo_pensiones_inactivos = p7470,
         cotizando_fondo_pensiones_inactivos2 = p6921) %>% 
  mutate(hogar = if_else(!is.na(hogar), hogar, hogar2),
         id = as.numeric(str_c(vivienda, hogar, persona))) %>%
  select(-hogar2)

write_parquet(inactivos, "~/Documents/git/Venezuelan-migrants/cleaning/output/inactivos.parquet")

# -------- 9. OTRAS ACTIVIDADES
# ========================================================
otras <- purrr::map_dfr(path_otras, read_data_generales)
prueba <- otras %>% filter(is.na(directorio)) %>% select(year, ano, month)


otras <- otras %>% 
  select(directorio, secuencia_p, orden, hogar, year, dpto,
         p7480s8) %>% 
  rename(vivienda = directorio,
         hogar2 = secuencia_p,
         persona = orden,
         asistir_cursos = p7480s8) %>% 
  mutate(hogar = if_else(!is.na(hogar), hogar, hogar2),
         id = as.numeric(str_c(vivienda, hogar, persona))) %>%
  select(-hogar2)

write_parquet(otras, "~/Documents/git/Venezuelan-migrants/cleaning/output/otras.parquet")


# -------- 10. OTRO INGRESO
# ========================================================
otro_ingreso <- purrr::map_dfr(path_otro_ingreso, read_data_generales)
prueba <- otro_ingreso %>% filter(is.na(directorio)) %>% select(year, ano, month)

otro_ingreso <- otro_ingreso %>% 
  select(directorio, secuencia_p, orden, hogar, year, dpto,
         p7510s1, p7510s2, p7510s3, p7510s3a1, p7510s2a1, p7510s1a1,
         p750s2, p750s2a1, p1661s1, p1661s1a1, p1661s2, p1661s2a1,
         p1661s3, p1661s3a1, p750s3, p750s3a1) %>% 
  rename(vivienda = directorio,
         hogar2 = secuencia_p,
         persona = orden,
         dinero_otra_persona_pais = p7510s1,
         dinero_otra_persona_exterior = p7510s2,
         dinero_institutiones_pais = p7510s3, 
         valor_instutiones_pais = p7510s3a1,
         valor_otra_exterior = p7510s2a1, 
         valor_otra_pais = p7510s1a1,
         entidades_gobierno = p750s2,
         valor_entidades_gobierno = p750s2a1,
         familias_accion = p1661s1,
         valor_familias_accion = p1661s1a1,
         jovenes_accion = p1661s2,
         valor_jovenes_accion = p1661s2a1, 
         colombia_mayor = p1661s3, 
         valor_colombia_mayor = p1661s3a1,
         entidades_fuera_pais = p750s3,
         valor_entidades_fuera_pais = p750s3a1) %>% 
  mutate(hogar = if_else(!is.na(hogar), hogar, hogar2),
         id = as.numeric(str_c(vivienda, hogar, persona))) %>%
  select(-hogar2)

write_parquet(otro_ingreso, "~/Documents/git/Venezuelan-migrants/cleaning/output/otro_ingreso.parquet")

# -------- 11. FORMACION PARA EL TRABAJO
# ========================================================
formacion <- purrr::map_dfr(path_mpft, read_data_generales)
prueba <- formacion %>% filter(is.na(directorio)) %>% select(year, ano, month)

formacion <- formacion %>% 
  select(directorio, secuencia_p, orden, year, area,
         p455, p456, p758, p465, p464, p467, p468, p469) %>% 
  rename(vivienda = directorio,
         hogar = secuencia_p,
         persona = orden,
         dpto = area,
         asistir_cursos_mpft = p455,
         ultimos2y_cursos = p456, 
         quien_pago = p758,
         objetivo_curso = p464,
         que_ha_permitido_curso = p465,
         razon_tomar_curso = p467,
         tiene_pensao_curso_12meses = p468,
         certificado_competencia_ultimos2 = p469) 

formacion <- formacion %>% 
  mutate(id = as.numeric(str_c(vivienda, hogar, persona)))

write_parquet(formacion, "~/Documents/git/Venezuelan-migrants/cleaning/output/formacion.parquet")


# -------- 12. TRABAJO INFANTIL
# ========================================================
infantil <- purrr::map_dfr(path_infantil, read_data_generales)
prueba <- infantil %>% filter(is.na(directorio)) %>% select(year)

infantil <- infantil %>% 
  select(directorio, secuencia_p, orden, year, area,
         p400, p405) %>% 
  rename(vivienda = directorio,
         hogar = secuencia_p,
         persona = orden,
         dpto = area,
         actividad_mayor_semana_pasada = p400, 
         por_que_trabaja = p405)

infantil <- infantil %>% 
  mutate(id = as.numeric(str_c(vivienda, hogar, persona)))

write_parquet(infantil, "~/Documents/git/Venezuelan-migrants/cleaning/output/infantil.parquet")

# -------- 13. SAVE FOR MEMORY OPTIMIZATION
# ========================================================

filter_dpto(input_path = "~/Documents/git/Venezuelan-migrants/cleaning/output")








