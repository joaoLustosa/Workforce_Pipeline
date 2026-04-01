import pandas as pd
from src.validation.input_checks import validate_input_columns
from src.validation.schema import schema
from src.transformation.cleaning import normalize_columns
from src.transformation.parsing import (
    parse_br_float,
    parse_bool,
    parse_yyyymm,
    parse_int
)

def transform(df):
  df = df.copy()

  # Validate and normalize input
  df = normalize_columns(df)
  validate_input_columns(df)

  # Transform
  df = rename_columns(df)

  apply_dates(df)
  apply_ints(df)
  apply_floats(df)
  apply_bools(df)
  apply_if_exists(df, "secao", lambda s: s.astype("string"))

  # Enforce schema contract
  schema.validate(df, lazy=True)

  return df

# Apply conversions
def rename_columns(df):
  rename_map = {
    "competenciamov": "competencia_mov",
    "regiao": "regiao",
    "uf": "uf",
    "municipio": "municipio",
    "seçao": "secao",
    "subclasse": "subclasse",
    "saldomovimentaçao": "saldo_movimentacao",
    "categoria": "categoria",
    "cbo2002ocupaçao": "cbo2002_ocupacao",
    "graudeinstruçao": "grau_de_instrucao",
    "idade": "idade",
    "horascontratuais": "horas_contratuais",
    "raçacor": "raca_cor",
    "sexo": "sexo",
    "tipoempregador": "tipo_empregador",
    "tipoestabelecimento": "tipo_estabelecimento",
    "tipomovimentaçao": "tipo_movimentacao",
    "tipodedeficiencia": "tipo_deficiencia",
    "indtrabintermitente": "ind_trab_intermitente",
    "indtrabparcial": "ind_trab_parcial",
    "salario": "salario",
    "tamestabjan": "tamestabjan",
    "indicadoraprendiz": "indicador_aprendiz",
    "origemdainformação": "origem_da_informacao",
    "competenciadec": "competencia_dec",
    "competenciaexc": "competencia_exc",
    "indicadordeexclusao": "indicador_de_exclusao",
    "indicadordeforadoprazo": "indicador_fora_do_prazo",
    "unidadesalariocódigo": "unidade_salario_codigo",
    "valorsalariofixo": "valor_salario_fixo"
  }

  return df.rename(columns=rename_map)

def apply_dates(df):
    date_cols = [
        "competencia_mov",
        "competencia_dec",
        "competencia_exc"
    ]
    for col in date_cols:
        apply_if_exists(df, col, parse_yyyymm)

def apply_ints(df):
    int_cols = [
        "regiao", "uf", "municipio", "subclasse", "saldo_movimentacao",
        "categoria", "cbo2002_ocupacao", "grau_de_instrucao", "idade",
        "raca_cor", "sexo", "tipo_empregador", "tipo_estabelecimento",
        "tipo_movimentacao", "tipo_deficiencia", "tamestabjan",
        "origem_da_informacao", "unidade_salario_codigo"
    ]
    for col in int_cols:
        apply_if_exists(df, col, parse_int)

def apply_floats(df):
    float_cols = [
        "salario",
        "valor_salario_fixo",
        "horas_contratuais"
    ]
    for col in float_cols:
        apply_if_exists(df, col, parse_br_float)

def apply_bools(df):
    bool_cols = [
        "ind_trab_intermitente",
        "ind_trab_parcial",
        "indicador_aprendiz",
        "indicador_de_exclusao",
        "indicador_fora_do_prazo"
    ]
    for col in bool_cols:
        apply_if_exists(df, col, parse_bool)

def apply_if_exists(df, col, func):
    if col in df.columns:
        df[col] = func(df[col])
