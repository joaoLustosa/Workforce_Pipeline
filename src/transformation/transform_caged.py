import pandas as pd
from src.validation.schema import schema
from src.transformation.cleaning import normalize_columns
from src.transformation.parsing import (
    parse_br_float,
    parse_bool,
    parse_yyyymm,
    parse_int
)
from src.config.data_contract import (
    CRITICAL_COLUMNS,
    IMPORTANT_COLUMNS,
    OPTIONAL_COLUMNS,
    COLUMN_THRESHOLDS
)

# -------------------------
# PIPELINE STAGES
# -------------------------

def transform(df):
    df = df.copy()

    df = normalize_columns(df)
    df = rename_columns(df)

    REQUIRED_COLUMNS = [
    "competencia_mov",
    "uf",
    "saldo_movimentacao"
    ]

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]

    if missing:
      raise ValueError(f"Missing critical columns: {missing}")

    df = cast_types(df)

    df["competencia_mov_partition"] = df["competencia_mov"].dt.strftime("%Y-%m")

    df = validate(df)

    return df


# -------------------------
# TRANSFORM STEPS
# -------------------------

def cast_types(df):
    apply_dates(df)
    apply_ints(df)
    apply_floats(df)
    apply_bools(df)
    apply_if_exists(df, "secao", lambda s: s.astype("string"))
    return df


def validate(df):
    try:
        schema.validate(df, lazy=True)
    except Exception as e:
        print("[WARNING] Data validation issues detected")
        print(e)

    if df["competencia_mov"].isna().all():
        raise ValueError("Critical failure: competencia_mov is entirely null")

    report_quality(df)

    return df

def report_quality(df):
    print("\n[DATA QUALITY REPORT]")

    total_rows = len(df)
    null_percent = (df.isna().sum() / total_rows * 100).round(2)

    errors = []

    for col, pct in null_percent.items():
        if pct == 0:
            continue

        threshold = COLUMN_THRESHOLDS.get(col)

        if col in CRITICAL_COLUMNS:
            print(f"[CRITICAL] {col}: {pct}% nulls")
            if pct > 0:
                errors.append(f"{col} has nulls but is critical")

        elif threshold is not None:
            if pct > threshold:
                print(f"[FAIL] {col}: {pct}% > {threshold}%")
                errors.append(f"{col} exceeds threshold")
            else:
                print(f"[WARNING] {col}: {pct}%")

        elif col in IMPORTANT_COLUMNS:
            print(f"[WARNING] {col}: {pct}%")

        else:
            print(f"[INFO] {col}: {pct}%")

    if errors:
        raise ValueError(f"Data quality check failed: {errors}")

# -------------------------
# RENAMING
# -------------------------

def rename_columns(df):
    rename_map = {
        "competenciamov": "competencia_mov",
        "regiao": "regiao",
        "uf": "uf",
        "municipio": "municipio",
        "secao": "secao",
        "subclasse": "subclasse",
        "saldomovimentacao": "saldo_movimentacao",
        "categoria": "categoria",
        "cbo2002ocupacao": "cbo2002_ocupacao",
        "graudeinstrucao": "grau_de_instrucao",
        "idade": "idade",
        "horascontratuais": "horas_contratuais",
        "racacor": "raca_cor",
        "sexo": "sexo",
        "tipoempregador": "tipo_empregador",
        "tipoestabelecimento": "tipo_estabelecimento",
        "tipomovimentacao": "tipo_movimentacao",
        "tipodedeficiencia": "tipo_deficiencia",
        "indtrabintermitente": "ind_trab_intermitente",
        "indtrabparcial": "ind_trab_parcial",
        "salario": "salario",
        "tamestabjan": "tamestabjan",
        "indicadoraprendiz": "indicador_aprendiz",
        "origemdainformacao": "origem_da_informacao",
        "competenciadec": "competencia_dec",
        "competenciaexc": "competencia_exc",
        "indicadordeexclusao": "indicador_de_exclusao",
        "indicadordeforadoprazo": "indicador_fora_do_prazo",
        "unidadesalariocodigo": "unidade_salario_codigo",
        "valorsalariofixo": "valor_salario_fixo"
    }

    return df.rename(columns=rename_map)


# -------------------------
# TYPE APPLICATIONS
# -------------------------

def apply_dates(df):
    date_cols = ["competencia_mov", "competencia_dec", "competencia_exc"]
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
    float_cols = ["salario", "valor_salario_fixo", "horas_contratuais"]
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


# -------------------------
# SCHEMA SHAPE ALIGNMENT
# -------------------------

def ensure_schema_columns(df):
    dtype_map = {
        "DateTime": "datetime64[ns]",
        "Int64": "Int64",
        "Float": "float64",
        "String": "string",
        "Bool": "boolean",
    }

    for col, col_schema in schema.columns.items():
        if col not in df.columns:
            pa_dtype = type(col_schema.dtype).__name__
            pd_dtype = dtype_map.get(pa_dtype, "object")
            df[col] = pd.Series([pd.NA] * len(df), dtype=pd_dtype)

    return df
