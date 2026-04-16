import pandas as pd
from pandera.errors import SchemaErrors
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
from src.validation.rejects import save_rejects
from src.config.settings import REJECTS_PATH

# -------------------------
# PIPELINE STAGES
# -------------------------

def transform(df):
    df = df.copy()

    df = normalize_columns(df)
    df = rename_columns(df)

    missing = [col for col in CRITICAL_COLUMNS if col not in df.columns]

    if missing:
      raise ValueError(f"Missing critical columns: {missing}")

    df = cast_types(df)

    df["competencia_mov_partition"] = df["competencia_mov"].dt.strftime("%Y-%m")

    original_len = len(df)

    df, failure_cases = validate_with_rejects(df)

    print(f"[INFO] Rows removed: {original_len - len(df)}")

    if failure_cases is not None:
      save_rejects(failure_cases, REJECTS_PATH)

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

    if df["competencia_mov"].isna().all():
        raise ValueError("Critical failure: competencia_mov is entirely null")

    report_quality(df)

    return df

def validate_with_rejects(df):
    try:
        schema.validate(df, lazy=True)

    except SchemaErrors as e:
        print("[WARNING] Schema validation issues detected")

        failure_df = e.failure_cases

        invalid_idx = failure_df["index"].unique()
        df = df.drop(index=invalid_idx)

        return df, failure_df

    for col in CRITICAL_COLUMNS:
        null_idx = df[df[col].isna()].index
        if len(null_idx) > 0:
            print(f"[WARNING] Dropping {len(null_idx)} rows due to nulls in {col}")
            df = df.drop(index=null_idx)

    return df, None

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
          print(f"[INFO] {col}: {pct}% (already enforced upstream)")

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

