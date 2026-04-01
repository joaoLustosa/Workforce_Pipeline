import pandera.pandas as pa
from pandera import Column, Check

schema = pa.DataFrameSchema(
    {
        # Dates
        "competencia_mov": Column(pa.DateTime, nullable=True),
        "competencia_dec": Column(pa.DateTime, nullable=True),
        "competencia_exc": Column(pa.DateTime, nullable=True),

        # Location / identifiers
        "regiao": Column(pa.Int64, nullable=True),
        "uf": Column(pa.Int64, nullable=True),
        "municipio": Column(pa.Int64, nullable=True),

        # CNAE
        "secao": Column(pa.String, nullable=True),
        "subclasse": Column(pa.Int64, nullable=True),

        # Movement
        "saldo_movimentacao": Column(pa.Int64, nullable=True),
        "tipo_movimentacao": Column(pa.Int64, nullable=True),

        # Worker classification
        "categoria": Column(pa.Int64, nullable=True),
        "cbo2002_ocupacao": Column(pa.Int64, nullable=True),
        "grau_de_instrucao": Column(pa.Int64, nullable=True),

        # Demographics
        "idade": Column(pa.Int64, Check.in_range(14, 100), nullable=True),
        "raca_cor": Column(pa.Int64, nullable=True),
        "sexo": Column(pa.Int64, nullable=True),

        # Employer
        "tipo_empregador": Column(pa.Int64, nullable=True),
        "tipo_estabelecimento": Column(pa.Int64, nullable=True),
        "tamestabjan": Column(pa.Int64, nullable=True),

        # Flags
        "ind_trab_intermitente": Column(pa.Bool, nullable=True),
        "ind_trab_parcial": Column(pa.Bool, nullable=True),
        "indicador_aprendiz": Column(pa.Bool, nullable=True),
        "indicador_de_exclusao": Column(pa.Bool, nullable=True),
        "indicador_fora_do_prazo": Column(pa.Bool, nullable=True),

        # Disability
        "tipo_deficiencia": Column(pa.Int64, nullable=True),

        # Origin
        "origem_da_informacao": Column(pa.Int64, nullable=True),

        # Salary
        "salario": Column(pa.Float, Check.ge(0), nullable=True),
        "valor_salario_fixo": Column(pa.Float, Check.ge(0), nullable=True),
        "unidade_salario_codigo": Column(pa.Int64, nullable=True),

        # Hours
        "horas_contratuais": Column(pa.Float, Check.in_range(0, 100), nullable=True),
    },
    checks=[
        Check(
            lambda df: (
                df["competencia_exc"].isna()
                | df["competencia_mov"].isna()
                | (df["competencia_exc"] >= df["competencia_mov"])
            )
        )
    ],
    strict=False,
    ordered=True
)
