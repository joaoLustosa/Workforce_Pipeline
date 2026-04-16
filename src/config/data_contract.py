CRITICAL_COLUMNS = [
    "competencia_mov",
    "uf",
    "saldo_movimentacao"
]

IMPORTANT_COLUMNS = [
    "salario",
    "idade",
    "horas_contratuais"
]

OPTIONAL_COLUMNS = [
    "competencia_exc",
    "indicador_de_exclusao",
    "ind_trab_parcial"
]

COLUMN_THRESHOLDS = {
    "saldo_movimentacao": 0.0,   # must be complete
    "competencia_mov": 0.0,
    "uf": 0.0,

    "salario": 10.0,             # tolerate some missing
    "idade": 5.0,
    "horas_contratuais": 10.0,
}
