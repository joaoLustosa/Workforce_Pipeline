def aggregate_employment_by_uf(df):
    return (
        df.groupby(
            ["competencia_mov", "competencia_mov_partition", "uf"],
            as_index=False
        )
        .agg(saldo_movimentacao_total=("saldo_movimentacao", "sum"))
    )
