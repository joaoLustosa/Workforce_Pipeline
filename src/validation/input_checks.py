def validate_input_columns(df):
    expected = {
    "competenciamov",
    "regiao",
    "uf",
    "municipio",
    "secao",
    "subclasse",
    "saldomovimentacao",
    "categoria",
    "cbo2002ocupacao",
    "graudeinstrucao",
    "idade",
    "horascontratuais",
    "racacor",
    "sexo",
    "tipoempregador",
    "tipoestabelecimento",
    "tipomovimentacao",
    "tipodedeficiencia",
    "indtrabintermitente",
    "indtrabparcial",
    "salario",
    "tamestabjan",
    "indicadoraprendiz",
    "origemdainformacao",
    "competenciadec",
    "competenciaexc",
    "indicadordeexclusao",
    "indicadordeforadoprazo",
    "unidadesalariocodigo",
    "valorsalariofixo"
    }

    missing = expected - set(df.columns)

    if missing:
        print(f"[WARNING] Missing columns: {missing}")
