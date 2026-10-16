# -*- coding: utf-8 -*-
"""
Carga otimizada da base de CNPJ da Receita Federal em PostgreSQL (somente estabelecimentos ATIVOS),
pensada para consulta de CNPJ e mineração de leads.

Principais otimizações em relação à versão anterior:
 - lê os .zip em streaming (não descompacta em disco);
 - filtra durante a leitura: só situação cadastral 02 (ativa) e só empresas/sócios/simples desses CNPJs
   (um bitmap de 12,5 MB indica quais cnpj_basico são ativos);
 - COPY nativo do Postgres (ENCODING LATIN1), em paralelo por arquivo;
 - schema tipado (DATE, NUMERIC, SMALLINT, CHAR, text[]), PKs e índices criados só no final;
 - tabela desnormalizada `lead` pronta para filtros, busca por nome e geração de embeddings;
 - troca atômica de schema: a base antiga continua no ar até a nova estar completa.

Conexão por variáveis de ambiente padrão do libpq (nada de senha no código):
    export PGHOST=... PGPORT=5432 PGDATABASE=... PGUSER=... PGPASSWORD=... PGSSLMODE=require

Uso:
    python dados_cnpj_postgres.py [--pasta-zip dados-publicos-zip] [--schema cnpj] [--workers 4] [--yes]
"""
import argparse, glob, os, re, sys, time, zipfile
from concurrent.futures import ProcessPoolExecutor
from datetime import date

import psycopg

BITMAP_TAM = 100_000_000 // 8 + 1  # um bit por cnpj_basico (8 dígitos)
TAM_LOTE = 8 * 1024 * 1024         # bytes por escrita no COPY

# Estabelecimento ativo: situação cadastral é o 6º campo (todos entre aspas, nome_fantasia pode ter ';')
RE_ATIVO = re.compile(rb'^"[^"]*";"[^"]*";"[^"]*";"[^"]*";"(?:[^"]|"")*";"02";')

def log(*a):
    print(time.strftime('%H:%M:%S'), *a, flush=True)

#%% DDL ---------------------------------------------------------------------------------------------

SQL_FUNCOES = '''
CREATE FUNCTION parse_data(x text) RETURNS date LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE WHEN x ~ '^[12][0-9]{7}$' AND pg_input_is_valid(x, 'date') THEN x::date END $$;

CREATE FUNCTION sem_acento(x text) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT lower(translate(x,
    'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
    'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn')) $$;
'''

SQL_STAGING = '''
CREATE UNLOGGED TABLE stg_estabelecimento (
  cnpj_basico text, cnpj_ordem text, cnpj_dv text, matriz_filial text, nome_fantasia text,
  situacao_cadastral text, data_situacao_cadastral text, motivo_situacao_cadastral text,
  nome_cidade_exterior text, pais text, data_inicio_atividades text, cnae_fiscal text,
  cnae_fiscal_secundaria text, tipo_logradouro text, logradouro text, numero text, complemento text,
  bairro text, cep text, uf text, municipio text, ddd1 text, telefone1 text, ddd2 text, telefone2 text,
  ddd_fax text, fax text, correio_eletronico text, situacao_especial text, data_situacao_especial text);
CREATE UNLOGGED TABLE stg_empresa (
  cnpj_basico text, razao_social text, natureza_juridica text, qualificacao_responsavel text,
  capital_social text, porte_empresa text, ente_federativo_responsavel text);
CREATE UNLOGGED TABLE stg_socio (
  cnpj_basico text, identificador_de_socio text, nome_socio text, cnpj_cpf_socio text,
  qualificacao_socio text, data_entrada_sociedade text, pais text, representante_legal text,
  nome_representante text, qualificacao_representante_legal text, faixa_etaria text);
CREATE UNLOGGED TABLE stg_simples (
  cnpj_basico text, opcao_simples text, data_opcao_simples text, data_exclusao_simples text,
  opcao_mei text, data_opcao_mei text, data_exclusao_mei text);
'''

SQL_TABELAS = '''
CREATE TABLE cnae (codigo char(7) PRIMARY KEY, descricao text NOT NULL);
CREATE TABLE motivo (codigo char(2) PRIMARY KEY, descricao text NOT NULL);
CREATE TABLE municipio (codigo char(4) PRIMARY KEY, descricao text NOT NULL);
CREATE TABLE natureza_juridica (codigo char(4) PRIMARY KEY, descricao text NOT NULL);
CREATE TABLE pais (codigo char(3) PRIMARY KEY, descricao text NOT NULL);
CREATE TABLE qualificacao_socio (codigo char(2) PRIMARY KEY, descricao text NOT NULL);

CREATE TABLE empresa (
  cnpj_basico char(8), razao_social text NOT NULL, natureza_juridica char(4),
  qualificacao_responsavel char(2), capital_social numeric(18,2), porte smallint,
  ente_federativo_responsavel text);
CREATE TABLE estabelecimento (
  cnpj char(14), cnpj_basico char(8) NOT NULL, matriz boolean NOT NULL, nome_fantasia text,
  situacao_cadastral smallint, data_situacao_cadastral date, motivo_situacao_cadastral char(2),
  nome_cidade_exterior text, pais char(3), data_inicio_atividades date,
  cnae_fiscal char(7), cnae_secundarias text[],
  tipo_logradouro text, logradouro text, numero text, complemento text, bairro text,
  cep char(8), uf char(2), municipio char(4),
  telefone1 text, telefone2 text, fax text, email text, situacao_especial text);
CREATE TABLE socio (
  cnpj_basico char(8) NOT NULL, identificador_de_socio smallint, nome_socio text,
  cnpj_cpf_socio text, qualificacao_socio char(2), data_entrada_sociedade date, pais char(3),
  representante_legal text, nome_representante text, qualificacao_representante_legal char(2),
  faixa_etaria smallint);
CREATE TABLE simples (
  cnpj_basico char(8), opcao_simples boolean, data_opcao_simples date, data_exclusao_simples date,
  opcao_mei boolean, data_opcao_mei date, data_exclusao_mei date);
CREATE TABLE _referencia (referencia text PRIMARY KEY, valor text);
'''

SQL_TRANSFORMA = '''
INSERT INTO empresa
SELECT cnpj_basico, trim(razao_social), nullif(trim(natureza_juridica),''),
       nullif(trim(qualificacao_responsavel),''),
       CASE WHEN capital_social ~ '^[0-9]+(,[0-9]+)?$' THEN replace(capital_social, ',', '.')::numeric(18,2) END,
       CASE WHEN porte_empresa ~ '^[0-9]+$' THEN porte_empresa::smallint END,
       nullif(trim(ente_federativo_responsavel),'')
FROM stg_empresa;
DROP TABLE stg_empresa;

INSERT INTO estabelecimento
SELECT cnpj_basico || cnpj_ordem || cnpj_dv, cnpj_basico, matriz_filial = '1',
       nullif(trim(nome_fantasia),''), situacao_cadastral::smallint, parse_data(data_situacao_cadastral),
       nullif(trim(motivo_situacao_cadastral),''), nullif(trim(nome_cidade_exterior),''),
       nullif(trim(pais),''), parse_data(data_inicio_atividades), nullif(trim(cnae_fiscal),''),
       string_to_array(nullif(regexp_replace(cnae_fiscal_secundaria, '[^0-9,]', '', 'g'), ''), ','),
       nullif(trim(tipo_logradouro),''), nullif(trim(logradouro),''), nullif(trim(numero),''),
       nullif(trim(complemento),''), nullif(trim(bairro),''), nullif(regexp_replace(cep,'[^0-9]','','g'),''),
       nullif(trim(uf),''), nullif(trim(municipio),''),
       CASE WHEN telefone1 ~ '^[0-9]{8,9}$' AND ddd1 ~ '^[0-9]{2,3}$' THEN trim(ddd1) || telefone1 END,
       CASE WHEN telefone2 ~ '^[0-9]{8,9}$' AND ddd2 ~ '^[0-9]{2,3}$' THEN trim(ddd2) || telefone2 END,
       CASE WHEN fax ~ '^[0-9]{8,9}$' AND ddd_fax ~ '^[0-9]{2,3}$' THEN trim(ddd_fax) || fax END,
       CASE WHEN lower(trim(correio_eletronico)) ~ '^[a-z0-9._%+-]+@[a-z0-9-]+(\\.[a-z0-9-]+)*\\.[a-z]{2,}$'
            THEN lower(trim(correio_eletronico)) END,
       nullif(trim(situacao_especial),'')
FROM stg_estabelecimento;
DROP TABLE stg_estabelecimento;

INSERT INTO socio
SELECT cnpj_basico, CASE WHEN identificador_de_socio ~ '^[0-9]$' THEN identificador_de_socio::smallint END,
       nullif(trim(nome_socio),''), nullif(trim(cnpj_cpf_socio),''), nullif(trim(qualificacao_socio),''),
       parse_data(data_entrada_sociedade), nullif(trim(pais),''), nullif(trim(representante_legal),''),
       nullif(trim(nome_representante),''), nullif(trim(qualificacao_representante_legal),''),
       CASE WHEN faixa_etaria ~ '^[0-9]$' THEN faixa_etaria::smallint END
FROM stg_socio;
DROP TABLE stg_socio;

INSERT INTO simples
SELECT cnpj_basico, opcao_simples = 'S', parse_data(data_opcao_simples), parse_data(data_exclusao_simples),
       opcao_mei = 'S', parse_data(data_opcao_mei), parse_data(data_exclusao_mei)
FROM stg_simples;
DROP TABLE stg_simples;
'''

SQL_INDICES_BASE = '''
CREATE INDEX IF NOT EXISTS estabelecimento_cnpj_basico_idx ON estabelecimento (cnpj_basico);
CREATE INDEX IF NOT EXISTS socio_cnpj_basico_idx ON socio (cnpj_basico);
CREATE INDEX IF NOT EXISTS socio_cnpj_cpf_socio_idx ON socio (cnpj_cpf_socio) WHERE cnpj_cpf_socio IS NOT NULL;
CREATE INDEX IF NOT EXISTS simples_cnpj_basico_idx ON simples (cnpj_basico);
'''

# Tabela de produto: uma linha por estabelecimento ativo, sem joins para filtrar/ranquear.
# A lead é montada por UF (e não numa consulta única): cada lote gasta pouco work_mem/temp e o espaço volta ao fim dele.
# Contagens de e-mail e de sócios são pré-agregadas em tabelas auxiliares com chave, para virarem lookup por índice.
SQL_LEAD_AUX = '''
CREATE TABLE _email_n AS SELECT email, count(*)::int AS n FROM estabelecimento WHERE email IS NOT NULL GROUP BY email;
ALTER TABLE _email_n ADD PRIMARY KEY (email);
CREATE TABLE _socios_n AS SELECT cnpj_basico, count(*)::int AS n FROM socio GROUP BY cnpj_basico;
ALTER TABLE _socios_n ADD PRIMARY KEY (cnpj_basico);
'''

SQL_LEAD_SELECT = '''
SELECT e.cnpj, e.cnpj_basico,
       emp.razao_social, e.nome_fantasia,
       sem_acento(coalesce(e.nome_fantasia || ' ', '') || emp.razao_social) AS nome_busca,
       e.matriz, emp.porte, emp.natureza_juridica, nj.descricao AS natureza_descricao, emp.capital_social,
       e.data_inicio_atividades,
       e.cnae_fiscal, c.descricao AS cnae_descricao, e.cnae_secundarias,
       e.uf, e.municipio AS municipio_codigo, m.descricao AS municipio_nome, e.cep,
       e.email, coalesce(em.n, 0) AS email_compartilhado,
       e.telefone1 AS telefone,
       (e.telefone1 ~ '^[0-9]{2}9[0-9]{8}$') AS telefone_celular,
       coalesce(s.opcao_simples, false) AS simples, coalesce(s.opcao_mei, false) AS mei,
       coalesce(so.n, 0) AS qtd_socios,
       format('%s, %s. Atividade principal: %s. Porte %s. %s, %s/%s. Aberta em %s.',
              coalesce(e.nome_fantasia, emp.razao_social), nj.descricao, c.descricao,
              CASE emp.porte WHEN 1 THEN 'microempresa' WHEN 3 THEN 'pequeno porte' WHEN 5 THEN 'demais' ELSE 'não informado' END,
              CASE WHEN e.matriz THEN 'Matriz' ELSE 'Filial' END, m.descricao, e.uf,
              to_char(e.data_inicio_atividades, 'MM/YYYY')) AS perfil_texto
FROM estabelecimento e
JOIN empresa emp USING (cnpj_basico)
LEFT JOIN cnae c ON c.codigo = e.cnae_fiscal
LEFT JOIN municipio m ON m.codigo = e.municipio
LEFT JOIN natureza_juridica nj ON nj.codigo = emp.natureza_juridica
LEFT JOIN simples s ON s.cnpj_basico = e.cnpj_basico
LEFT JOIN _email_n em ON em.email = e.email
LEFT JOIN _socios_n so ON so.cnpj_basico = e.cnpj_basico
'''

def cria_lead(conn):
    executa(conn, SQL_LEAD_AUX, 'contagens auxiliares (e-mail, sócios)')
    conn.execute('CREATE TABLE lead AS ' + SQL_LEAD_SELECT + ' WHERE false')
    ufs = [r[0] for r in conn.execute('select uf from estabelecimento group by uf order by uf')]
    t = time.time()
    for uf in ufs:
        t0 = time.time()
        # NULL/vazio vira um lote próprio
        # sem parâmetros: o format() da consulta usa %s literal
        cond = 'e.uf IS NULL' if uf is None else "e.uf = '" + uf.replace("'", "''") + "'"
        conn.execute('INSERT INTO lead ' + SQL_LEAD_SELECT + ' WHERE ' + cond)
        log(f'lead {uf}: lote em {time.time() - t0:.0f}s')
    conn.execute('DROP TABLE _email_n, _socios_n')
    conn.execute("COMMENT ON TABLE lead IS 'completa'")
    log(f'tabela lead criada ({time.time() - t:.0f}s)')

SQL_LEAD_INDICES = [
    ('lead_uf_mun_cnae', 'CREATE INDEX IF NOT EXISTS lead_uf_mun_cnae ON lead (uf, municipio_codigo, cnae_fiscal)'),
    ('lead_cnae_uf', 'CREATE INDEX IF NOT EXISTS lead_cnae_uf ON lead (cnae_fiscal, uf)'),
    ('lead_porte_capital', 'CREATE INDEX IF NOT EXISTS lead_porte_capital ON lead (porte, capital_social)'),
    ('lead_data_inicio', 'CREATE INDEX IF NOT EXISTS lead_data_inicio ON lead (data_inicio_atividades)'),
    ('lead_cnae_sec', 'CREATE INDEX IF NOT EXISTS lead_cnae_sec ON lead USING gin (cnae_secundarias)'),
    ('lead_contatavel', 'CREATE INDEX IF NOT EXISTS lead_contatavel ON lead (uf, cnae_fiscal) WHERE email IS NOT NULL OR telefone_celular'),
]
SQL_TRGM = 'CREATE INDEX IF NOT EXISTS lead_nome_trgm ON lead USING gin (nome_busca gin_trgm_ops)'

def indices_lead(conn):
    # cada índice é uma transação própria (autocommit): se o processo for interrompido, o que já foi criado fica
    if conn.execute("select 1 from pg_constraint where conrelid='lead'::regclass and contype='p'").fetchone() is None:
        executa(conn, 'ALTER TABLE lead ADD PRIMARY KEY (cnpj)', 'lead: chave primária')
    for nome, sql in SQL_LEAD_INDICES:
        executa(conn, sql, f'lead: {nome}')


#%% leitura / filtro / COPY -----------------------------------------------------------------------

# tipo -> (sufixo/trecho no nome do membro do zip, tabela destino, número de campos)
TIPOS = {
    'estab':   ('ESTABELE', 'stg_estabelecimento', 30),
    'empresa': ('EMPRECSV', 'stg_empresa', 7),
    'socio':   ('SOCIOCSV', 'stg_socio', 11),
    'simples': ('SIMPLES',  'stg_simples', 7),
}
DOMINIOS = [('.CNAECSV', 'cnae'), ('.MOTICSV', 'motivo'), ('.MUNICCSV', 'municipio'),
            ('.NATJUCSV', 'natureza_juridica'), ('.PAISCSV', 'pais'), ('.QUALSCSV', 'qualificacao_socio')]

def copy_sql(tabela):
    return f"COPY {tabela} FROM STDIN (FORMAT csv, DELIMITER ';', QUOTE '\"', ENCODING 'LATIN1')"

def conecta(schema):
    c = psycopg.connect(autocommit=True, options=f'-c search_path={schema},public -c synchronous_commit=off')
    return c

def basico(linha):
    b = linha[1:9]
    return int(b) if len(b) == 8 and b.isdigit() else None

# Linha bem formada: n campos, todos entre aspas, aspas internas duplicadas ("").
# Alguns registros da Receita têm aspas soltas dentro do campo (ex.: "RUA "A" 5"); enviadas ao COPY como estão,
# o Postgres considera o campo aberto e engole as linhas seguintes até estourar o buffer de 1 GB.
RE_LINHA = {n: re.compile(rb'^(?:"(?:[^"]|"")*";){%d}"(?:[^"]|"")*"\r?\n?$' % (n - 1)) for n in (7, 11, 30)}

def repara_linha(linha, n):
    """Reconstrói uma linha com aspas soltas separando por '";"'. Retorna None se não for possível."""
    fim = b'\r\n' if linha.endswith(b'\r\n') else b'\n'  # COPY não aceita fins de linha misturados
    corpo = linha.rstrip(b'\r\n')
    if len(corpo) < 2 or corpo[:1] != b'"' or corpo[-1:] != b'"':
        return None
    campos = corpo[1:-1].split(b'";"')
    if len(campos) != n:
        return None
    return b';'.join(b'"' + c.replace(b'""', b'"').replace(b'"', b'""') + b'"' for c in campos) + fim

def processa_arquivo(args):
    """Worker: lê um membro de zip em streaming, filtra e envia via COPY.
    Retorna (membro, lidas, gravadas, reparadas, descartadas, bitmap)."""
    schema, caminho_zip, membro, tipo, bitmap_in = args
    sufixo, tabela, ncampos = TIPOS[tipo]
    re_ok = RE_LINHA[ncampos]
    bm = bytearray(BITMAP_TAM) if tipo == 'estab' else bitmap_in
    lidas = gravadas = reparadas = descartadas = 0
    with conecta(schema) as conn, conn.cursor() as cur, zipfile.ZipFile(caminho_zip) as z, z.open(membro) as f, \
            cur.copy(copy_sql(tabela)) as cp:
        lote, tam = [], 0
        for linha in f:
            lidas += 1
            if tipo == 'estab' and not RE_ATIVO.match(linha) and b';"02";' not in linha:
                continue  # rejeição barata; se há '"02"' em outro ponto a linha pode ser ativa com aspas soltas
            if not re_ok.match(linha):
                linha = repara_linha(linha, ncampos)
                if linha is None:
                    descartadas += 1
                    continue
                reparadas += 1
            if tipo == 'estab':
                if not RE_ATIVO.match(linha):
                    continue
                n = basico(linha)
                if n is None:
                    continue
                bm[n >> 3] |= 1 << (n & 7)
            else:
                n = basico(linha)
                if n is None or not (bm[n >> 3] >> (n & 7)) & 1:
                    continue
            lote.append(linha); tam += len(linha); gravadas += 1
            if tam >= TAM_LOTE:
                cp.write(b''.join(lote)); lote, tam = [], 0
        if lote:
            cp.write(b''.join(lote))
    if lidas > 10_000 and gravadas == 0:
        raise RuntimeError(f'{membro}: nenhuma linha aproveitada em {lidas} — layout do arquivo mudou?')
    if lidas and descartadas > max(100, lidas // 1000):
        raise RuntimeError(f'{membro}: {descartadas:,} linhas irreparáveis em {lidas:,} — layout do arquivo mudou?')
    return membro, lidas, gravadas, reparadas, descartadas, bytes(bm) if tipo == 'estab' else None

def carrega_dominio(conn, caminho_zip, membro, tabela):
    with zipfile.ZipFile(caminho_zip) as z, z.open(membro) as f, conn.cursor() as cur, \
            cur.copy(copy_sql(tabela)) as cp:
        while dados := f.read(1 << 20):
            cp.write(dados)

def data_referencia(membros):
    """Arquivos terminam em .DAMMDD (ex.: D40913). Resolve a década pelo ano corrente."""
    for _, m, _ in membros:
        mt = re.search(r'\.D(\d)(\d\d)(\d\d)$', m.upper())
        if mt:
            ano = date.today().year // 10 * 10 + int(mt.group(1))
            if ano > date.today().year:
                ano -= 10
            return f'{mt.group(3)}/{mt.group(2)}/{ano}'
    return None

#%% principal -------------------------------------------------------------------------------------

def chave_unica(conn, tabela, coluna, preferir=''):
    """Cria PK; se a fonte tiver registros repetidos, mantém um por chave (o que satisfaz `preferir`, senão o último carregado)."""
    try:
        conn.execute(f'ALTER TABLE {tabela} ADD PRIMARY KEY ({coluna})')
        return
    except psycopg.errors.UniqueViolation as ex:
        log(f'AVISO: chave duplicada em {tabela} ({str(ex.diag.message_detail or "")}). Removendo repetidos...')
    t = time.time()
    conn.execute(f'CREATE TABLE {tabela}_u AS SELECT DISTINCT ON ({coluna}) * FROM {tabela} ORDER BY {coluna}, {preferir}ctid DESC')
    antes = conn.execute(f'select count(*) from {tabela}').fetchone()[0]
    depois = conn.execute(f'select count(*) from {tabela}_u').fetchone()[0]
    conn.execute(f'DROP TABLE {tabela}')
    conn.execute(f'ALTER TABLE {tabela}_u RENAME TO {tabela}')
    conn.execute(f'ALTER TABLE {tabela} ADD PRIMARY KEY ({coluna})')
    log(f'{tabela}: {antes - depois:,} linhas repetidas removidas ({time.time() - t:.0f}s)')

def executa(conn, sql, rotulo=None):
    t = time.time()
    conn.execute(sql)
    if rotulo:
        log(f'{rotulo} ({time.time() - t:.0f}s)')

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--pasta-zip', default='dados-publicos-zip')
    ap.add_argument('--schema', default='cnpj', help='schema final consumido pela API')
    ap.add_argument('--workers', type=int, default=4)
    ap.add_argument('--yes', action='store_true')
    ap.add_argument('--sem-trgm', action='store_true',
                    help='não cria o índice trigram da lead (demorado); crie depois com --indice-trgm')
    ap.add_argument('--indice-trgm', action='store_true',
                    help='só cria o índice trigram (busca por nome) no schema final já no ar, e sai')
    ap.add_argument('--retomar', action='store_true',
                    help='continua a partir do schema <schema>_novo já carregado, sem reler os zips')
    a = ap.parse_args()

    faltam = [v for v in ('PGHOST', 'PGDATABASE', 'PGUSER', 'PGPASSWORD') if not os.environ.get(v)]
    if faltam:
        sys.exit('Variáveis de ambiente ausentes: ' + ', '.join(faltam) +
                 '\nDefina PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD e PGSSLMODE=require no mesmo terminal antes de rodar.')

    if a.indice_trgm:
        with psycopg.connect(autocommit=True, options=f'-c search_path={a.schema},public') as c:
            c.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public')
            c.execute("SET maintenance_work_mem = '1GB'")
            c.execute('SET max_parallel_maintenance_workers = 4')
            executa(c, SQL_TRGM, 'índice trigram (busca por nome)')
            executa(c, 'ANALYZE lead', 'ANALYZE lead')
        sys.exit(0)

    zips = sorted(glob.glob(os.path.join(a.pasta_zip, '*.zip')))
    if not zips:
        sys.exit(f'Nenhum .zip em {a.pasta_zip}. Rode dados_cnpj_baixa.py primeiro.')

    membros = []  # (zip, nome do membro, tipo|dominio)
    for zp in zips:
        with zipfile.ZipFile(zp) as z:
            for nome in z.namelist():
                up = nome.upper()
                if up.endswith('/') or up.endswith('.TXT'):
                    continue
                for tipo, (suf, _, _) in TIPOS.items():
                    if suf in up:
                        membros.append((zp, nome, tipo)); break
                else:
                    for suf, tab in DOMINIOS:
                        if up.endswith(suf):
                            membros.append((zp, nome, 'dom:' + tab)); break
    # a Receita publica 10 partições de empresas/estabelecimentos/sócios, 1 de simples e 6 tabelas de domínio
    esperado = {'estab': 10, 'empresa': 10, 'socio': 10, 'simples': 1}
    achado = {t: sum(1 for m in membros if m[2] == t) for t in esperado}
    nomes = {'estab': 'Estabelecimentos', 'empresa': 'Empresas', 'socio': 'Sócios', 'simples': 'Simples'}
    faltando = [f'{nomes[t]}: {achado[t]}/{esperado[t]}' for t in esperado if achado[t] < esperado[t]]
    if faltando or sum(1 for m in membros if m[2].startswith('dom:')) < len(DOMINIOS):
        sys.exit('Download incompleto, abortando antes de tocar no banco. ' + ' | '.join(faltando) +
                 '\nApague os zips e rode dados_cnpj_baixa.py novamente.')
    ref = data_referencia(membros) or time.strftime('%d/%m/%Y')
    novo = a.schema + '_novo'

    with psycopg.connect(autocommit=True) as c0:
        info = c0.execute("select current_database(), inet_server_addr()::text, version()").fetchone()
    log(f'Banco {info[0]} @ {info[1]} | {info[2][:30]}')
    log(f'Referência da base: {ref} | {len(membros)} arquivos | schema final "{a.schema}" (construído em "{novo}")')
    if not a.yes and input('Construir/substituir o schema acima? (S/N) ').strip().upper() != 'S':
        sys.exit()

    conn = psycopg.connect(autocommit=True)
    with conn:
        # extensões opcionais: dependem de liberação em azure.extensions no portal
        ext = {}
        for e in ('pg_trgm', 'vector'):
            try:
                conn.execute(f'CREATE EXTENSION IF NOT EXISTS {e} SCHEMA public')
                ext[e] = True
            except psycopg.Error as ex:
                ext[e] = False
                log(f'AVISO: extensão {e} indisponível ({str(ex).splitlines()[0]}). Pulando recursos que dependem dela.')

        if a.retomar:
            if not conn.execute('select 1 from pg_namespace where nspname=%s', (novo,)).fetchone():
                sys.exit(f'--retomar: schema {novo} não existe.')
        else:
            conn.execute(f'DROP SCHEMA IF EXISTS {novo} CASCADE')
            conn.execute(f'CREATE SCHEMA {novo}')
        conn.execute(f'SET search_path = {novo}, public')
        conn.execute('SET synchronous_commit = off')
        conn.execute("SET maintenance_work_mem = '1GB'")
        conn.execute("SET work_mem = '64MB'")  # a criação da lead faz hash joins grandes; com 4MB gasta GBs de arquivo temporário
        conn.execute('SET max_parallel_maintenance_workers = 4')
        if not a.retomar:
            for blk in (SQL_FUNCOES, SQL_STAGING, SQL_TABELAS):
                conn.execute(blk)

        if not a.retomar:
            # 1) domínios (pequenos)
            for zp, nome, tipo in membros:
                if tipo.startswith('dom:'):
                    carrega_dominio(conn, zp, nome, tipo[4:])
            log('domínios carregados')

            # 2) estabelecimentos ativos -> bitmap de cnpj_basico ativos
            bitmap = 0
            t = time.time()
            with ProcessPoolExecutor(a.workers) as ex:
                tarefas = [(novo, zp, nome, 'estab', None) for zp, nome, tipo in membros if tipo == 'estab']
                for nome, lidas, grav, rep, desc, bm in ex.map(processa_arquivo, tarefas):
                    bitmap |= int.from_bytes(bm, 'little')
                    log(f'{nome}: {grav:,} ativos de {lidas:,} (reparadas {rep:,}, descartadas {desc:,})')
            bitmap = bitmap.to_bytes(BITMAP_TAM, 'little')
            log(f'estabelecimentos ativos carregados ({time.time() - t:.0f}s)')

            # 3) empresas / sócios / simples filtrados pelo bitmap
            t = time.time()
            with ProcessPoolExecutor(a.workers) as ex:
                tarefas = [(novo, zp, nome, tipo, bitmap) for zp, nome, tipo in membros if tipo in ('empresa', 'socio', 'simples')]
                for nome, lidas, grav, rep, desc, _ in ex.map(processa_arquivo, tarefas):
                    log(f'{nome}: {grav:,} de {lidas:,} (reparadas {rep:,}, descartadas {desc:,})')
            log(f'empresas/sócios/simples carregados ({time.time() - t:.0f}s)')

        # 4) tipagem, chaves, tabela de produto e índices
        if conn.execute("select to_regclass('stg_empresa')").fetchone()[0] is not None:
            executa(conn, SQL_TRANSFORMA, 'tabelas tipadas criadas')
        lead_ok = conn.execute("select obj_description(r, 'pg_class') = 'completa' from (select to_regclass('lead') r) x where r is not null").fetchone()
        lead_ok = bool(lead_ok and lead_ok[0])
        if not lead_ok:
            conn.execute('DROP TABLE IF EXISTS lead, lead_embedding, _email_n, _socios_n')
        # a Receita publica ocasionalmente um CNPJ básico duplicado, com uma das linhas vazia (ex.: 08314885)
        for tabela, coluna, preferir in (('empresa', 'cnpj_basico', "(coalesce(razao_social, '') <> '') DESC, "),
                                         ('estabelecimento', 'cnpj', ''), ('simples', 'cnpj_basico', '')):
            if conn.execute("select 1 from pg_constraint where conrelid=%s::regclass and contype='p'", (tabela,)).fetchone() is None:
                chave_unica(conn, tabela, coluna, preferir)
        log('chaves primárias criadas')
        executa(conn, SQL_INDICES_BASE, 'índices base')
        if lead_ok:
            log('tabela lead já completa, reaproveitando')
        else:
            cria_lead(conn)
        indices_lead(conn)
        if a.sem_trgm:
            log('AVISO: índice trigram adiado (rode --indice-trgm depois da troca do schema)')
        elif ext['pg_trgm']:
            executa(conn, SQL_TRGM, 'índice trigram (busca por nome)')
        else:
            executa(conn, 'CREATE INDEX lead_nome_prefixo ON lead (nome_busca text_pattern_ops)', 'índice por prefixo de nome (sem pg_trgm)')
        if ext['vector']:
            conn.execute("CREATE TABLE IF NOT EXISTS lead_embedding (cnpj char(14) PRIMARY KEY, embedding vector(1024) NOT NULL, modelo text NOT NULL)")
            log('tabela lead_embedding criada (índice HNSW deve ser criado após popular)')

        n = {t: conn.execute(f'select count(*) from {t}').fetchone()[0] for t in ('empresa', 'estabelecimento', 'socio', 'lead')}
        conn.execute("INSERT INTO _referencia VALUES ('CNPJ', %s), ('carga', %s), ('contagens', %s) "
                     "ON CONFLICT (referencia) DO UPDATE SET valor = EXCLUDED.valor",
                     (ref, time.strftime('%Y-%m-%d %H:%M:%S'), str(n)))
        executa(conn, 'ANALYZE', 'ANALYZE')

        # 5) troca atômica
        with conn.transaction():
            conn.execute(f'DROP SCHEMA IF EXISTS {a.schema}_antigo CASCADE')
            existe = conn.execute('select 1 from pg_namespace where nspname=%s', (a.schema,)).fetchone()
            if existe:
                conn.execute(f'ALTER SCHEMA {a.schema} RENAME TO {a.schema}_antigo')
            conn.execute(f'ALTER SCHEMA {novo} RENAME TO {a.schema}')
        conn.execute(f'DROP SCHEMA IF EXISTS {a.schema}_antigo CASCADE')
        log('contagens:', n)
        log(f'FIM: schema "{a.schema}" no ar.')

if __name__ == '__main__':
    main()
