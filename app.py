# app.py
from __future__ import annotations

import os
import re
from io import BytesIO
from datetime import datetime

import pandas as pd
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    send_file,
    jsonify,
)

from dotenv import load_dotenv
from supabase import create_client, Client

from comissoes_backend import calcular_comissoes

# =====================================================================
# 1) CONFIGURAÇÃO BÁSICA DO FLASK
# =====================================================================

app = Flask(__name__)
app.secret_key = "segredo-muito-simples-so-pra-flash"

load_dotenv()

# =====================================================================
# 2) CONFIGURAÇÃO DO SUPABASE
# =====================================================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "comissoes")

supabase: Client | None = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print("Erro ao criar client do Supabase:", e)
        supabase = None
else:
    print("⚠️ SUPABASE_URL ou SUPABASE_KEY não configurados. Upload ficará desativado.")

# =====================================================================
# 3) PASTA DE OUTPUT LOCAL (APENAS PARA RODAR NA MÁQUINA / DEBUG)
# =====================================================================

if os.getenv("VERCEL"):
    OUTPUT_DIR = "/tmp/outputs"
else:
    OUTPUT_DIR = os.path.join(app.root_path, "outputs")

os.makedirs(OUTPUT_DIR, exist_ok=True)

OUTPUT_FILES = {
    "df_final": os.path.join(OUTPUT_DIR, "df_final.xlsx"),
    "df_juntar": os.path.join(OUTPUT_DIR, "df_juntar.xlsx"),
    "pj1": os.path.join(OUTPUT_DIR, "pj1.xlsx"),
    "seg": os.path.join(OUTPUT_DIR, "seguro_pj.xlsx"),
    "cam": os.path.join(OUTPUT_DIR, "cambio.xlsx"),
    "co_ter": os.path.join(OUTPUT_DIR, "co_corretagem_terceiras.xlsx"),
    "co_xpvp": os.path.join(OUTPUT_DIR, "co_corretagem_xpvp.xlsx"),
    "cre": os.path.join(OUTPUT_DIR, "credito.xlsx"),
    "xpcs": os.path.join(OUTPUT_DIR, "xpcs.xlsx"),
    "lan_man": os.path.join(OUTPUT_DIR, "lancamentos_manuais.xlsx"),
    "tim_rep": os.path.join(OUTPUT_DIR, "times_repasses.xlsx"),
    "lan_pro": os.path.join(OUTPUT_DIR, "lancamento_produtos.xlsx"),
}

# =====================================================================
# 4) FUNÇÕES AUXILIARES
# =====================================================================

FONTE_KEYS = [
    "pj1",
    "seg",
    "cam",
    "co_ter",
    "co_xpvp",
    "cre",
    "xpcs",
    "lan_man",
    "tim_rep",
    "lan_pro",
]

FONTE_NOMES = {
    "PJ1 - Base Principal": "pj1",
    "Seguro PJ": "seg",
    "Câmbio": "cam",
    "Co-corretagem Terceiras": "co_ter",
    "Co-corretagem XPVP": "co_xpvp",
    "Crédito": "cre",
    "XPCS": "xpcs",
    "Lançamentos Manuais": "lan_man",
    "Times e Repasses": "tim_rep",
    "Lançamento de Produtos": "lan_pro",
}

FONTE_ARQUIVOS_PREFIXO = {
    "pj1": "pj1",
    "seg": "seguro_pj",
    "cam": "cambio",
    "co_ter": "co_corretagem_terceiras",
    "co_xpvp": "co_corretagem_xpvp",
    "cre": "credito",
    "xpcs": "xpcs",
    "lan_man": "lancamentos_manuais",
    "tim_rep": "times_repasses",
    "lan_pro": "lancamento_produtos",
}


def classificar_arquivos(uploaded_files):
    slots = {k: None for k in ["pj1", "seg", "cam", "co_ter", "co_xpvp", "cre", "xpcs", "lan_man", "tim_rep", "lan_pro"]}
    usados = set()

    for f in uploaded_files:
        nome = (f.filename or "").lower()

        def marca(chave):
            if slots[chave] is None:
                slots[chave] = f
                usados.add((f.filename or "").lower())

        if "seguro" in nome:
            marca("seg")
        elif "câmbio" in nome or "cambio" in nome:
            marca("cam")
        elif "terceiras" in nome:
            marca("co_ter")
        elif "xpvp" in nome:
            marca("co_xpvp")
        elif "crédito" in nome or "credito" in nome:
            marca("cre")
        elif "xpcs" in nome:
            marca("xpcs")
        elif "lançamentos manuais" in nome or "lancamentos manuais" in nome:
            marca("lan_man")
        elif "times e repasses" in nome:
            marca("tim_rep")
        elif "lançamento de produtos" in nome or "lancamento de produtos" in nome:
            marca("lan_pro")

    nao_usados = [f for f in uploaded_files if (f.filename or "").lower() not in usados]
    if nao_usados and slots["pj1"] is None:
        slots["pj1"] = nao_usados[0]

    faltando = [k for k, v in slots.items() if v is None]
    return slots, faltando


def brl(valor: float) -> str:
    if pd.isna(valor):
        valor = 0.0
    return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")


def _supabase_list(path: str):
    if supabase is None:
        return []
    try:
        # OBS: se seu bucket tiver MUITOS arquivos, aqui dá pra implementar paginação.
        return supabase.storage.from_(SUPABASE_BUCKET).list(path=path)
    except Exception as e:
        print("Erro listando no Supabase:", e)
        return []


# -----------------------
# LISTAGEM DE COMPETÊNCIAS
# -----------------------
def listar_competencias() -> list[str]:
    itens = _supabase_list("")
    comps = []
    for it in itens:
        nome = it.get("name", "")
        if re.match(r"^\d{4}-\d{2}$", nome):
            comps.append(nome)
    return sorted(comps, reverse=True)


# -----------------------
# LISTAGEM DF_FINAL: aceita timestamp antigo e versão nova (vN)
# -----------------------
_RE_DF_FINAL_TS = re.compile(r"^df_final_(\d{8}_\d{6})\.xlsx$")
_RE_DF_FINAL_V = re.compile(r"^df_final_v(\d+)\.xlsx$")


def listar_df_final_por_competencia(competencia: str) -> list[str]:
    itens = _supabase_list(competencia)
    arquivos = []
    for it in itens:
        nome = it.get("name", "")
        if _RE_DF_FINAL_TS.match(nome) or _RE_DF_FINAL_V.match(nome):
            arquivos.append(f"{competencia}/{nome}")

    # Ordena: primeiro por versão (maior vN), depois por timestamp (mais recente)
    def sort_key(path: str):
        base = path.split("/")[-1]
        mv = _RE_DF_FINAL_V.match(base)
        if mv:
            return (2, int(mv.group(1)))  # tipo 2 = versão
        mt = _RE_DF_FINAL_TS.match(base)
        if mt:
            return (1, mt.group(1))       # tipo 1 = timestamp
        return (0, base)

    arquivos.sort(key=sort_key, reverse=True)
    return arquivos


def escolher_mais_recente_df_final(competencia: str) -> str | None:
    arquivos = listar_df_final_por_competencia(competencia)
    return arquivos[0] if arquivos else None


def supabase_download_bytes(path: str) -> bytes | None:
    if supabase is None:
        return None
    try:
        data = supabase.storage.from_(SUPABASE_BUCKET).download(path)
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        if hasattr(data, "data"):
            return data.data
        return None
    except Exception as e:
        print("Erro no download do Supabase:", e)
        return None


def carregar_excel_do_supabase(path: str) -> pd.DataFrame | None:
    b = supabase_download_bytes(path)
    if not b:
        return None
    return pd.read_excel(BytesIO(b))


def supabase_upload_df_upsert(df: pd.DataFrame, path: str):
    if supabase is None:
        raise RuntimeError("Supabase não configurado")

    buf = BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)

    supabase.storage.from_(SUPABASE_BUCKET).upload(
        path=path,
        file=buf.getvalue(),
        file_options={
            "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "upsert": "true",
        },
    )


# -----------------------
# PARSE: pega competência e "id" da versão (vN ou timestamp)
# -----------------------
def parse_comp_versionid_from_df_final_path(df_final_path: str) -> tuple[str | None, str | None]:
    """
    Retorna (competencia, version_id)
    version_id pode ser: "v3" ou "20251216_173207"
    """
    if not df_final_path or "/" not in df_final_path:
        return None, None

    comp = df_final_path.split("/")[0]
    base = df_final_path.split("/")[-1]

    if not re.match(r"^\d{4}-\d{2}$", comp):
        return None, None

    mv = _RE_DF_FINAL_V.match(base)
    if mv:
        return comp, f"v{mv.group(1)}"

    mt = _RE_DF_FINAL_TS.match(base)
    if mt:
        return comp, mt.group(1)

    return None, None


def montar_links_fontes_local():
    return {
        "PJ1 - Base Principal": url_for("download_excel", nome="pj1"),
        "Seguro PJ": url_for("download_excel", nome="seg"),
        "Câmbio": url_for("download_excel", nome="cam"),
        "Co-corretagem Terceiras": url_for("download_excel", nome="co_ter"),
        "Co-corretagem XPVP": url_for("download_excel", nome="co_xpvp"),
        "Crédito": url_for("download_excel", nome="cre"),
        "XPCS": url_for("download_excel", nome="xpcs"),
        "Lançamentos Manuais": url_for("download_excel", nome="lan_man"),
        "Times e Repasses": url_for("download_excel", nome="tim_rep"),
        "Lançamento de Produtos": url_for("download_excel", nome="lan_pro"),
    }


def montar_links_fontes_supabase(comp: str, version_id: str):
    links = {}
    for nome_bonito, chave in FONTE_NOMES.items():
        prefixo = FONTE_ARQUIVOS_PREFIXO[chave]
        path = f"{comp}/{prefixo}_{version_id}.xlsx"
        links[nome_bonito] = url_for("download_supabase", file=path)
    return links


def proxima_versao_da_competencia(comp: str) -> int:
    """
    Procura por df_final_vN.xlsx na pasta da competência e devolve N+1.
    Se não existir nenhum, começa em 1.
    """
    itens = _supabase_list(comp)
    max_v = 0
    for it in itens:
        nome = it.get("name", "")
        mv = _RE_DF_FINAL_V.match(nome)
        if mv:
            max_v = max(max_v, int(mv.group(1)))
    return max_v + 1


def montar_contexto_dashboard(
    df_final: pd.DataFrame,
    competencia_label: str,
    caminho_df_final: str | None,
    df_juntar: pd.DataFrame | None = None,
    tabelas_fontes_dfs: dict[str, pd.DataFrame] | None = None,
    fontes_keys: dict[str, str] | None = None,
    links_fontes_override: dict[str, str] | None = None,
):
    colunas_numericas = df_final.select_dtypes(include=["number"]).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].round(2)

    df_display = df_final.copy()
    for col in colunas_numericas:
        df_display[col] = df_display[col].apply(
            lambda x: f"{x:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        )

    tabela_html = df_display.to_html(
        classes="table table-striped table-bordered table-sm dataframe",
        index=False,
    )

    if "Valor Total Assessor" in df_final.columns:
        total_assessores = len(df_final)
        soma_total = df_final["Valor Total Assessor"].sum()
        media_total = df_final["Valor Total Assessor"].mean()
        max_total = df_final["Valor Total Assessor"].max()
    else:
        total_assessores = len(df_final)
        soma_total = media_total = max_total = 0.0

    df_juntar_registros = []
    if df_juntar is not None and not df_juntar.empty:
        df_juntar_registros = df_juntar.to_dict(orient="records")

    def df_to_html(df):
        return df.to_html(
            classes="table table-striped table-bordered table-sm dataframe",
            index=False,
        )

    tabelas_fontes = {}
    if tabelas_fontes_dfs:
        tabelas_fontes = {nome: df_to_html(df) for nome, df in tabelas_fontes_dfs.items()}

    links_fontes = links_fontes_override if links_fontes_override is not None else montar_links_fontes_local()

    competencias_disponiveis = listar_competencias()
    competencia_atual = None
    if caminho_df_final and "/" in caminho_df_final:
        competencia_atual = caminho_df_final.split("/")[0]

    arquivos_df_final = listar_df_final_por_competencia(competencia_atual) if competencia_atual else []

    return dict(
        tabela=tabela_html,
        total_assessores=total_assessores,
        soma_total=brl(soma_total),
        media_total=brl(media_total),
        max_total_val=brl(max_total),
        tabelas_fontes=tabelas_fontes,
        links_fontes=links_fontes,
        fontes_keys=(fontes_keys or {}),
        df_juntar=df_juntar_registros,
        caminho_df_final=caminho_df_final,
        competencia=competencia_label,
        competencias_disponiveis=competencias_disponiveis,
        competencia_atual=competencia_atual,
        arquivos_df_final=arquivos_df_final,
    )

# =====================================================================
# 5) ROTAS
# =====================================================================

@app.route("/")
def index():
    competencias = listar_competencias()
    return render_template("index.html", competencias_disponiveis=competencias)


@app.route("/api/arquivos")
def api_arquivos():
    comp = (request.args.get("competencia") or "").strip()
    if not re.match(r"^\d{4}-\d{2}$", comp):
        return jsonify({"ok": False, "files": []})
    files = listar_df_final_por_competencia(comp)
    return jsonify({"ok": True, "files": files})


@app.route("/api/substituir_fonte", methods=["POST"])
def api_substituir_fonte():
    if supabase is None:
        return jsonify({"ok": False, "error": "Supabase não configurado."}), 400

    df_final_path = (request.form.get("df_final_path") or "").strip()
    fonte_key = (request.form.get("fonte_key") or "").strip()

    if fonte_key not in ["pj1", "seg", "cam", "co_ter", "co_xpvp", "cre", "xpcs", "lan_man", "tim_rep", "lan_pro"]:
        return jsonify({"ok": False, "error": "fonte_key inválida."}), 400

    up_file = request.files.get("file")
    if not up_file or not up_file.filename:
        return jsonify({"ok": False, "error": "Nenhum arquivo enviado."}), 400

    comp, version_id = parse_comp_versionid_from_df_final_path(df_final_path)
    if not comp or not version_id:
        return jsonify({"ok": False, "error": "df_final_path inválido (precisa conter competência e versão)."}), 400

    caminhos = {
        "df_final": df_final_path,
        "df_juntar": f"{comp}/df_juntar_{version_id}.xlsx",
    }
    for k in FONTE_ARQUIVOS_PREFIXO:
        prefixo = FONTE_ARQUIVOS_PREFIXO[k]
        caminhos[k] = f"{comp}/{prefixo}_{version_id}.xlsx"

    dfs = {}
    for k in ["pj1", "seg", "cam", "co_ter", "co_xpvp", "cre", "xpcs", "lan_man", "tim_rep", "lan_pro"]:
        if k == fonte_key:
            continue
        df_old = carregar_excel_do_supabase(caminhos[k])
        if df_old is None:
            return jsonify({"ok": False, "error": f"Não encontrei no Supabase a fonte '{k}' desta versão ({caminhos[k]})."}), 400
        dfs[k] = df_old

    try:
        df_new = pd.read_excel(up_file)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Não consegui ler o Excel enviado: {e}"}), 400

    dfs[fonte_key] = df_new

    try:
        df_final_new, df_juntar_new = calcular_comissoes(
            dfs["pj1"],
            dfs["seg"],
            dfs["cam"],
            dfs["co_ter"],
            dfs["co_xpvp"],
            dfs["cre"],
            dfs["xpcs"],
            dfs["lan_man"],
            dfs["tim_rep"],
            dfs["lan_pro"],
        )
    except Exception as e:
        return jsonify({"ok": False, "error": f"Erro ao recalcular comissões: {e}"}), 500

    colunas_numericas = df_final_new.select_dtypes(include=["number"]).columns
    df_final_new[colunas_numericas] = df_final_new[colunas_numericas].round(2)

    try:
        supabase_upload_df_upsert(df_final_new, caminhos["df_final"])
        supabase_upload_df_upsert(df_juntar_new, caminhos["df_juntar"])
        supabase_upload_df_upsert(df_new, caminhos[fonte_key])
    except Exception as e:
        return jsonify({"ok": False, "error": f"Erro ao enviar atualização ao Supabase: {e}"}), 500

    return jsonify({"ok": True, "redirect": url_for("visualizar_antigo", file=caminhos["df_final"])})


@app.route("/visualizar")
def visualizar_antigo():
    if supabase is None:
        flash("Supabase não está configurado. Não consigo listar/abrir arquivos antigos.")
        return redirect(url_for("index"))

    file_path = (request.args.get("file") or "").strip()
    competencia = (request.args.get("competencia") or "").strip()

    if not file_path:
        if not re.match(r"^\d{4}-\d{2}$", competencia):
            flash("Selecione uma competência válida para visualizar.")
            return redirect(url_for("index"))
        file_path = escolher_mais_recente_df_final(competencia)
        if not file_path:
            flash("Não encontrei df_final para essa competência no Supabase.")
            return redirect(url_for("index"))

    df_final = carregar_excel_do_supabase(file_path)
    if df_final is None:
        flash("Não consegui baixar/ler o Excel selecionado do Supabase.")
        return redirect(url_for("index"))

    comp, version_id = parse_comp_versionid_from_df_final_path(file_path)

    competencia_label = "—"
    if comp and re.match(r"^\d{4}-\d{2}$", comp):
        competencia_label = f"{comp.split('-')[1]}/{comp.split('-')[0]}"

    df_juntar = None
    tabelas_fontes_dfs = None
    links_fontes = None

    if comp and version_id:
        caminhos = {
            "df_juntar": f"{comp}/df_juntar_{version_id}.xlsx",
            "pj1": f"{comp}/pj1_{version_id}.xlsx",
            "seg": f"{comp}/seguro_pj_{version_id}.xlsx",
            "cam": f"{comp}/cambio_{version_id}.xlsx",
            "co_ter": f"{comp}/co_corretagem_terceiras_{version_id}.xlsx",
            "co_xpvp": f"{comp}/co_corretagem_xpvp_{version_id}.xlsx",
            "cre": f"{comp}/credito_{version_id}.xlsx",
            "xpcs": f"{comp}/xpcs_{version_id}.xlsx",
            "lan_man": f"{comp}/lancamentos_manuais_{version_id}.xlsx",
            "tim_rep": f"{comp}/times_repasses_{version_id}.xlsx",
            "lan_pro": f"{comp}/lancamento_produtos_{version_id}.xlsx",
        }

        df_juntar = carregar_excel_do_supabase(caminhos["df_juntar"])

        tabelas_fontes_dfs = {}
        for nome_bonito, chave in FONTE_NOMES.items():
            df_tmp = carregar_excel_do_supabase(caminhos[chave])
            if df_tmp is not None:
                tabelas_fontes_dfs[nome_bonito] = df_tmp

        if not tabelas_fontes_dfs:
            tabelas_fontes_dfs = None

        links_fontes = montar_links_fontes_supabase(comp, version_id)

    contexto = montar_contexto_dashboard(
        df_final=df_final,
        competencia_label=competencia_label,
        caminho_df_final=file_path,
        df_juntar=df_juntar,
        tabelas_fontes_dfs=tabelas_fontes_dfs,
        fontes_keys=FONTE_NOMES,
        links_fontes_override=links_fontes,
    )

    contexto["max_total"] = contexto.pop("max_total_val")
    return render_template("resultado.html", **contexto)


@app.route("/processar", methods=["POST"])
def processar():
    arquivos = request.files.getlist("files")

    competencia = (request.form.get("competencia") or "").strip()
    if not re.match(r"^\d{4}-\d{2}$", competencia):
        flash("Selecione a competência (mês/ano) antes de processar.")
        return redirect(url_for("index"))

    ano, mes = competencia.split("-")
    prefixo_competencia = f"{ano}-{mes}"
    competencia_label = f"{mes}/{ano}"

    if not arquivos or arquivos[0].filename == "":
        flash("Nenhum arquivo foi enviado. Selecione a pasta ou os arquivos de comissão.")
        return redirect(url_for("index"))

    slots, faltando = classificar_arquivos(arquivos)
    if faltando:
        flash("Não consegui identificar estes tipos de arquivo: " + ", ".join(faltando))
        flash(
            "Confira se os nomes contêm: Seguro, Câmbio, Terceiras, XPVP, Crédito, XPCS, "
            "Lançamentos Manuais, Times e Repasses, Lançamento de Produtos."
        )
        return redirect(url_for("index"))

    pj1 = pd.read_excel(slots["pj1"])
    seg = pd.read_excel(slots["seg"])
    cam = pd.read_excel(slots["cam"])
    co_ter = pd.read_excel(slots["co_ter"])
    co_xpvp = pd.read_excel(slots["co_xpvp"])
    cre = pd.read_excel(slots["cre"])
    xpcs = pd.read_excel(slots["xpcs"])
    lan_man = pd.read_excel(slots["lan_man"])
    tim_rep = pd.read_excel(slots["tim_rep"])
    lan_pro = pd.read_excel(slots["lan_pro"])

    df_final, df_juntar = calcular_comissoes(
        pj1, seg, cam, co_ter, co_xpvp, cre, xpcs, lan_man, tim_rep, lan_pro
    )

    colunas_numericas = df_final.select_dtypes(include=["number"]).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].round(2)

    if not os.getenv("VERCEL"):
        pasta_competencia = os.path.join(OUTPUT_DIR, prefixo_competencia)
        os.makedirs(pasta_competencia, exist_ok=True)

        df_final.to_excel(OUTPUT_FILES["df_final"], index=False)
        df_juntar.to_excel(OUTPUT_FILES["df_juntar"], index=False)
        pj1.to_excel(OUTPUT_FILES["pj1"], index=False)
        seg.to_excel(OUTPUT_FILES["seg"], index=False)
        cam.to_excel(OUTPUT_FILES["cam"], index=False)
        co_ter.to_excel(OUTPUT_FILES["co_ter"], index=False)
        co_xpvp.to_excel(OUTPUT_FILES["co_xpvp"], index=False)
        cre.to_excel(OUTPUT_FILES["cre"], index=False)
        xpcs.to_excel(OUTPUT_FILES["xpcs"], index=False)
        lan_man.to_excel(OUTPUT_FILES["lan_man"], index=False)
        tim_rep.to_excel(OUTPUT_FILES["tim_rep"], index=False)
        lan_pro.to_excel(OUTPUT_FILES["lan_pro"], index=False)

        df_final.to_excel(os.path.join(pasta_competencia, "df_final.xlsx"), index=False)

    nome_arquivo_df_final = None

    if supabase is not None:
        try:
            # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            # NOVO: versionamento por mês: v1, v2, v3...
            # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
            prox = proxima_versao_da_competencia(prefixo_competencia)
            version_id = f"v{prox}"

            def upload_df(df: pd.DataFrame, path: str):
                buf = BytesIO()
                df.to_excel(buf, index=False)
                buf.seek(0)
                supabase.storage.from_(SUPABASE_BUCKET).upload(
                    path=path,
                    file=buf.getvalue(),
                    file_options={
                        "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "upsert": "true",
                    },
                )

            nome_arquivo_df_final = f"{prefixo_competencia}/df_final_{version_id}.xlsx"
            upload_df(df_final, nome_arquivo_df_final)

            upload_df(df_juntar, f"{prefixo_competencia}/df_juntar_{version_id}.xlsx")
            upload_df(pj1, f"{prefixo_competencia}/pj1_{version_id}.xlsx")
            upload_df(seg, f"{prefixo_competencia}/seguro_pj_{version_id}.xlsx")
            upload_df(cam, f"{prefixo_competencia}/cambio_{version_id}.xlsx")
            upload_df(co_ter, f"{prefixo_competencia}/co_corretagem_terceiras_{version_id}.xlsx")
            upload_df(co_xpvp, f"{prefixo_competencia}/co_corretagem_xpvp_{version_id}.xlsx")
            upload_df(cre, f"{prefixo_competencia}/credito_{version_id}.xlsx")
            upload_df(xpcs, f"{prefixo_competencia}/xpcs_{version_id}.xlsx")
            upload_df(lan_man, f"{prefixo_competencia}/lancamentos_manuais_{version_id}.xlsx")
            upload_df(tim_rep, f"{prefixo_competencia}/times_repasses_{version_id}.xlsx")
            upload_df(lan_pro, f"{prefixo_competencia}/lancamento_produtos_{version_id}.xlsx")

        except Exception as e:
            print("Erro ao fazer upload para o Supabase:", e)
            flash("Não consegui enviar os Excels para o Supabase. Você ainda pode ver a tabela na tela.")
            nome_arquivo_df_final = None

    tabelas_fontes_dfs = {
        "PJ1 - Base Principal": pj1,
        "Seguro PJ": seg,
        "Câmbio": cam,
        "Co-corretagem Terceiras": co_ter,
        "Co-corretagem XPVP": co_xpvp,
        "Crédito": cre,
        "XPCS": xpcs,
        "Lançamentos Manuais": lan_man,
        "Times e Repasses": tim_rep,
        "Lançamento de Produtos": lan_pro,
    }

    contexto = montar_contexto_dashboard(
        df_final=df_final,
        competencia_label=competencia_label,
        caminho_df_final=nome_arquivo_df_final,
        df_juntar=df_juntar,
        tabelas_fontes_dfs=tabelas_fontes_dfs,
        fontes_keys=FONTE_NOMES,
    )

    contexto["max_total"] = contexto.pop("max_total_val")
    return render_template("resultado.html", **contexto)

# =====================================================================
# 6) DOWNLOADS
# =====================================================================

@app.route("/download")
def download():
    path = OUTPUT_FILES["df_final"]
    if not os.path.exists(path):
        flash("Arquivo df_final.xlsx não existe no servidor (use o download via Supabase).")
        return redirect(url_for("index"))
    return send_file(path, as_attachment=True)


@app.route("/api/deletar_fonte", methods=["POST"])
def api_deletar_fonte():
    if supabase is None:
        return jsonify({"ok": False, "error": "Supabase não configurado."}), 400

    df_final_path = (request.form.get("df_final_path") or "").strip()
    fonte_key = (request.form.get("fonte_key") or "").strip()

    comp, version_id = parse_comp_versionid_from_df_final_path(df_final_path)
    if not comp or not version_id:
        return jsonify({"ok": False, "error": "df_final_path inválido (precisa conter competência e versão)."}), 400

    caminhos = {
        "df_juntar": f"{comp}/df_juntar_{version_id}.xlsx",
        "pj1": f"{comp}/pj1_{version_id}.xlsx",
        "seg": f"{comp}/seguro_pj_{version_id}.xlsx",
        "cam": f"{comp}/cambio_{version_id}.xlsx",
        "co_ter": f"{comp}/co_corretagem_terceiras_{version_id}.xlsx",
        "co_xpvp": f"{comp}/co_corretagem_xpvp_{version_id}.xlsx",
        "cre": f"{comp}/credito_{version_id}.xlsx",
        "xpcs": f"{comp}/xpcs_{version_id}.xlsx",
        "lan_man": f"{comp}/lancamentos_manuais_{version_id}.xlsx",
        "tim_rep": f"{comp}/times_repasses_{version_id}.xlsx",
        "lan_pro": f"{comp}/lancamento_produtos_{version_id}.xlsx",
    }

    if fonte_key not in caminhos:
        return jsonify({"ok": False, "error": "fonte_key desconhecida."}), 400

    try:
        df_atual = carregar_excel_do_supabase(caminhos[fonte_key])
        if df_atual is None:
            df_vazio = pd.DataFrame()
        else:
            df_vazio = df_atual.iloc[0:0].copy()

        supabase_upload_df_upsert(df_vazio, caminhos[fonte_key])

        def df_ou_vazio(df):
            return df if df is not None else pd.DataFrame()

        pj1 = df_ou_vazio(carregar_excel_do_supabase(caminhos["pj1"]))
        seg = df_ou_vazio(carregar_excel_do_supabase(caminhos["seg"]))
        cam = df_ou_vazio(carregar_excel_do_supabase(caminhos["cam"]))
        co_ter = df_ou_vazio(carregar_excel_do_supabase(caminhos["co_ter"]))
        co_xpvp = df_ou_vazio(carregar_excel_do_supabase(caminhos["co_xpvp"]))
        cre = df_ou_vazio(carregar_excel_do_supabase(caminhos["cre"]))
        xpcs = df_ou_vazio(carregar_excel_do_supabase(caminhos["xpcs"]))
        lan_man = df_ou_vazio(carregar_excel_do_supabase(caminhos["lan_man"]))
        tim_rep = df_ou_vazio(carregar_excel_do_supabase(caminhos["tim_rep"]))
        lan_pro = df_ou_vazio(carregar_excel_do_supabase(caminhos["lan_pro"]))

        df_final, df_juntar = calcular_comissoes(
            pj1, seg, cam, co_ter, co_xpvp, cre, xpcs, lan_man, tim_rep, lan_pro
        )

        supabase_upload_df_upsert(df_final, df_final_path)
        supabase_upload_df_upsert(df_juntar, caminhos["df_juntar"])

        return jsonify({"ok": True, "redirect": url_for("visualizar_antigo", file=df_final_path)})

    except Exception as e:
        return jsonify({"ok": False, "error": f"Erro ao deletar/recalcular: {e}"}), 500


@app.route("/download/<nome>")
def download_excel(nome):
    path = OUTPUT_FILES.get(nome)
    if not path or not os.path.exists(path):
        flash("Arquivo não encontrado para download local.")
        return redirect(url_for("index"))
    return send_file(path, as_attachment=True)


@app.route("/download_supabase")
def download_supabase():
    nome_arquivo = request.args.get("file")

    if not nome_arquivo:
        flash("Nenhum arquivo informado para download.")
        return redirect(url_for("index"))

    if supabase is None or not SUPABASE_URL or not SUPABASE_BUCKET:
        flash("Supabase não está configurado. Não foi possível baixar o arquivo.")
        return redirect(url_for("index"))

    base_public_url = f"{SUPABASE_URL}/storage/v1/object/public/{SUPABASE_BUCKET}"
    url_arquivo = f"{base_public_url}/{nome_arquivo}"
    return redirect(url_arquivo)

# =====================================================================
# 7) MAIN LOCAL
# =====================================================================

if __name__ == "__main__":
    app.run(debug=True)
