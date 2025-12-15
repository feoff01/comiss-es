# app.py
from supabase import create_client, Client
from dotenv import load_dotenv

from flask import (
    Flask,
    render_template,
    request,
    send_file,
    redirect,
    url_for,
    flash,
)
import pandas as pd
import os
from io import BytesIO
from datetime import datetime

from supabase import create_client, Client
from dotenv import load_dotenv

from comissoes_backend import calcular_comissoes  # sua função de cálculo

# =====================================================================
# 1) CONFIGURAÇÃO BÁSICA DO FLASK
# =====================================================================

app = Flask(__name__)
app.secret_key = "segredo-muito-simples-so-pra-flash"

# Carrega variáveis de ambiente do .env (quando rodando local)
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
        # Se der algum problema, não quebra o app inteiro.
        print("Erro ao criar client do Supabase:", e)
        supabase = None
else:
    print("⚠️ SUPABASE_URL ou SUPABASE_KEY não configurados. Upload ficará desativado.")

# =====================================================================
# 3) PASTA DE OUTPUT LOCAL (APENAS PARA RODAR NA MÁQUINA / DEBUG)
# =====================================================================

# Em serverless (Vercel) o código é só leitura; se quiser salvar algo em disco,
# o lugar correto é /tmp. Localmente, usamos ./outputs.
if os.getenv("VERCEL"):
    OUTPUT_DIR = "/tmp/outputs"
else:
    OUTPUT_DIR = os.path.join(app.root_path, "outputs")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Mapeamento "nome lógico" -> caminho do arquivo LOCAL
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

def classificar_arquivos(uploaded_files):
    """
    Recebe uma lista de FileStorage (arquivos do formulário)
    e tenta mapear cada um para o papel correto:
    pj1, seg, cam, co_ter, co_xpvp, cre, xpcs, lan_man, tim_rep, lan_pro
    usando trechos do nome do arquivo.
    """
    slots = {
        "pj1": None,
        "seg": None,
        "cam": None,
        "co_ter": None,
        "co_xpvp": None,
        "cre": None,
        "xpcs": None,
        "lan_man": None,
        "tim_rep": None,
        "lan_pro": None,
    }

    usados = set()

    for f in uploaded_files:
        nome = f.filename.lower()

        def marca(chave):
            if slots[chave] is None:
                slots[chave] = f
                usados.add(nome)

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

    # qualquer arquivo que sobrar e ainda não foi usado pode ser a base PJ1
    nao_usados = [f for f in uploaded_files if f.filename.lower() not in usados]
    if nao_usados and slots["pj1"] is None:
        slots["pj1"] = nao_usados[0]

    faltando = [k for k, v in slots.items() if v is None]
    return slots, faltando


def brl(valor: float) -> str:
    """Formata número em R$ brasileiro."""
    if pd.isna(valor):
        valor = 0.0
    return f"R$ {valor:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")


# =====================================================================
# 5) ROTAS
# =====================================================================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/processar", methods=["POST"])
def processar():
    # recebe todos os arquivos (pasta ou múltiplos arquivos)
    arquivos = request.files.getlist("files")

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

    # Lê os DataFrames a partir dos arquivos classificados
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

    # chama SEU motor de cálculo
    df_final, df_juntar = calcular_comissoes(
        pj1, seg, cam, co_ter, co_xpvp, cre, xpcs, lan_man, tim_rep, lan_pro
    )

    # ----------- AJUSTE IMPORTANTE: arredondar para 2 casas -----------
    colunas_numericas = df_final.select_dtypes(include=["number"]).columns
    df_final[colunas_numericas] = df_final[colunas_numericas].round(2)

    # =================================================================
    # 5.1) SALVAR LOCALMENTE (APENAS QUANDO NÃO ESTIVER NA VERCEL)
    # =================================================================
    if not os.getenv("VERCEL"):
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

    # =================================================================
    # 5.2) ENVIAR df_final PARA O SUPABASE (PRINCIPAL DOWNLOAD)
    # =================================================================
    nome_arquivo_df_final = None

    if supabase is not None:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nome_arquivo_df_final = f"df_final_{timestamp}.xlsx"

            buffer = BytesIO()
            df_final.to_excel(buffer, index=False)
            buffer.seek(0)

            supabase.storage.from_(SUPABASE_BUCKET).upload(
                path=nome_arquivo_df_final,
                file=buffer.getvalue(),
                file_options={
                    "content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                },
            )
        except Exception as e:
            print("Erro ao fazer upload do df_final para o Supabase:", e)
            # se der erro, apenas avisa na tela, mas não quebra o resto
            flash("Não consegui enviar o Excel final para o Supabase. Você ainda pode ver a tabela na tela.")
            nome_arquivo_df_final = None
    else:
        # supabase não configurado
        nome_arquivo_df_final = None

    # ----------- Cópia apenas para exibir, formatada em pt-BR ----------
    df_display = df_final.copy()
    for col in colunas_numericas:
        df_display[col] = df_display[col].apply(
            lambda x: f"{x:,.2f}".replace(",", "v").replace(".", ",").replace("v", ".")
        )

    tabela_html = df_display.to_html(
        classes="table table-striped table-bordered table-sm dataframe",
        index=False,
    )

    # --------- TABELAS FONTES EM HTML (para exibir na aba "Fontes") ----------
    def df_to_html(df):
        return df.to_html(
            classes="table table-striped table-bordered table-sm dataframe",
            index=False,
        )

    # dicionário com os DataFrames fonte
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

    # converter para HTML para exibir no template
    tabelas_fontes = {nome: df_to_html(df) for nome, df in tabelas_fontes_dfs.items()}

    # links de download PARA USO LOCAL (arquivo salvo em disco)
    links_fontes = {
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

    # métricas iniciais
    if "Valor Total Assessor" in df_final.columns:
        total_assessores = len(df_final)
        soma_total = df_final["Valor Total Assessor"].sum()
        media_total = df_final["Valor Total Assessor"].mean()
        max_total = df_final["Valor Total Assessor"].max()
    else:
        total_assessores = len(df_final)
        soma_total = media_total = max_total = 0.0

    # df_juntar em formato para o JavaScript (árvore)
    df_juntar_registros = df_juntar.to_dict(orient="records")

    return render_template(
        "resultado.html",
        tabela=tabela_html,
        total_assessores=total_assessores,
        soma_total=brl(soma_total),
        media_total=brl(media_total),
        max_total=brl(max_total),
        tabelas_fontes=tabelas_fontes,
        links_fontes=links_fontes,
        df_juntar=df_juntar_registros,  # usado pela aba Árvore
        caminho_df_final=nome_arquivo_df_final,  # usado para download via Supabase
    )


# =====================================================================
# 6) DOWNLOADS
# =====================================================================

@app.route("/download")
def download():
    """
    Download padrão do resultado final (df_final) — VERSÃO LOCAL.
    Na Vercel, o botão principal deve usar /download_supabase.
    """
    path = OUTPUT_FILES["df_final"]
    if not os.path.exists(path):
        flash("Arquivo df_final.xlsx não existe no servidor (use o download via Supabase).")
        return redirect(url_for("index"))
    return send_file(path, as_attachment=True)


@app.route("/download/<nome>")
def download_excel(nome):
    """
    Download genérico local: /download/pj1, /download/seg, /download/df_juntar etc.
    Usado pelos botões de cada tabela de fonte (apenas quando rodando local).
    """
    path = OUTPUT_FILES.get(nome)
    if not path or not os.path.exists(path):
        flash("Arquivo não encontrado para download local.")
        return redirect(url_for("index"))
    return send_file(path, as_attachment=True)


@app.route("/download_supabase")
def download_supabase():
    """
    Faz o download do df_final que foi enviado ao Supabase.
    Aqui eu assumo que o bucket é PUBLIC.
    Se for PRIVATE, você deve gerar uma signed URL.
    """
    nome_arquivo = request.args.get("file")

    if not nome_arquivo:
        flash("Nenhum arquivo informado para download.")
        return redirect(url_for("index"))

    if supabase is None or not SUPABASE_URL or not SUPABASE_BUCKET:
        flash("Supabase não está configurado. Não foi possível baixar o arquivo.")
        return redirect(url_for("index"))

    # URL pública padrão do Supabase Storage
    base_public_url = f"{SUPABASE_URL}/storage/v1/object/public/{SUPABASE_BUCKET}"
    url_arquivo = f"{base_public_url}/{nome_arquivo}"

    # Redireciona o usuário diretamente para a URL pública do arquivo
    return redirect(url_arquivo)


# =====================================================================
# 7) MAIN LOCAL
# =====================================================================

if __name__ == "__main__":
    # Ao rodar localmente, debug=True ajuda a ver erros
    app.run(debug=True)
