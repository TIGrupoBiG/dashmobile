import streamlit as st
import oracledb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import datetime
import numpy as np
import altair as alt
import locale
#import matplotlib.pyplot as plt
import plotly.graph_objects as go
import random
import duckdb
import os

# Tentar definir a localidade para o Brasil
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    st.warning("Localidade 'pt_BR.UTF-8' não está disponível. Usando localidade padrão.")
    
# Função para formatar como moeda do Brasil
def formatar_moeda(valor):
    return locale.currency(valor, grouping=True)

# create engine
from sqlalchemy import create_engine
engine = create_engine('oracle+oracledb://', creator=lambda: connection)

# Defina a variável de ambiente ORACLE_HOME
oracledb.init_oracle_client(lib_dir=r"/home/vscode/oracle_client/instantclient_23_4")

st.set_page_config(
    page_title="Dashboard Diretoria",
    page_icon="📊",
    layout="wide",
    #initial_sidebar_state="expanded"
    )
alt.themes.enable("dark")

# connect to oracle database
connection=oracledb.connect(
    user="EDIUSER",
    password="EDIUSER",
    #dsn="10.180.200.2:1521/PROTON"#,
    dsn="187.109.221.38:1521/PROTON"#,
    #config_dir=wallet_location,
    #wallet_location=wallet_location,
    #wallet_password=wallet_pw
    )

    
def creds_entered(login, senha):
    # Verifica se o login e a senha foram fornecidos
    if not login or not senha:
        st.warning("Insira o login e a senha")
        return

    # Get the data into a DataFrame144
    query = """SELECT
                TSYS_USUARIO.TSYS_USUARIO_PK,
                TSYS_USUARIO.TSYS_NOME,
                TSYS_USUARIO.TSYS_SENHA
            FROM
                TSYS_USUARIO
                LEFT JOIN TSYS_USUARIO_ACESSO ON TSYS_USUARIO_ACESSO.TSYS_USUARIO_FK_PK = TSYS_USUARIO.TSYS_USUARIO_PK
            WHERE
                TSYS_USUARIO.TSYS_USUARIO_PK = :usuario
                AND TSYS_USUARIO.TSYS_SENHA = :senha
                AND TSYS_USUARIO.TSYS_ADMIN = 'S' 
                AND TSYS_USUARIO_ACESSO.TSYS_MENU_ITEM_PK = 'MG_REL_FAT_VENDAS_POR_DIA' 
                AND TSYS_USUARIO_ACESSO.TSYS_UNIDADE_FK_PK = 1
            ORDER BY
                1"""
    # Execute a consulta e leia o resultado em um DataFrame
    dfLogin = pd.read_sql(query, connection, params={"usuario": login, "senha": senha})

    if not dfLogin.empty:
        st.session_state["authenticated"] = True
    else:
        st.session_state["authenticated"] = False
        st.error("Usuário e senha inválidos")

def authenticate_user():
    # Verifica se o usuário já está autenticado na sessão
    if "authenticated" in st.session_state:
        if st.session_state["authenticated"]:
            return True

    # Se o usuário não estiver autenticado, mostra os campos de login e senha
    login = st.number_input(label="Login:", value=None, key="user", step=1, min_value=1)
    senha = st.text_input(label="Senha:", value=None, key="pass", type="password")

    if st.button("Login", type="primary"):
        creds_entered(login, senha)

    return False

if not authenticate_user():
    st.stop()  # Encerra a execução se o usuário não estiver autenticado



if authenticate_user():
    def espera_selecionar_datas():
        while True:
            with st.expander("Selecionar Período"):
                selected_dates = st.date_input('Selecione o intervalo de datas:',
                                                [datetime.datetime.now().replace(day=1), datetime.datetime.now()],
                                                format="YYYY/MM/DD")

            # Cria um seletor de datas para o intervalo de datas
            #selected_dates = st.sidebar.date_input('Selecione o intervalo de datas:',
            #                                       [datetime.datetime.now().replace(day=1), datetime.datetime.now()],
            #                                      format="YYYY/MM/DD")
            
            # Verifica se o usuário selecionou ambas as datas
            if len(selected_dates) == 2:
                return selected_dates[0], selected_dates[1]
            elif len(selected_dates) == 1:
                st.warning("Por favor, selecione a segunda data.")

    start_date, end_date = espera_selecionar_datas()

    # Botão para recarregar a página
    rerun_button = st.button("Atualizar", key='rerun_button')
    # Adicionando estilo para expandir o botão
    st.markdown("""
        <style>
            div[data-testid="stButton"] > button {
                width: 100%;
            }
        </style>
    """, unsafe_allow_html=True)
    # Se o botão for pressionado, recarregue a página
    if rerun_button:
        st.experimental_rerun()


    # Get the data into a DataFrame
    query = """SELECT
        TPED_UNIDADE_FK_PK AS cod_und,
        TUND_UNIDADE.TUND_FANTASIA AS unidade,
        SUM( QTD ) AS qntd_pedidos, 
        SUM( TPED_VALOR_TOTAL_PEDIDO ) - NVL(
            (
            SELECT
                SUM( TMOV_VALOR_TOTAL ) 
            FROM
                TMOV_EXTRA 
            WHERE
                TMOV_EXTRA.TMOV_UNIDADE_FK = TPED_UNIDADE_FK_PK 
                AND TMOV_EXTRA.TMOV_NATUREZA_MOVIMENTACAO = 'DC' 
                AND TMOV_ESTADO_MOVIMENTACAO = 'CL' 
                AND TMOV_APENAS_IMPRESSAO = 'N' 
                AND TMOV_EXTRA.TMOV_DATA_DOCUMENTO BETWEEN :start_date AND :end_date 
            ),
            0 
        ) AS valor_total,
        MTA_METAS.MTA_META,
        MTA_METAS.MTA_BIG_META,
        MTA_METAS.MTA_META_TKT_MEDIO
    FROM
        (
        SELECT
            TPED_PEDIDO_VENDA.TPED_UNIDADE_FK_PK,
            TPED_DATA_EMISSAO,
            COUNT( TPED_PEDIDO_VENDA.TPED_NUMERO_PEDIDO_PK ) AS QTD,
            SUM( TPED_PEDIDO_VENDA.TPED_VALOR_TOTAL_PEDIDO ) AS TPED_VALOR_TOTAL_PEDIDO 
        FROM
            TPED_PEDIDO_VENDA 
        WHERE
            TPED_PEDIDO_VENDA.TPED_UNIDADE_FK_PK IN ( '2', '3', '4', '5', '20', '9', '10', '13', '14', '18', '19' ) 
            AND TPED_PEDIDO_VENDA.TPED_DATA_EMISSAO BETWEEN :start_date AND :end_date 
            AND TPED_STATUS_PEDIDO = 'MA' 
            AND TPED_PEDIDO_VENDA.TPED_NATUREZA_MOVIMENTACAO = 'VM' 
        GROUP BY
            TPED_UNIDADE_FK_PK, TPED_DATA_EMISSAO
    UNION ALL
        SELECT
            TPED_HISTORICO_VENDA.TPED_UNIDADE_FK_PK,
            TPED_DATA_EMISSAO,
            COUNT( TPED_HISTORICO_VENDA.TPED_NUMERO_PEDIDO_PK ) AS QTD,
            SUM( TPED_HISTORICO_VENDA.TPED_VALOR_TOTAL_PEDIDO ) AS TPED_VALOR_TOTAL_PEDIDO 
        FROM
            TPED_HISTORICO_VENDA 
        WHERE
            TPED_HISTORICO_VENDA.TPED_UNIDADE_FK_PK IN ( '2', '3', '4', '5', '20', '9', '10', '13', '14', '18', '19' ) 
            AND TPED_HISTORICO_VENDA.TPED_DATA_EMISSAO BETWEEN :start_date AND :end_date 
            AND TPED_STATUS_PEDIDO = 'MA' 
            AND TPED_HISTORICO_VENDA.TPED_NATUREZA_MOVIMENTACAO = 'VM' 
        GROUP BY
            TPED_UNIDADE_FK_PK, TPED_DATA_EMISSAO
        ) 
        LEFT JOIN TUND_UNIDADE ON TUND_UNIDADE.TUND_UNIDADE_PK = TPED_UNIDADE_FK_PK
        LEFT JOIN DBAUSER.MTA_METAS ON DBAUSER.MTA_METAS.MTA_UNIDADE_FK_PK = TPED_UNIDADE_FK_PK 
              AND TO_CHAR(TRUNC(DBAUSER.MTA_METAS.MTA_MES_ANO, 'MM'), 'MMYYYY') = TO_CHAR(TRUNC(:start_date , 'MM'), 'MMYYYY')
    GROUP BY
        TPED_UNIDADE_FK_PK,
        TUND_UNIDADE.TUND_FANTASIA, 
        MTA_METAS.MTA_META,
        MTA_METAS.MTA_BIG_META,
        MTA_METAS.MTA_META_TKT_MEDIO
    ORDER BY
        valor_total,cod_und DESC"""

    # Execute a consulta e leia o resultado em um DataFrame
    df = pd.read_sql(query, engine, params={"start_date": start_date, "end_date": end_date})
    
    ##############################################################################################
    #######################################
    # VISUALIZATION METHODS
    #######################################


    def plot_metric(label, value, prefix="", suffix="", show_graph=False, color_graph=""):
        fig = go.Figure()

        fig.add_trace(
            go.Indicator(
                value=value,
                gauge={"axis": {"visible": False}},
                number={
                    "prefix": prefix,
                    "suffix": suffix,
                    "font.size": 28,
                },
                title={
                    "text": label,
                    "font": {"size": 24},
                },
            )
        )

        if show_graph:
            fig.add_trace(
                go.Scatter(
                    y=random.sample(range(0, 101), 30),
                    hoverinfo="skip",
                    fill="tozeroy",
                    fillcolor=color_graph,
                    line={
                        "color": color_graph,
                    },
                )
            )

        fig.update_xaxes(visible=False, fixedrange=True)
        fig.update_yaxes(visible=False, fixedrange=True)
        fig.update_layout(
            # paper_bgcolor="lightgrey",
            margin=dict(t=30, b=0),
            showlegend=False,
            plot_bgcolor="white",
            height=100,
        )

        st.plotly_chart(fig, use_container_width=True)


    def plot_gauge(
        indicator_number, indicator_color, indicator_suffix, indicator_title, max_bound, Meta, bigMeta
    ):
    # Criação do gráfico Gauge
        fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=indicator_number,
        title={"text": indicator_title, "font": {"size": 23}},
        #subtitle={'text': "Subtítulo"},
        domain={'x': [0, 1], 'y': [0, 1]},
        number={
                "prefix": indicator_suffix,
                #"suffix": indicator_suffix,
                "font.size": 35,
                "valueformat": ",.2f"
        }, 
        gauge={'axis': {'range': [None, bigMeta + bigMeta*0.05]},
            'bar': {'color': indicator_color},
            'steps': [
                {'range': [0, Meta], 'color': "#6196ee"},
                {'range': [Meta, bigMeta + bigMeta*0.05], 'color': "#59bb71"}],
            #'threshold': { 'line': {'color': indicator_color, 'width': 4}, 'thickness': 0.75, 'value': indicator_number}
            }
        ))    
        fig.add_annotation(
            x=0.5, y=0.4,
            xref='paper',yref='paper',
            text= 'Meta: ' + str(formatar_moeda(Meta)),
            font=dict(size=12),
            showarrow=False
        )
        fig.add_annotation(
            x=0.5, y=0.24,
            xref='paper',yref='paper',
            text= 'BiG Meta: ' + str(formatar_moeda(bigMeta)),
            font=dict(size=12),
            showarrow=False
        )
        
        
        fig.update_layout(
            #paper_bgcolor="#262730",
            height=200,
            margin=dict(l=10, r=10, t=50, b=10, pad=8),
            
        )
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")


    def plot_top_right():
        sales_data = duckdb.sql(
            f"""
            WITH sales_data AS (
                UNPIVOT ( 
                    SELECT 
                        Scenario,
                        business_unit,
                        {','.join(all_months)} 
                        FROM df 
                        WHERE Year='2023' 
                        AND Account='Sales' 
                    ) 
                ON {','.join(all_months)}
                INTO
                    NAME month
                    VALUE sales
            ),

            aggregated_sales AS (
                SELECT
                    Scenario,
                    business_unit,
                    SUM(sales) AS sales
                FROM sales_data
                GROUP BY Scenario, business_unit
            )
            
            SELECT * FROM aggregated_sales
            """
        ).df()

        fig = px.bar(
            sales_data,
            x="business_unit",
            y="sales",
            color="Scenario",
            barmode="group",
            text_auto=".2s",
            title="Sales for Year 2023",
            height=400,
        )
        fig.update_traces(
            textfont_size=12, textangle=0, textposition="outside", cliponaxis=False
        )
        st.plotly_chart(fig, use_container_width=True)


    def plot_bottom_left():
        sales_data = duckdb.sql(
            f"""
            WITH sales_data AS (
                SELECT 
                Scenario,{','.join(all_months)} 
                FROM df 
                WHERE Year='2023' 
                AND Account='Sales'
                AND business_unit='Software'
            )

            UNPIVOT sales_data 
            ON {','.join(all_months)}
            INTO
                NAME month
                VALUE sales
        """
        ).df()

        fig = px.line(
            sales_data,
            x="month",
            y="sales",
            color="Scenario",
            markers=True,
            text="sales",
            title="Monthly Budget vs Forecast 2023",
        )
        fig.update_traces(textposition="top center")
        st.plotly_chart(fig, use_container_width=True)


    def plot_bottom_right():
        sales_data = duckdb.sql(
            f"""
            WITH sales_data AS (
                UNPIVOT ( 
                    SELECT 
                        Account,Year,{','.join([f'ABS({month}) AS {month}' for month in all_months])}
                        FROM df 
                        WHERE Scenario='Actuals'
                        AND Account!='Sales'
                    ) 
                ON {','.join(all_months)}
                INTO
                    NAME year
                    VALUE sales
            ),

            aggregated_sales AS (
                SELECT
                    Account,
                    Year,
                    SUM(sales) AS sales
                FROM sales_data
                GROUP BY Account, Year
            )
            
            SELECT * FROM aggregated_sales
        """
        ).df()

        fig = px.bar(
            sales_data,
            x="Year",
            y="sales",
            color="Account",
            title="Actual Yearly Sales Per Account",
        )
        st.plotly_chart(fig, use_container_width=True)

    ##############################################################################################



    #######################
    # Dashboard Main Panel
    st.title('Dashboard Diretoria')
    # Exibindo o componente de métrica com o valor formatado
    st.metric(label="Total Geral", value=formatar_moeda(df['valor_total'].sum()), delta="Meta: " + formatar_moeda(df['mta_meta'].sum()) + " - BigMeta: " + formatar_moeda(df['mta_big_meta'].sum()))

    #Ordenando o dataframe
    df = df.sort_values(by='cod_und', ascending=True)    


    st.markdown('#### Ranking' )
    df_selected_year_sorted = df.sort_values(by="valor_total", ascending=False)
    df_selected_year_sorted['valor_total_formatado'] = df_selected_year_sorted['valor_total'].apply(formatar_moeda)
    st.dataframe(df_selected_year_sorted,
                    column_order=("unidade", "qntd_pedidos", "valor_total_formatado", "valor_total" ),
                    hide_index=True,
                    width=None,
                    column_config={
                    "unidade": st.column_config.TextColumn(
                        "Unidade",
                    ),
                    "qntd_pedidos": st.column_config.ProgressColumn(
                        "Qntd",
                        format="%f",
                        min_value=0,
                        max_value=max(df_selected_year_sorted.qntd_pedidos),
                        width=100,
                        ),
                    "valor_total_formatado": st.column_config.TextColumn(
                        "Valor Total",
                        ),
                    "valor_total": st.column_config.ProgressColumn(
                        "Ranking Vendas",
                        format=" ",
                        min_value=0,
                        max_value=max(df_selected_year_sorted.valor_total),
                        )}
                    )

    #Gráicos de vendas
    # Iterando sobre as linhas
    for index, row in df.iterrows():
        # Extrair valores do DataFrame
        codUnidade = row['cod_und']
        Unidade = row['unidade']
        ValorVendido = row['valor_total']
        MetaMes = row['mta_meta']
        plot_gauge(ValorVendido, "#262626", "R$", str(Unidade) ,  row['mta_meta'], row['mta_meta'], row['mta_big_meta'])
        

    #plot_gauge(df.loc[df['cod_und'] == 2, 'valor_total'].iloc[0], "#000080", "R$", "Lojas BiG - Meta: " + formatar_moeda(349000.00), 349000.00)
    #plot_gauge(df.loc[df['cod_und'] == 3, 'valor_total'].iloc[0], "#D2691E", "R$", "Lojas Tabajara - Meta " + formatar_moeda(680000.00), 680000.00)
    #plot_gauge(df.loc[df['cod_und'] == 4, 'valor_total'].iloc[0], "#0068C9", "R$", "BiG Atacadão - Meta " + formatar_moeda(489000.00), 489000.00)
    #plot_gauge(df.loc[df['cod_und'] == 7, 'valor_total'].iloc[0], "#FFD700", "R$", "10&Cia Jaguaquara - Meta " + formatar_moeda(559000.00), 559000.00)
    #plot_gauge(df.loc[df['cod_und'] == 9, 'valor_total'].iloc[0], "#FFD700", "R$", "10&Cia Ilhéus - Meta " + formatar_moeda(1590000.00), 1590000.00)
    #plot_gauge(df.loc[df['cod_und'] == 10, 'valor_total'].iloc[0], "#FFD700", "R$", "10&Cia Itabuna 01 - Meta " + formatar_moeda(790000.00), 790000.00)
    #plot_gauge(df.loc[df['cod_und'] == 13, 'valor_total'].iloc[0], "#FFD700", "R$", "10&Cia Itabuna 02 - Meta " + formatar_moeda(1257000.00), 1257000.00)
    #plot_gauge(df.loc[df['cod_und'] == 14, 'valor_total'].iloc[0], "#FFD700", "R$", "10&Cia Jequié - Meta " + formatar_moeda(769000.00), 769000.00)
    #plot_gauge(df.loc[df['cod_und'] == 18, 'valor_total'].iloc[0], "#FFD700", "R$", "10&Cia Ipiaú - Meta " + formatar_moeda(890000.00), 890000.00)
    #plot_gauge(df.loc[df['cod_und'] == 19, 'valor_total'].iloc[0], "#FFD700", "R$", "10&Cia VCA - Meta " + formatar_moeda(1390000.00), 1390000.00)


        

    

