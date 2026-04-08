import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import re
import io
import time  # ← 追加：待機時間用

st.set_page_config(layout="wide", page_title="My Stock Portfolio")

# --- 1. 保有株CSV読み込み関数 ---
def load_sbi_csv(uploaded_file):
    if uploaded_file is None:
        return None, "👈 左側のメニューから、保有株のCSVをアップロードしてください。"

    try:
        content = uploaded_file.getvalue().decode('shift_jis', errors='replace')
        lines = content.splitlines()
        
        data_list = []
        for line in lines:
            clean_line = line.replace('"', '').strip()
            parts = clean_line.split(',')

            match = re.match(r'^\s*(\d{4})\s+(.*)$', parts[0])
            if len(parts) >= 5 and match:
                try:
                    code = match.group(1).strip()
                    name = match.group(2).strip()

                    shares = float(parts[2].replace(',', '').replace(' ', '')) if parts[2].strip() else 0.0
                    avg_price = float(parts[3].replace(',', '').replace(' ', '')) if parts[3].strip() else 0.0
                    current_price = float(parts[4].replace(',', '').replace(' ', '')) if parts[4].strip() else np.nan
                    

                    data_list.append({
                        '銘柄コード': code,
                        '銘柄名称': name,
                        '保有株数': shares,
                        '取得単価': avg_price,
                        '現在値': current_price
                    })
                except Exception:
                    continue

        if not data_list:
            return None, "アップロードされたファイルから保有銘柄のデータが見つかりませんでした。"

        return pd.DataFrame(data_list), f"「{uploaded_file.name}」を正常に読み込みました！"
    except Exception as e:
        return None, f"エラーが発生しました: {e}"

# --- 2. 配当金CSV読み込み関数 ---
def load_dividend_csv(uploaded_file):
    if uploaded_file is None:
        return None, "配当金データ(DISTRIBUTION_*.csv)をアップロードしてください。"
    
    try:
        content = uploaded_file.getvalue().decode('shift_jis', errors='replace')
        lines = content.splitlines()
        
        header_idx = -1
        for i, line in enumerate(lines):
            if "受渡日" in line and "銘柄名" in line:
                header_idx = i
                break
        
        if header_idx == -1:
            return None, "CSV内に「受渡日」のヘッダーが見つかりませんでした。"
        
        df_div = pd.read_csv(io.StringIO(content), skiprows=header_idx)
        
        amount_col = [c for c in df_div.columns if "受取額" in c]
        if not amount_col:
            return None, "「受取額」の列が見つかりません。"
        amount_col = amount_col[-1] 

        df_div = df_div.dropna(subset=['受渡日', amount_col])
        df_div['受渡日'] = pd.to_datetime(df_div['受渡日'], errors='coerce')
        df_div = df_div.dropna(subset=['受渡日']) 
        df_div['年'] = df_div['受渡日'].dt.year.astype(int).astype(str) 
        
        df_div[amount_col] = df_div[amount_col].astype(str).str.replace(',', '', regex=False).str.strip().astype(float)
        
        yearly_div = df_div.groupby('年')[amount_col].sum().reset_index()
        yearly_div.rename(columns={amount_col: '受取配当金合計(円)'}, inplace=True)
        yearly_div['受取配当金合計(円)'] = yearly_div['受取配当金合計(円)'].astype(int)
        
        return yearly_div, f"「{uploaded_file.name}」から配当金実績を読み込みました！"
        
    except Exception as e:
        return None, f"配当金CSVエラー: {e}"

# --- 3. Yahoo Finance API 取得関数（キャッシュ＆リトライ＆待機時間付き） ---
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_yahoo_finance_data(tickers):
    api_data = []
    total = len(tickers)
    status_text = st.empty()
    
    for i, ticker in enumerate(tickers):
        status_text.text(f"Yahoo Financeからデータを取得中... {ticker} ({i+1}/{total})")
        
        success = False
        retries = 0
        
        # ★ 制限回避：最大3回までリトライ（再挑戦）するループ
        while not success and retries < 3:
            try:
                stock = yf.Ticker(f"{ticker}.T")
                info = stock.info
                
                # 情報が空っぽの場合はエラーを起こして意図的にリトライへ回す
                if not info or ('regularMarketPrice' not in info and 'currentPrice' not in info):
                    raise ValueError("データが取得できませんでした")

                current_price = info.get('currentPrice') or info.get('regularMarketPrice') or np.nan
                dividend = info.get('dividendRate', np.nan)
                
                # ★ バグ修正：利回りが340%などになるのを防ぐため、正確に割り算する
                if pd.notnull(current_price) and pd.notnull(dividend) and current_price > 0:
                    yield_pct = (dividend / current_price) * 100
                else:
                    yield_pct = np.nan

                api_data.append({
                    '銘柄コード': str(ticker),
                    '現在値(API)': current_price,
                    '1株配当': dividend,
                    '配当利回り(%)': yield_pct,
                    'EPS': info.get('trailingEps', np.nan),
                    '配当性向(%)': info.get('payoutRatio', 0) * 100 if pd.notnull(info.get('payoutRatio')) else np.nan,
                    'PER': info.get('trailingPE', np.nan),
                    'PBR': info.get('priceToBook', np.nan)
                })
                success = True
                time.sleep(4.0)  # ★ 取得成功時の待機を4秒に伸ばして、優しくアクセスする
                
            except Exception as e:
                retries += 1
                if retries < 3:
                    status_text.text(f" ⚠️ {ticker} 制限に引っかかりました。10秒待機して再試行します... ({retries}/2)")
                    time.sleep(10.0)  # ★ 制限に引っかかったら、しっかり10秒休ませる
                else:
                    st.warning(f" ⚠️ 銘柄 {ticker} の取得に失敗しました。")
            
    status_text.empty()
    return api_data


# ==========================================
# メイン画面の構築
# ==========================================
st.title("📊 My Stock Portfolio")

with st.sidebar:
    st.header("📂 データのアップロード")
    st.write("SBI証券からダウンロードしたCSVをここに入れてください。")
    portfolio_file = st.file_uploader("1. 保有株のCSV", type=['csv'])
    dividend_file = st.file_uploader("2. 配当金のCSV", type=['csv'])

portfolio_df, message = load_sbi_csv(portfolio_file)

if portfolio_df is None or portfolio_df.empty:
    st.info(message)
    st.stop()

st.success(f"✅ {message}")

use_api = st.toggle("🌐 最新指標(PER・PBR・EPSなど)をYahoo Financeから取得する", value=False)

df = portfolio_df.copy()
df['投資金額'] = df['取得単価'] * df['保有株数']
df['評価額'] = df['現在値'] * df['保有株数']

# APIデータの取得と結合
if use_api:
    # .tolist() を追加して、Python標準のリスト型に変換します
    unique_tickers = df['銘柄コード'].unique().tolist()  
    # 上で作ったキャッシュ付き関数を呼び出す
    fetched_data = fetch_yahoo_finance_data(unique_tickers)
    
    if fetched_data:
        stock_df = pd.DataFrame(fetched_data)
        stock_df['銘柄コード'] = stock_df['銘柄コード'].astype(str)
        df = pd.merge(df, stock_df, on='銘柄コード', how='left')

        if '現在値(API)' in df.columns:
            df['現在値(API)'] = pd.to_numeric(df['現在値(API)'], errors='coerce')
            df['現在値'] = df['現在値(API)'].fillna(df['現在値'])
            df['評価額'] = df['現在値'] * df['保有株数']
        st.success("✅ 指標の取得が完了しました！")

for col in['1株配当', '配当利回り(%)', 'EPS', '配当性向(%)', 'PER', 'PBR']:
    if col not in df.columns:
        df[col] = np.nan

df = df.sort_values(by='投資金額', ascending=True).reset_index(drop=True)

tab1, tab2, tab3 = st.tabs(["1. ポートフォリオ一覧", "2. 指標の推移", "3. 配当金管理"])

with tab1:
    if not use_api:
        st.info("💡 上のスイッチをONにすると、自動でPERなどの指標が取得・追加されます。")

    display_cols =['銘柄コード', '銘柄名称', '現在値', '取得単価', '保有株数', '投資金額', '評価額',
                    '1株配当', '配当利回り(%)', 'EPS', '配当性向(%)', 'PER', 'PBR']
    df_display = df[[c for c in display_cols if c in df.columns]].copy()

    def fmt_money(x): return f"¥{int(x):,}" if pd.notnull(x) else "-"
    def fmt_float(x): return f"{x:,.1f}" if pd.notnull(x) else "-"
    def fmt_share(x): return f"{int(x):,}" if pd.notnull(x) else "-"
    def fmt_pct(x): return f"{x:.2f}%" if pd.notnull(x) else "-"

    df_display['現在値'] = df_display['現在値'].apply(fmt_money)
    df_display['取得単価'] = df_display['取得単価'].apply(fmt_money)
    df_display['投資金額'] = df_display['投資金額'].apply(fmt_money)
    df_display['評価額'] = df_display['評価額'].apply(fmt_money)
    df_display['保有株数'] = df_display['保有株数'].apply(fmt_share)
    df_display['1株配当'] = df_display['1株配当'].apply(fmt_float)
    df_display['EPS'] = df_display['EPS'].apply(fmt_float)
    df_display['配当利回り(%)'] = df_display['配当利回り(%)'].apply(fmt_pct)

    if 'PER' in df_display.columns:
        df_display['PER'] = df_display['PER'].apply(lambda x: f"⭕️ {x:.1f}倍" if pd.notnull(x) and x <= 10 else (f"⚠️ {x:.1f}倍" if pd.notnull(x) and x >= 20 else (f"{x:.1f}倍" if pd.notnull(x) else "-")))
    if 'PBR' in df_display.columns:
        df_display['PBR'] = df_display['PBR'].apply(lambda x: f"⭕️ {x:.2f}倍" if pd.notnull(x) and x <= 1.0 else (f"⚠️ {x:.2f}倍" if pd.notnull(x) and x >= 1.5 else (f"{x:.2f}倍" if pd.notnull(x) else "-")))
    if '配当性向(%)' in df_display.columns:
        df_display['配当性向(%)'] = df_display['配当性向(%)'].apply(lambda x: f"⚠️ {x:.1f}%" if pd.notnull(x) and x >= 90 else (f"{x:.1f}%" if pd.notnull(x) else "-"))

    st.table(df_display)

with tab2:
    st.markdown("銘柄ごとの株価および過去指標の推移を確認できます。")
    options = df['銘柄コード'] + " (" + df['銘柄名称'] + ")"
    selected_option = st.selectbox("確認したい銘柄を選択", options)

    if st.button("グラフを取得・表示する"):
        if selected_option:
            ticker_code = selected_option.split(" ")[0]
            try:
                with st.spinner(f'{ticker_code} の履歴データを取得中...'):
                    stock = yf.Ticker(f"{ticker_code}.T")
                    col_a, col_b = st.columns(2)

                    with col_a:
                        st.write("📈 **過去20年の株価推移**")
                        hist = stock.history(period="20y")
                        if not hist.empty: st.line_chart(hist['Close'])
                        else: st.warning("株価データが取得できませんでした")

                    with col_b:
                        st.write("📊 **過去の配当金実績 (円)**")
                        divs = stock.dividends
                        if not divs.empty: st.bar_chart(divs.groupby(divs.index.year).sum())
                        else: st.write("配当金データがありません")

                    st.write("📊 **過去のEPS (1株当たり純利益) 推移**")
                    st.info("※ 無料APIの制限上、財務データ（EPSなど）は直近4年分程度しか取得できません。")
                    income = stock.income_stmt
                    if not income.empty and 'Basic EPS' in income.index:
                        eps_data = income.loc['Basic EPS'].dropna()
                        eps_data.index =[pd.to_datetime(d).year if isinstance(d, str) else d.year for d in eps_data.index]
                        st.bar_chart(eps_data.sort_index())
                    else:
                        st.warning("この銘柄のEPSデータは無料APIでは提供されていません。")
            except Exception as e:
                st.error(f"エラーが発生しました: {e}")

with tab3:
    st.markdown("受け取った配当金の実績です。サイドバーに配当金のCSVをアップロードしてください。")
    
    div_df, div_msg = load_dividend_csv(dividend_file)
    
    if div_df is None or div_df.empty:
        st.warning(div_msg)
    else:
        st.success(div_msg)
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("■ 年別 配当金帳簿")
            display_div_df = div_df.copy()
            display_div_df['受取配当金合計(円)'] = display_div_df['受取配当金合計(円)'].apply(lambda x: f"¥{x:,}")
            st.table(display_div_df.set_index('年'))

        with col2:
            st.write("■ 配当金推移グラフ")
            st.bar_chart(div_df.set_index('年')['受取配当金合計(円)'])
