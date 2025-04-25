import streamlit as st
import pandas as pd
import numpy as np
from functools import reduce
import seaborn as sns
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = 'Malgun Gothic'
import matplotlib as mpl
mpl.rcParams['axes.unicode_minus'] = False
import io, base64

# ------------------------------------------------------------------
# 엑셀 생성: 이미지 포함해서 Bytes 반환
# ------------------------------------------------------------------
def to_excel_with_images(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Sheet1", index=False)
        workbook  = writer.book
        worksheet = writer.sheets["Sheet1"]

        img_cols = ["박스(원본)", "박스(숨김)"]
        for col_name in img_cols:
            if col_name not in df.columns:
                continue
            col_idx = df.columns.get_loc(col_name)
            for row_i, html in enumerate(df[col_name]):
                if pd.isna(html):
                    continue
                try:
                    b64 = html.split("base64,")[1].split('"')[0]
                except:
                    continue
                img_data = base64.b64decode(b64)
                img_buf  = io.BytesIO(img_data)
                worksheet.insert_image(
                    row_i + 1, col_idx,   # +1: 헤더 행
                    "",                   # 파일명 대신
                    {
                        "image_data": img_buf,
                        "x_scale": 0.5,
                        "y_scale": 0.5,
                        "object_position": 1
                    }
                )
    output.seek(0)
    return output.read()

# ------------------------------------------------------------------
# 데이터 로드 & 처리 (캐시 적용)
# ------------------------------------------------------------------
@st.cache_data
def load_data():
    df = pd.read_parquet('first_item.parquet')
    master_table = pd.read_parquet('master_table.parquet')

    # 그룹별 최댓값 행 추출
    df_max = df.loc[df.groupby('LOT_NO')['공정순위'].idxmax(), ['LOT_NO','공정순위']]
    df = pd.merge(df, df_max, on=['LOT_NO','공정순위'], how='inner')

    # 제조공기 계산
    df['생산의뢰년월'] = pd.to_datetime(df['생산의뢰년월'])
    df['제조공기(입고일-생산의뢰년월일)'] = (
        df['생산정보_작업일자'] - df['생산의뢰년월']
    ).dt.days

    # KEY 컬럼 생성 로직 (생략 가능하니 그대로)
    df['수요가형상주문강종'] = (
        df['수요가명'].str.strip()
        + df['주문형상'].str.strip()
        + df['주문강종명'].str.strip()
    )
    lookup1 = dict(zip(master_table['key'], master_table['value_1']))
    lookup2 = dict(zip(master_table['key'], master_table['value_2']))

    def classify1(r):
        res = lookup1.get(r['수요가형상주문강종'])
        return res if res else ('탄합선재' if r['주문형상']=='WR' else '탄합봉강')
    df['방산구분'] = df.apply(classify1, axis=1)

    def classify2(r):
        if r['방산구분']=='방산': 
            return lookup2.get(r['수요가형상주문강종'])
        if r['방산구분']=='탄합선재':
            return '탄합선재_비열처리' if pd.isna(r['열처리']) or r['열처리']=='' else '탄합선재_열처리'
        if r['HEAT_NO_구분']=='ESR_HEAT': 
            return '탄합봉강_ESR'
        if pd.isna(r['열처리']) or r['열처리']=='': 
            return '탄합봉강_비열처리'
        return '탄합봉강_QT' if r['열처리']=='QT' else '탄합봉강_열처리'
    df['제품구분'] = df.apply(classify2, axis=1)

    df['KEY'] = (
        df['제품구분'].str.strip() + '_'
        + df['품종'].str.strip() + '_'
        + df['주문형상'].str.strip() + '_'
        + df['표면'].str.strip()
    )

    # 통계치, 중량가중평균/표준편차, IQR확장평균 계산 (생략)
    # ... (기존 load_data 본문 그대로 복사) ...

    return final_df  # merged + img_df 가 합쳐진 최종 DataFrame

# ------------------------------------------------------------------
# Streamlit UI
# ------------------------------------------------------------------
st.set_page_config(page_title="탄합선재_탄합봉강 분석", layout="wide")
st.title("탄합선재·탄합봉강 입고 분석 결과")

# 1) 데이터 로드
with st.spinner("데이터 로드 중..."):
    df = load_data()

# 2) KEY 필터링
all_keys = df['KEY'].unique().tolist()
selected = st.sidebar.multiselect("🔑 필터할 KEY 선택", all_keys, default=all_keys[:3])
if selected:
    df = df[df['KEY'].isin(selected)]
else:
    st.sidebar.warning("하나 이상의 KEY를 선택해주세요.")

# 3) 테이블 출력
st.markdown("### 분석 결과 테이블 (박스플롯 이미지 포함)")
st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)

# 4) 엑셀 다운로드 버튼
excel_data = to_excel_with_images(df)
st.download_button(
    label="📥 엑셀로 다운로드",
    data=excel_data,
    file_name="탄합선재_탄합봉강_분석결과.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
