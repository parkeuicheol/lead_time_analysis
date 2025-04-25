import streamlit as st
import pandas as pd
import numpy as np
from functools import reduce
import seaborn as sns
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = 'Malgun Gothic'
import matplotlib as mpl
mpl.rcParams['axes.unicode_minus'] = False
import io
import base64

# 캐싱된 데이터 로드 및 처리 함수
@st.cache_data
def load_data():
    # 1) 데이터 로드
    results_df = pd.read_parquet('250402_results_df.parquet')
    master_table = pd.read_excel('1.탄합선재_탄합봉강_MASTER_TABLE.xlsx')

    # 2) 필터링: 탄합선재 + 탄합봉강
    cond_A = results_df['대강종명'] == '탄합강'
    cond_B = results_df['주문형상'].isin(['WR','RB','FB','SB','HB'])
    df = results_df[cond_A & cond_B]

    # MARAGING 포함 행 추가
    df = pd.concat([df, results_df[results_df['사내강종명'].str.contains('MARAGING')]], ignore_index=True)
    df = df[~df['수요가명'].str.contains('강관영업팀')].reset_index(drop=True)

    # 그룹별 최댓값 행 추출
    df_max = df.loc[df.groupby('LOT_NO')['공정순위'].idxmax(), ['LOT_NO','공정순위']]
    df = pd.merge(df, df_max, on=['LOT_NO','공정순위'], how='inner')

    # 제조공기 계산
    df['생산의뢰년월'] = pd.to_datetime(df['생산의뢰년월'])
    df['제조공기(입고일-생산의뢰년월일)'] = (df['생산정보_작업일자'] - df['생산의뢰년월']).dt.days

    # 수요가형상주문강종 KEY 생성
    df['수요가형상주문강종'] = df['수요가명'].str.strip() + df['주문형상'].str.strip() + df['주문강종명'].str.strip()

    # 방산구분 생성 (value_1 lookup)
    lookup1 = dict(zip(master_table['key'], master_table['value_1']))
    def classify1(r):
        res = lookup1.get(r['수요가형상주문강종'])
        return res if res else ('탄합선재' if r['주문형상']=='WR' else '탄합봉강')
    df['방산구분'] = df.apply(classify1, axis=1)

    # 제품구분 생성 (value_2 lookup)
    lookup2 = dict(zip(master_table['key'], master_table['value_2']))
    def classify2(r):
        if r['방산구분']=='방산': return lookup2.get(r['수요가형상주문강종'])
        if r['방산구분']=='탄합선재': return '탄합선재_비열처리' if pd.isna(r['열처리']) or r['열처리']=='' else '탄합선재_열처리'
        if r['HEAT_NO_구분']=='ESR_HEAT': return '탄합봉강_ESR'
        if pd.isna(r['열처리']) or r['열처리']=='': return '탄합봉강_비열처리'
        return '탄합봉강_QT' if r['열처리']=='QT' else '탄합봉강_열처리'
    df['제품구분'] = df.apply(classify2, axis=1)

    # KEY 컬럼 생성
    df['KEY'] = df['제품구분'].str.strip() + '_' + df['품종'].str.strip() + '_' + df['주문형상'].str.strip() + '_' + df['표면'].str.strip()

    # 통계치 그룹핑
    agg_funcs = {
        'LOT_NO': pd.Series.nunique,
        '입고중량': 'sum',
        '제조공기(입고일-생산의뢰년월일)': ['median', 'mean', lambda x: x.quantile(0.25), lambda x: x.quantile(0.75)]
    }
    stats = df.groupby('KEY').agg(agg_funcs)
    stats.columns = ['KEY별 LOT 갯수','KEY 총 중량','제조공기_중앙값','제조공기_단순평균','제조공기_1분위수','제조공기_3분위수']
    stats = stats.reset_index()

    # 중량가중평균/표준편차 계산
    df = df.merge(stats[['KEY','KEY 총 중량']], on='KEY', how='left')
    df['가중계수'] = df['입고중량']/df['KEY 총 중량']
    df['제조공기*가중'] = df['제조공기(입고일-생산의뢰년월일)']*df['가중계수']
    wmean = df.groupby('KEY')['제조공기*가중'].sum().reset_index(name='KEY별 중량가중평균')
    df = df.merge(wmean, on='KEY', how='left')
    df['편차제곱*중량'] = df['입고중량']*((df['제조공기(입고일-생산의뢰년월일)']-df['KEY별 중량가중평균'])**2)
    var = df.groupby('KEY')['편차제곱*중량'].sum().reset_index(name='분산합')
    var = var.merge(df.groupby('KEY')['입고중량'].sum().reset_index(name='총중량'), on='KEY')
    var['중량가중_표준편차'] = np.sqrt(var['분산합']/var['총중량'])

    # IQR 확장 평균
    def avg_iqr(s):
        q1, q3 = s.quantile(0.25), s.quantile(0.75)
        iqr = q3-q1
        return s[(s>=q1-1.5*iqr)&(s<=q3+1.5*iqr)].mean()
    ext_iqr = df.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].apply(avg_iqr).reset_index(name='IQR확장평균')

    # merge all
    merged = stats.merge(wmean, on='KEY')
    merged = merged.merge(var[['KEY','중량가중_표준편차']], on='KEY')
    merged = merged.merge(ext_iqr, on='KEY')

    # boxplot 이미지 생성 (이상치 포함/숨김)
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(6,2));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('off'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode()

    imgs = [{'KEY':k,
             '박스(원본)':f'<img src="data:image/png;base64,{make_img(g)}"/>' ,
             '박스(숨김)':f'<img src="data:image/png;base64,{make_img(g,False)}"/>'}
            for k,g in df.groupby('KEY')['제조공기(입고일-생산의뢰년월일)']]
    img_df = pd.DataFrame(imgs)
    final_df = merged.merge(img_df, on='KEY')

    return final_df

# Streamlit 앱
st.set_page_config(page_title="탄합선재_탄합봉강 분석", layout="wide")
st.title("탄합선재·탄합봉강 입고 분석 결과")

# 데이터 로드 및 처리
with st.spinner("데이터 로드 중..."):
    df = load_data()

# 테이블 화면 출력
st.markdown("### 분석 결과 테이블 (박스플롯 이미지 포함)")
# HTML 테이블을 사용해 이미지가 보이도록 렌더링
st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)
