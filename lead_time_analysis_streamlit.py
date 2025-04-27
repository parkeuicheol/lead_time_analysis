import streamlit as st
import pandas as pd
import numpy as np
from functools import reduce
import seaborn as sns
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = 'Malgun Gothic'
import matplotlib as mpl
mpl.rcParams['axes.unicode_minus'] = False
import io, base64, os
import xlsxwriter
from IPython.display import HTML
import shutil
from pyexcelerate import Workbook
import datetime

# ------------------------------------------------------------------
# 엑셀 생성: 이미지 포함해서 Bytes 반환
# ------------------------------------------------------------------
def to_excel_with_images(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Sheet1", index=False)
        workbook = writer.book
        worksheet = writer.sheets["Sheet1"]
        img_cols = ["BOX PLOT(원본)", "BOX PLOT(이상치 숨김)"]
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
                img_buf = io.BytesIO(img_data)
                worksheet.insert_image(
                    row_i + 1, col_idx,
                    "",
                    {"image_data": img_buf, "x_scale": 0.5, "y_scale": 0.5, "object_position": 2}
                )
    output.seek(0)
    return output.read()

# ------------------------------------------------------------------
# 데이터 로드 & 처리 (1. 탄합선재, 탄합봉강)
# ------------------------------------------------------------------
@st.cache_data
def load_data_tan():
    '''
    1) 탄합선재 + 탄합봉강
    - 강종대분류: A,B,C,K,M (대강종명 == '탄합강')
    - 형상(주문형상) : WR, RB, FB, SB, HB 
    '''
    # 0) raw_data parquet file import
    탄합선재_탄합봉강 = pd.read_parquet('1.탄합선재_탄합봉강.parquet')
    master_table = pd.read_parquet('master_table.parquet')
    
    # 1) 그룹별 최댓값 행 추출
    df_max = 탄합선재_탄합봉강.loc[탄합선재_탄합봉강.groupby('LOT_NO')['공정순위'].idxmax(), ['LOT_NO','공정순위']]
    
    # 2) 원본 데이터프레임과 df_max inner merge
    탄합선재_탄합봉강_입고 = pd.merge(탄합선재_탄합봉강, df_max, on=['LOT_NO','공정순위'], how='inner')
    
    # 3) '생산정보_작업일자' 컬럼을 datetime 형식으로 변환 및 '생산의뢰년월' 컬럼과의 차이 계산
    탄합선재_탄합봉강_입고['생산의뢰년월'] = pd.to_datetime(탄합선재_탄합봉강_입고['생산의뢰년월'])
    탄합선재_탄합봉강_입고['제조공기(입고일-생산의뢰년월일)'] = (탄합선재_탄합봉강_입고['생산정보_작업일자'] - 탄합선재_탄합봉강_입고['생산의뢰년월']).dt.days
    
    # 4) 파생변수 생성
    # 각 컬럼 데이터의 공백을 제거하고, 문자열로 변환하여 KEY 생성
    탄합선재_탄합봉강_입고['수요가형상주문강종'] = (탄합선재_탄합봉강_입고['수요가명'].str.strip() + 
                                 탄합선재_탄합봉강_입고['주문형상'].str.strip() + 
                                 탄합선재_탄합봉강_입고['주문강종명'].str.strip()
                                 )
    
    # 예시: lookup 테이블 DataFrame (데이터프레임 'DATA 분류 KEY 및 요청사항'에서 N열~P열 사용)
    # 여기서는 첫 번째 열을 key, 두 번째 열을 value로 사용한다고 가정합니다.
    lookup1 = dict(zip(master_table['key'], master_table['value_1']))
        
    def classify1(r):
        # lookup_dict에서 "수요가형상주문강종" 값을 key로 하여 값을 가져옵니다.
        res = lookup1.get(r['수요가형상주문강종'])
        # # "주문형상" 값에 따라 값을 지정
        return res if res else ('탄합선재' if r['주문형상'] == 'WR' else '탄합봉강')
    # 각 행마다 classify1 함수를 적용하여 새로운 열 생성
    탄합선재_탄합봉강_입고['방산구분'] = 탄합선재_탄합봉강_입고.apply(classify1, axis=1)
    
    
    # 여기서는 첫 번째 열을 key, 세 번째 열을 value로 사용한다고 가정합니다.
    lookup2 = dict(zip(master_table['key'], master_table['value_2']))

    def classify2(r):
        # 조건 1: "방산 구분"이 "방산"일 경우
        if r['방산구분']=='방산':
            return lookup2.get(r['수요가형상주문강종'])
        # 조건 2: "방산 구분"이 "탄합선재"인 경우
        if r['방산구분']=='탄합선재':
            return '탄합선재_비열처리' if pd.isna(r['열처리']) or r['열처리']=='' else '탄합선재_열처리'
        # "HEAT_NO_구분"가 "ESR_HEAT"인 경우
        if r['HEAT_NO_구분']=='ESR_HEAT':
            return '탄합봉강_ESR'
        # "열처리"값이 비어있으면 "탄합봉강_비열처리"
        if pd.isna(r['열처리']) or r['열처리']=='':
            return '탄합봉강_비열처리'
        return '탄합봉강_QT' if r['열처리']=='QT' else '탄합봉강_열처리' # "열처리"값이 "QT"이면 "탄합봉강_QT", 그 외면 "탄합봉강_열처리"
    
    # 각 행마다 classify2 함수를 적용하여 새로운 '결과' 컬럼 생성
    탄합선재_탄합봉강_입고['제품구분'] = 탄합선재_탄합봉강_입고.apply(classify2, axis=1)

    # 각 컬럼 데이터의 공백을 제거하고, 문자열로 변환하여 KEY 생성
    탄합선재_탄합봉강_입고['KEY'] = (
        탄합선재_탄합봉강_입고['제품구분'].str.strip() + '_'
        + 탄합선재_탄합봉강_입고['품종'].str.strip() + '_'
        + 탄합선재_탄합봉강_입고['주문형상'].str.strip() + '_'
        + 탄합선재_탄합봉강_입고['표면'].str.strip()
        )
    
    # 5) 통계치 계산
    # A컬럼을 그룹핑하여 각 그룹별 B컬럼의 고유값(nunique) 갯수를 계산 후, 결과를 새로운 DataFrame 생성
    result_A = 탄합선재_탄합봉강_입고.groupby('KEY')['LOT_NO'].nunique().reset_index(name='KEY별 LOT 갯수')

    # A컬럼을 그룹핑하여 각 그룹별 B컬럼의 합산(sum)을 계산 후, 결과를 새로운 DataFrame 생성
    result_B = 탄합선재_탄합봉강_입고.groupby('KEY')['입고중량'].sum().reset_index(name='KEY 총 중량')

    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 중앙값을 계산하고 결과를 새로운 DataFrame 생성
    result_C = 탄합선재_탄합봉강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].median().reset_index(name='제조공기_중앙값')

    # A컬럼을 그룹핑하여 각 그룹별 B 컬럼의 1분위수(25th percentile)를 계산하고,
    # 그 결과를 새로운 DataFrame으로 생성합니다.
    result_D = 탄합선재_탄합봉강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].quantile(0.25).reset_index(name='제조공기_1분위수')

    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 75번째 백분위수(3분위수)를 계산 후,
    # 결과를 새로운 DataFrame으로 생성합니다.
    result_E = 탄합선재_탄합봉강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].quantile(0.75).reset_index(name='제조공기_3분위수')

    # 그룹별로 (Q1 - 1.5×IQR) ~ (Q3 + 1.5×IQR) 구간에 해당하는 값들의 평균을 계산하는 함수
    def avg_iqr(series):
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3-q1
        return series[(series>=q1-1.5*iqr)&(series<=q3+1.5*iqr)].mean()
    result_F = 탄합선재_탄합봉강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].apply(avg_iqr).reset_index(name='제조공기_(Q1-1.5IQR~Q3+1.5IQR)_평균')
    
    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 평균값을 계산하고 결과를 새로운 DataFrame 생성
    result_G = 탄합선재_탄합봉강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].mean().reset_index(name='제조공기_단순평균')
    
    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 중량가중평균을 계산하고 결과를 새로운 DataFrame 생성
    탄합선재_탄합봉강_입고 = 탄합선재_탄합봉강_입고.merge(result_B, on='KEY')
    탄합선재_탄합봉강_입고['가중계수'] = 탄합선재_탄합봉강_입고['입고중량']/탄합선재_탄합봉강_입고['KEY 총 중량']
    탄합선재_탄합봉강_입고['제조공기*가중계수'] = 탄합선재_탄합봉강_입고['제조공기(입고일-생산의뢰년월일)']*탄합선재_탄합봉강_입고['가중계수']
    result_H = 탄합선재_탄합봉강_입고.groupby('KEY')['제조공기*가중계수'].sum().reset_index(name='KEY별 중량가중평균')

    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 중량가중평균을 계산하고 결과를 새로운 DataFrame 생성
    탄합선재_탄합봉강_입고 = 탄합선재_탄합봉강_입고.merge(result_H, on='KEY')
    탄합선재_탄합봉강_입고['제조공기-중량가중평균'] = 탄합선재_탄합봉강_입고['제조공기(입고일-생산의뢰년월일)'] - 탄합선재_탄합봉강_입고['KEY별 중량가중평균']
    탄합선재_탄합봉강_입고['제조공기-중량가중평균^2'] = 탄합선재_탄합봉강_입고['제조공기-중량가중평균'] ** 2
    탄합선재_탄합봉강_입고['제조공기-중량가중평균^2 * 각 LOT중량'] = 탄합선재_탄합봉강_입고['입고중량'] * 탄합선재_탄합봉강_입고['제조공기-중량가중평균^2']

    # 그룹핑한 후, B와 C 컬럼의 합계를 계산    
    result_I = 탄합선재_탄합봉강_입고.groupby('KEY').agg({'제조공기-중량가중평균^2 * 각 LOT중량':'sum','입고중량':'sum'}).reset_index()

    # 각 그룹별로 B 합계값을 C 합계값으로 나눈 값(비율)을 계산하여 새로운 컬럼 생성
    result_I['중량가중분산'] = result_I['제조공기-중량가중평균^2 * 각 LOT중량'] / result_I['입고중량']
    
    # 특정컬럼의 제곱근을 계산하여 새로운 파생변수 "특정컬럼_제곱근" 생성
    result_I['중량가중_표준편차'] = np.sqrt(result_I['중량가중분산'])

    # 8) final summary dataframe 생성
    # 데이터프레임 리스트 생성
    dfs = [result_A, result_B, result_C, result_D, result_E, result_F, result_G, result_H, result_I]
    merged_df = reduce(lambda l,r: pd.merge(l,r,on='KEY'), dfs)
    
    # 9) boxplot 이미지 생성
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(8,4));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('off'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode()
    imgs = [{'KEY':k,
             'BOX PLOT(원본)':f'<img src="data:image/png;base64,{make_img(g)}"/>',
             'BOX PLOT(이상치 숨김)':f'<img src="data:image/png;base64,{make_img(g,False)}"/>'}
            for k,g in 탄합선재_탄합봉강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)']]
    img_df = pd.DataFrame(imgs)

    # 10) final summary dataframe과 img_df merge
    merged_df = merged_df.merge(img_df, on='KEY')

    # 11) KEY별 LOT 갯수 기준으로 정렬
    merged_df = merged_df.sort_values(by='KEY별 LOT 갯수', ascending=False).reset_index(drop=True)
    return merged_df

# ------------------------------------------------------------------
# 데이터 로드 & 처리 (2. STS봉강_특수합금)
# ------------------------------------------------------------------
@st.cache_data
def load_data_sts():
    '''
    2) STS봉강 + 특수합금
    - 강종대분류: S, V
    - 형상: RB, FB, SB, HB
    '''
    # 0) raw_data parquet file import
    STS봉강_특수합금 = pd.read_parquet('2.STS봉강_특수합금.parquet')
    
    # 1) 그룹별(공정순위가 최대인 행)의 인덱스를 구한 뒤, 해당 행만 추출
    df_max = STS봉강_특수합금.loc[STS봉강_특수합금.groupby('LOT_NO')['공정순위'].idxmax(), ['LOT_NO','공정순위']]
    
    # 2) 원본 데이터프레임과 df_max inner merge
    STS봉강_특수합금_입고 = pd.merge(STS봉강_특수합금, df_max, on=['LOT_NO','공정순위'], how='inner')
    
    # 3) '생산정보_작업일자' 컬럼을 datetime 형식으로 변환 및 '생산의뢰년월' 컬럼과의 차이 계산
    STS봉강_특수합금_입고['생산의뢰년월'] = pd.to_datetime(STS봉강_특수합금_입고['생산의뢰년월'])
    STS봉강_특수합금_입고['제조공기(입고일-생산의뢰년월일)'] = (STS봉강_특수합금_입고['생산정보_작업일자'] - STS봉강_특수합금_입고['생산의뢰년월']).dt.days
    
    # 4) 파생변수 생성 (열처리_구분)
    # 조건 정의
    conditions = [
        (STS봉강_특수합금_입고['열처리']=='NH') | (STS봉강_특수합금_입고['열처리'].isna()),
        (STS봉강_특수합금_입고['열처리']=='QT')
    ]
    
    # 각 조건에 해당하는 값 목록
    choices = ['N',  # 첫 번째 조건에 해당하면 'N'
               'Y(QT)' # 두 번째 조건에 해당하면 'Y(QT)'
               ]
    
    # 조건에 맞지 않는 경우 'Y'
    STS봉강_특수합금_입고['열처리_구분'] = np.select(conditions, choices, default='Y')
    
    # 5) 파생변수 생성 (형상_구분)
    # 조건 정의
    conditions = [(STS봉강_특수합금_입고['주문형상']=='FB')]
    
    # 각 조건에 해당하는 값 목록
    choices = ['FB']
    
    # 조건에 맞지 않는 경우 'RB'
    STS봉강_특수합금_입고['형상_구분'] = np.select(conditions, choices, default='RB')
    
    # 5) 파생변수 생성 (특수제강_구분)
    # 조건 설정
    conditions = [
        (STS봉강_특수합금_입고['HEAT_NO_구분']=='ESR_HEAT') & (STS봉강_특수합금_입고['모HEAT_NO'].str.startswith('K')),
        (STS봉강_특수합금_입고['HEAT_NO_구분']=='VAR_HEAT') & (STS봉강_특수합금_입고['모HEAT_NO'].str.startswith('K')),
        (STS봉강_특수합금_입고['HEAT_NO_구분'].isin(['소형_VIM_HEAT','중형_VIM_HEAT'])),
        (STS봉강_특수합금_입고['HEAT_NO_구분']=='VAR_HEAT') & (STS봉강_특수합금_입고['모HEAT_NO'].str.startswith('N')),
        (STS봉강_특수합금_입고['HEAT_NO_구분']=='ESR_HEAT') & (STS봉강_특수합금_입고['모HEAT_NO'].str.startswith('N'))
    ]
    
    # 각 조건에 해당하는 값 목록
    choices = ['VIM-ESR','VIM-VAR','VIM','VAR','ESR']
    
    # 조건에 맞지 않는 경우 default로 None 할당
    STS봉강_특수합금_입고['특수제강_구분'] = np.select(conditions, choices, default='None')
    
    # 6) 파생변수 생성 : 각 컬럼 데이터의 공백을 제거하고, 문자열로 변환하여 KEY 생성
    STS봉강_특수합금_입고['KEY'] = (
        STS봉강_특수합금_입고['열처리_구분'].str.strip() + "_" +
        STS봉강_특수합금_입고['형상_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['특수제강_구분'].str.strip()
        )
    
    # 7) 통계치 계산
    # A컬럼을 그룹핑하여 각 그룹별 B컬럼의 고유값(nunique) 갯수를 계산 후, 결과를 새로운 DataFrame 생성
    result_A = STS봉강_특수합금_입고.groupby('KEY')['LOT_NO'].nunique().reset_index(name='KEY별 LOT 갯수')

    # A컬럼을 그룹핑하여 각 그룹별 B컬럼의 합산(sum)을 계산 후, 결과를 새로운 DataFrame 생성
    result_B = STS봉강_특수합금_입고.groupby('KEY')['입고중량'].sum().reset_index(name='KEY 총 중량')

    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 중앙값을 계산하고 결과를 새로운 DataFrame 생성
    result_C = STS봉강_특수합금_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].median().reset_index(name='제조공기_중앙값')

    # A컬럼을 그룹핑하여 각 그룹별 B 컬럼의 1분위수(25th percentile)를 계산하고,
    # 그 결과를 새로운 DataFrame으로 생성합니다.
    result_D = STS봉강_특수합금_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].quantile(0.25).reset_index(name='제조공기_1분위수')

    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 75번째 백분위수(3분위수)를 계산 후,
    # 결과를 새로운 DataFrame으로 생성합니다.
    result_E = STS봉강_특수합금_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].quantile(0.75).reset_index(name='제조공기_3분위수')

    # 그룹별로 (Q1 - 1.5×IQR) ~ (Q3 + 1.5×IQR) 구간에 해당하는 값들의 평균을 계산하는 함수
    def avg_iqr(series):
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3-q1
        return series[(series>=q1-1.5*iqr)&(series<=q3+1.5*iqr)].mean()
    result_F = STS봉강_특수합금_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].apply(avg_iqr).reset_index(name='제조공기_(Q1-1.5IQR~Q3+1.5IQR)_평균')
    
    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 평균값을 계산하고 결과를 새로운 DataFrame 생성
    result_G = STS봉강_특수합금_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].mean().reset_index(name='제조공기_단순평균')
    
    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 중량가중평균을 계산하고 결과를 새로운 DataFrame 생성
    STS봉강_특수합금_입고 = STS봉강_특수합금_입고.merge(result_B, on='KEY')
    STS봉강_특수합금_입고['가중계수'] = STS봉강_특수합금_입고['입고중량']/STS봉강_특수합금_입고['KEY 총 중량']
    STS봉강_특수합금_입고['제조공기*가중계수'] = STS봉강_특수합금_입고['제조공기(입고일-생산의뢰년월일)']*STS봉강_특수합금_입고['가중계수']
    result_H = STS봉강_특수합금_입고.groupby('KEY')['제조공기*가중계수'].sum().reset_index(name='KEY별 중량가중평균')

    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 중량가중평균을 계산하고 결과를 새로운 DataFrame 생성
    STS봉강_특수합금_입고 = STS봉강_특수합금_입고.merge(result_H, on='KEY')
    STS봉강_특수합금_입고['제조공기-중량가중평균'] = STS봉강_특수합금_입고['제조공기(입고일-생산의뢰년월일)'] - STS봉강_특수합금_입고['KEY별 중량가중평균']
    STS봉강_특수합금_입고['제조공기-중량가중평균^2'] = STS봉강_특수합금_입고['제조공기-중량가중평균'] ** 2
    STS봉강_특수합금_입고['제조공기-중량가중평균^2 * 각 LOT중량'] = STS봉강_특수합금_입고['입고중량'] * STS봉강_특수합금_입고['제조공기-중량가중평균^2']

    # 그룹핑한 후, B와 C 컬럼의 합계를 계산    
    result_I = STS봉강_특수합금_입고.groupby('KEY').agg({'제조공기-중량가중평균^2 * 각 LOT중량':'sum','입고중량':'sum'}).reset_index()

    # 각 그룹별로 B 합계값을 C 합계값으로 나눈 값(비율)을 계산하여 새로운 컬럼 생성
    result_I['중량가중분산'] = result_I['제조공기-중량가중평균^2 * 각 LOT중량'] / result_I['입고중량']
    
    # 특정컬럼의 제곱근을 계산하여 새로운 파생변수 "특정컬럼_제곱근" 생성
    result_I['중량가중_표준편차'] = np.sqrt(result_I['중량가중분산'])

    # 8) final summary dataframe 생성
    # 데이터프레임 리스트 생성
    dfs = [result_A, result_B, result_C, result_D, result_E, result_F, result_G, result_H, result_I]
    merged_df = reduce(lambda l,r: pd.merge(l,r,on='KEY'), dfs)
    
    # 9) boxplot 이미지 생성
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(6,2));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('off'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode()
    imgs = [{'KEY':k,
             'BOX PLOT(원본)':f'<img src="data:image/png;base64,{make_img(g)}"/>',
             'BOX PLOT(이상치 숨김)':f'<img src="data:image/png;base64,{make_img(g,False)}"/>'}
            for k,g in STS봉강_특수합금_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)']]
    img_df = pd.DataFrame(imgs)

    # 10) final summary dataframe과 img_df merge
    merged_df = merged_df.merge(img_df, on='KEY')

    # 11) KEY별 LOT 갯수 기준으로 정렬
    merged_df = merged_df.sort_values(by='KEY별 LOT 갯수', ascending=False).reset_index(drop=True)
    return merged_df

# ------------------------------------------------------------------
# 데이터 로드 & 처리 (3. STS선재)
# ------------------------------------------------------------------
@st.cache_data
def load_data_sts_wr():
    '''
    3) STS선재 
    - 강종대분류 : S,V
    - 형상: WR
    '''
    # 0) raw_data parquet file import
    STS선재 = pd.read_parquet('3.STS선재.parquet')
    
    # 1) 그룹별(공정순위가 최대인 행)의 인덱스를 구한 뒤, 해당 행만 추출
    df_max = STS선재.loc[STS선재.groupby('LOT_NO')['공정순위'].idxmax(), ['LOT_NO','공정순위']]
    
    # 2) 원본 데이터프레임과 df_max inner merge
    STS선재_입고 = pd.merge(STS선재, df_max, on=['LOT_NO','공정순위'], how='inner')
    
    # 3) '생산정보_작업일자' 컬럼을 datetime 형식으로 변환 및 '생산의뢰년월' 컬럼과의 차이 계산
    STS선재_입고['생산의뢰년월'] = pd.to_datetime(STS선재_입고['생산의뢰년월'])
    STS선재_입고['제조공기(입고일-생산의뢰년월일)'] = (STS선재_입고['생산정보_작업일자'] - STS선재_입고['생산의뢰년월']).dt.days
    
    # 4) 파생변수 생성
    # 여러 조건을 리스트로 정의합니다.
    conditions = [
        # "산세_200계": 대강종코드가 'S', 사내강종이 "SF"로 시작, 표면이 "P"로 시작, HEAT_NO가 'S','N','E' 중 하나로 시작
        (STS선재_입고['대강종코드'] == 'S') &
        (STS선재_입고['사내강종'].str.startswith("SF")) &
        (STS선재_입고['표면'].str.startswith("P")) &
        (STS선재_입고['HEAT_NO'].str.startswith(('S','N','E'))),
        
        # "산세_300계": 대강종코드가 'S', 사내강종이 "SS"로 시작, 표면이 "P"로 시작, HEAT_NO가 'S','N','E' 중 하나로 시작
        (STS선재_입고['대강종코드'] == 'S') &
        (STS선재_입고['사내강종'].str.startswith("SS")) &
        (STS선재_입고['표면'].str.startswith("P")) &
        (STS선재_입고['HEAT_NO'].str.startswith(('S','N','E'))),
        
        # "산세_400계": 대강종코드가 'S', 사내강종이 "SM"로 시작, 표면이 "P"로 시작, HEAT_NO가 'S','N','E' 중 하나로 시작
        (STS선재_입고['대강종코드'] == 'S') &
        (STS선재_입고['사내강종'].str.startswith("SM")) &
        (STS선재_입고['표면'].str.startswith("P")) &
        (STS선재_입고['HEAT_NO'].str.startswith(('S','N','E'))),
        
        # "산세_내열강": 대강종코드가 'S', 사내강종이 "SU"로 시작, 표면이 "P"로 시작, HEAT_NO가 'S','N','E' 중 하나로 시작
        (STS선재_입고['대강종코드'] == 'S') &
        (STS선재_입고['사내강종'].str.startswith("SU")) &
        (STS선재_입고['표면'].str.startswith("P")) &
        (STS선재_입고['HEAT_NO'].str.startswith(('S','N','E'))),
        
        # "산세_특수합금": 대강종코드가 'V'이고, 표면이 "P"로 시작
        (STS선재_입고['대강종코드'] == 'V') &
        (STS선재_입고['표면'].str.startswith("P")),
        
        # "특수정련_산세_300계": 대강종코드가 'S', 사내강종이 "SS"로 시작, 표면이 "P"로 시작, HEAT_NO가 ('K','H','R','V') 중 하나로 시작
        (STS선재_입고['대강종코드'] == 'S') &
        (STS선재_입고['사내강종'].str.startswith("SS")) &
        (STS선재_입고['표면'].str.startswith("P")) &
        (STS선재_입고['HEAT_NO'].str.startswith(('K','H','R','V'))),
        
        # "특수정련_산세_400계": 대강종코드가 'S', 사내강종이 "SM"로 시작, 표면이 "P"로 시작, HEAT_NO가 ('K','H','R','V') 중 하나로 시작
        (STS선재_입고['대강종코드'] == 'S') &
        (STS선재_입고['사내강종'].str.startswith("SM")) &
        (STS선재_입고['표면'].str.startswith("P")) &
        (STS선재_입고['HEAT_NO'].str.startswith(('K','H','R','V'))),
        
        # "흑피_비열처리": 표면이 "B"로 시작하고, 열처리 컬럼이 결측치인 경우
        (STS선재_입고['표면'].str.startswith("B")) &
        (STS선재_입고['열처리'].isna()),
        
        # "흑피_열처리": 표면이 "B"로 시작하고, 열처리 컬럼에 값이 있는 경우
        (STS선재_입고['표면'].str.startswith("B")) &
        (STS선재_입고['열처리'].notna())
        ]

    # 각 조건에 대응하는 레이블(출력값) 리스트
    choices = [
        "산세_200계",
        "산세_300계",
        "산세_400계",      # 주석과 맞추어 "산세_400계"로 수정(원본 코드에서는 "산세_300계"였음)
        "산세_내열강",
        "산세_특수합금",
        "특수정련_산세_300계",
        "특수정련_산세_400계",
        "흑피_비열처리",
        "흑피_열처리"
        ]

    # np.select를 사용하여 조건에 맞는 레이블 할당 (어떤 조건에도 해당하지 않으면 기본값(default) None을 할당)
    STS선재_입고['KEY'] = np.select(conditions, choices, default=None)

    # 5) 통계치 계산
    # A컬럼을 그룹핑하여 각 그룹별 B컬럼의 고유값(nunique) 갯수를 계산 후, 결과를 새로운 DataFrame 생성
    result_A = STS선재_입고.groupby('KEY')['LOT_NO'].nunique().reset_index(name='KEY별 LOT 갯수')

    # A컬럼을 그룹핑하여 각 그룹별 B컬럼의 합산(sum)을 계산 후, 결과를 새로운 DataFrame 생성
    result_B = STS선재_입고.groupby('KEY')['입고중량'].sum().reset_index(name='KEY 총 중량')

    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 중앙값을 계산하고 결과를 새로운 DataFrame 생성
    result_C = STS선재_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].median().reset_index(name='제조공기_중앙값')

    # A컬럼을 그룹핑하여 각 그룹별 B 컬럼의 1분위수(25th percentile)를 계산하고,
    # 그 결과를 새로운 DataFrame으로 생성합니다.
    result_D = STS선재_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].quantile(0.25).reset_index(name='제조공기_1분위수')

    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 75번째 백분위수(3분위수)를 계산 후,
    # 결과를 새로운 DataFrame으로 생성합니다.
    result_E = STS선재_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].quantile(0.75).reset_index(name='제조공기_3분위수')

    # 그룹별로 (Q1 - 1.5×IQR) ~ (Q3 + 1.5×IQR) 구간에 해당하는 값들의 평균을 계산하는 함수
    def avg_iqr(series):
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3-q1
        return series[(series>=q1-1.5*iqr)&(series<=q3+1.5*iqr)].mean()
    result_F = STS선재_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].apply(avg_iqr).reset_index(name='제조공기_(Q1-1.5IQR~Q3+1.5IQR)_평균')
    
    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 평균값을 계산하고 결과를 새로운 DataFrame 생성
    result_G = STS선재_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].mean().reset_index(name='제조공기_단순평균')
    
    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 중량가중평균을 계산하고 결과를 새로운 DataFrame 생성
    STS선재_입고 = STS선재_입고.merge(result_B, on='KEY')
    STS선재_입고['가중계수'] = STS선재_입고['입고중량']/STS선재_입고['KEY 총 중량']
    STS선재_입고['제조공기*가중계수'] = STS선재_입고['제조공기(입고일-생산의뢰년월일)']*STS선재_입고['가중계수']
    result_H = STS선재_입고.groupby('KEY')['제조공기*가중계수'].sum().reset_index(name='KEY별 중량가중평균')

    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 중량가중평균을 계산하고 결과를 새로운 DataFrame 생성
    STS선재_입고 = STS선재_입고.merge(result_H, on='KEY')
    STS선재_입고['제조공기-중량가중평균'] = STS선재_입고['제조공기(입고일-생산의뢰년월일)'] - STS선재_입고['KEY별 중량가중평균']
    STS선재_입고['제조공기-중량가중평균^2'] = STS선재_입고['제조공기-중량가중평균'] ** 2
    STS선재_입고['제조공기-중량가중평균^2 * 각 LOT중량'] = STS선재_입고['입고중량'] * STS선재_입고['제조공기-중량가중평균^2']

    # 그룹핑한 후, B와 C 컬럼의 합계를 계산    
    result_I = STS선재_입고.groupby('KEY').agg({'제조공기-중량가중평균^2 * 각 LOT중량':'sum','입고중량':'sum'}).reset_index()

    # 각 그룹별로 B 합계값을 C 합계값으로 나눈 값(비율)을 계산하여 새로운 컬럼 생성
    result_I['중량가중분산'] = result_I['제조공기-중량가중평균^2 * 각 LOT중량'] / result_I['입고중량']
    
    # 특정컬럼의 제곱근을 계산하여 새로운 파생변수 "특정컬럼_제곱근" 생성
    result_I['중량가중_표준편차'] = np.sqrt(result_I['중량가중분산'])

    # 8) final summary dataframe 생성
    # 데이터프레임 리스트 생성
    dfs = [result_A, result_B, result_C, result_D, result_E, result_F, result_G, result_H, result_I]
    merged_df = reduce(lambda l,r: pd.merge(l,r,on='KEY'), dfs)
    
    # 9) boxplot 이미지 생성
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(6,2));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('off'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode()
    imgs = [{'KEY':k,
             'BOX PLOT(원본)':f'<img src="data:image/png;base64,{make_img(g)}"/>',
             'BOX PLOT(이상치 숨김)':f'<img src="data:image/png;base64,{make_img(g,False)}"/>'}
            for k,g in STS선재_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)']]
    img_df = pd.DataFrame(imgs)

    # 10) final summary dataframe과 img_df merge
    merged_df = merged_df.merge(img_df, on='KEY')

    # 11) KEY별 LOT 갯수 기준으로 정렬
    merged_df = merged_df.sort_values(by='KEY별 LOT 갯수', ascending=False).reset_index(drop=True)
    return merged_df



# ------------------------------------------------------------------
# 데이터 로드 & 처리 (4. 금형/공구강)
# ------------------------------------------------------------------
@st.cache_data
def load_data_tool():
    '''
    4) 공구강/금형강
    - 강종대분류: T
    - 형상 : ALL
    '''
    # 0) raw_data parquet file import
    공구_금형강 = pd.read_parquet('4.공구_금형강.parquet')
    
    # 1) 그룹별(공정순위가 최대인 행)의 인덱스를 구한 뒤, 해당 행만 추출
    df_max = 공구_금형강.loc[공구_금형강.groupby('LOT_NO')['공정순위'].idxmax(), ['LOT_NO','공정순위']]
    
    # 2) 원본 데이터프레임과 df_max inner merge
    공구_금형강_입고 = pd.merge(공구_금형강, df_max, on=['LOT_NO','공정순위'], how='inner')
    
    # 3) '생산정보_작업일자' 컬럼을 datetime 형식으로 변환 및 '생산의뢰년월' 컬럼과의 차이 계산
    공구_금형강_입고['생산의뢰년월'] = pd.to_datetime(공구_금형강_입고['생산의뢰년월'])
    공구_금형강_입고['제조공기(입고일-생산의뢰년월일)'] = (공구_금형강_입고['생산정보_작업일자'] - 공구_금형강_입고['생산의뢰년월']).dt.days
    
    # 4) 파생변수 생성 (입고시기)
    # 조건설정
    def determine_quarter(date_str):
        """
        date_str: "YYYY-MM" 형식의 문자열 예: "2024-10"
        반환: "YY년 X분기" (예: "24년 4분기") 또는 파싱 실패 시 None
        """
        try:
            # date_str을 datetime 객체로 변환 (일자는 무시)
            dt = datetime.datetime.strptime(date_str, "%Y-%m")
        except ValueError:
            return None
        
        # (month - 1) // 3 + 1 를 이용해 분기를 계산 (1~3월: 1분기, ...)
        quarter = (dt.month - 1) // 3 + 1
        # 연도는 두 자리로 변환하여 레이블 생성
        return f"{dt.year % 100:02d}년 {quarter}분기"

    # 파생컬럼 값에 대해 함수 적용하여 Y 컬럼 생성
    공구_금형강_입고["입고시기"] = 공구_금형강_입고["입고년월"].apply(determine_quarter)

    # 5) 파생변수 생성 (강종구분 KEY)
    # 조건설정
    def categorize(사내강종명, 주문형상):
        # 조건 1: 사내강종명 값이 "STD"로 시작하면
        if isinstance(사내강종명, str) and 사내강종명.startswith("STD"):
            return "STD11_61종"
        # 조건 2: 사내강종명 값이 "SMATE", "SMATF", "SMATV" 중 하나면
        elif 사내강종명 in ("SMATE", "SMATF", "SMATV"):
            return 사내강종명
        # 조건 3: 사내강종명 값이 "TP"로 시작하면
        elif isinstance(사내강종명, str) and 사내강종명.startswith("TP"):
            return "TP계열"
        # 조건 4: 사내강종명 값이 "THKT"로 시작하면
        elif isinstance(사내강종명, str) and 사내강종명.startswith("THKT"):
            return "SKS4계열"
        # 조건 5: 사내강종명 값이 "D2" 또는 "S7" 이거나, 사내강종명 값이 "W2"로 시작하면
        elif (사내강종명 in ("D2", "S7")) or (isinstance(사내강종명, str) and 사내강종명.startswith("W2")):
            return "수출대상"
        # 조건 6: 주문형상 값이 "FK"이면
        elif 주문형상 == "FK":
            return "FK"
        # 나머지 경우
        else:
            return "기타_재분류"

    # apply를 사용하여 새로운 열 'Y' 생성
    공구_금형강_입고["강종구분 KEY"] = 공구_금형강_입고.apply(lambda row: categorize(row["사내강종명"], row["주문형상"]), axis=1)

    # 6) 파생변수 생성 (종합 최종 KEY)
    # 조건설정
    # '품종', '주문형상', '표면' 컬럼의 공백을 제거하고, 각 컬럼을 문자열로 변환하여 KEY 생성
    공구_금형강_입고["KEY"] = (
        공구_금형강_입고["입고시기"].str.strip().astype(str) + "_" +
        공구_금형강_입고["강종구분 KEY"].str.strip().astype(str) + "_" +
        공구_금형강_입고["품종"].str.strip().astype(str) + "_" +
        공구_금형강_입고["주문형상"].str.strip().astype(str) + "_" +
        공구_금형강_입고["열처리"].str.strip().astype(str) + "_" +
        공구_금형강_입고["표면"].str.strip().astype(str)
    )

    # 7) 통계치 계산
    # A컬럼을 그룹핑하여 각 그룹별 B컬럼의 고유값(nunique) 갯수를 계산 후, 결과를 새로운 DataFrame 생성
    result_A = 공구_금형강_입고.groupby('KEY')['LOT_NO'].nunique().reset_index(name='KEY별 LOT 갯수')

    # A컬럼을 그룹핑하여 각 그룹별 B컬럼의 합산(sum)을 계산 후, 결과를 새로운 DataFrame 생성
    result_B = 공구_금형강_입고.groupby('KEY')['입고중량'].sum().reset_index(name='KEY 총 중량')

    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 중앙값을 계산하고 결과를 새로운 DataFrame 생성
    result_C = 공구_금형강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].median().reset_index(name='제조공기_중앙값')

    # A컬럼을 그룹핑하여 각 그룹별 B 컬럼의 1분위수(25th percentile)를 계산하고,
    # 그 결과를 새로운 DataFrame으로 생성합니다.
    result_D = 공구_금형강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].quantile(0.25).reset_index(name='제조공기_1분위수')

    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 75번째 백분위수(3분위수)를 계산 후,
    # 결과를 새로운 DataFrame으로 생성합니다.
    result_E = 공구_금형강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].quantile(0.75).reset_index(name='제조공기_3분위수')

    # 그룹별로 (Q1 - 1.5×IQR) ~ (Q3 + 1.5×IQR) 구간에 해당하는 값들의 평균을 계산하는 함수
    def avg_iqr(series):
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3-q1
        return series[(series>=q1-1.5*iqr)&(series<=q3+1.5*iqr)].mean()
    result_F = 공구_금형강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].apply(avg_iqr).reset_index(name='제조공기_(Q1-1.5IQR~Q3+1.5IQR)_평균')
    
    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 평균값을 계산하고 결과를 새로운 DataFrame 생성
    result_G = 공구_금형강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)'].mean().reset_index(name='제조공기_단순평균')
    
    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 중량가중평균을 계산하고 결과를 새로운 DataFrame 생성
    공구_금형강_입고 = 공구_금형강_입고.merge(result_B, on='KEY')
    공구_금형강_입고['가중계수'] = 공구_금형강_입고['입고중량']/공구_금형강_입고['KEY 총 중량']
    공구_금형강_입고['제조공기*가중계수'] = 공구_금형강_입고['제조공기(입고일-생산의뢰년월일)']*공구_금형강_입고['가중계수']
    result_H = 공구_금형강_입고.groupby('KEY')['제조공기*가중계수'].sum().reset_index(name='KEY별 중량가중평균')

    # A 컬럼을 그룹핑하여 각 그룹별 B 컬럼의 중량가중평균을 계산하고 결과를 새로운 DataFrame 생성
    공구_금형강_입고 = 공구_금형강_입고.merge(result_H, on='KEY')
    공구_금형강_입고['제조공기-중량가중평균'] = 공구_금형강_입고['제조공기(입고일-생산의뢰년월일)'] - 공구_금형강_입고['KEY별 중량가중평균']
    공구_금형강_입고['제조공기-중량가중평균^2'] = 공구_금형강_입고['제조공기-중량가중평균'] ** 2
    공구_금형강_입고['제조공기-중량가중평균^2 * 각 LOT중량'] = 공구_금형강_입고['입고중량'] * 공구_금형강_입고['제조공기-중량가중평균^2']

    # 그룹핑한 후, B와 C 컬럼의 합계를 계산    
    result_I = 공구_금형강_입고.groupby('KEY').agg({'제조공기-중량가중평균^2 * 각 LOT중량':'sum','입고중량':'sum'}).reset_index()

    # 각 그룹별로 B 합계값을 C 합계값으로 나눈 값(비율)을 계산하여 새로운 컬럼 생성
    result_I['중량가중분산'] = result_I['제조공기-중량가중평균^2 * 각 LOT중량'] / result_I['입고중량']
    
    # 특정컬럼의 제곱근을 계산하여 새로운 파생변수 "특정컬럼_제곱근" 생성
    result_I['중량가중_표준편차'] = np.sqrt(result_I['중량가중분산'])

    # 8) final summary dataframe 생성
    # 데이터프레임 리스트 생성
    dfs = [result_A, result_B, result_C, result_D, result_E, result_F, result_G, result_H, result_I]
    merged_df = reduce(lambda l,r: pd.merge(l,r,on='KEY'), dfs)
    
    # 9) boxplot 이미지 생성
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(6,2));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('off'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode()
    imgs = [{'KEY':k,
             'BOX PLOT(원본)':f'<img src="data:image/png;base64,{make_img(g)}"/>',
             'BOX PLOT(이상치 숨김)':f'<img src="data:image/png;base64,{make_img(g,False)}"/>'}
            for k,g in 공구_금형강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)']]
    img_df = pd.DataFrame(imgs)

    # 10) final summary dataframe과 img_df merge
    merged_df = merged_df.merge(img_df, on='KEY')

    # 11) KEY별 LOT 갯수 기준으로 정렬
    merged_df = merged_df.sort_values(by='KEY별 LOT 갯수', ascending=False).reset_index(drop=True)
    return merged_df

# ------------------------------------------------------------------
# Streamlit UI
# ------------------------------------------------------------------
st.set_page_config(page_title="입고 분석 앱", layout="wide")
st.title("분석 결과 테이블")

# 데이터 선택
dataset = st.sidebar.radio("분석할 데이터 선택", ("1.탄합선재_탄합봉강", "2.STS봉강_특수합금", "3.STS선재", "4.공금_금형강"))

with st.spinner("데이터 로드 중..."):
    if dataset == "1.탄합선재_탄합봉강":
        df = load_data_tan()
    elif dataset == "2.STS봉강_특수합금":
        df = load_data_sts()
    elif dataset == "3.STS선재":
        df = load_data_sts_wr()
    elif dataset == "4.공금_금형강":
        df = load_data_tool()
    else:
        st.error("데이터셋을 선택해주세요.")

# KEY 필터링
all_keys = df['KEY'].unique().tolist()
selected = st.sidebar.multiselect("🔑 필터할 KEY 선택", all_keys, default=all_keys[:5])
if selected:
    df = df[df['KEY'].isin(selected)]
else:
    st.sidebar.warning("하나 이상의 KEY를 선택해주세요.")

# 테이블 출력
# st.markdown("### 분석 결과 테이블")
st.write(df.to_html(escape=False, index=False), unsafe_allow_html=True)

# 엑셀 다운로드
excel_data = to_excel_with_images(df)
st.download_button("📥 엑셀 다운로드", data=excel_data, file_name=f"{dataset}_분석결과.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
