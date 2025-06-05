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
import re

# ------------------------------------------------------------------
# 엑셀 생성: 이미지 포함해서 Bytes 반환
# ------------------------------------------------------------------
@st.cache_data
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
    1) 탄합선재 + 탄합봉강 + MARAGING강
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
    merged_df.drop(columns=['입고중량'], inplace=True)
    merged_df.rename(columns={'KEY 총 중량':'KEY 총 중량(입고중량)'}, inplace=True)
    
    # 9) boxplot 이미지 생성
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(6,2));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('on'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
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
    
    # 0-1) '주문구분'이 '강관원재'가 아닌 행만 추출
    STS봉강_특수합금 = STS봉강_특수합금[STS봉강_특수합금['주문구분'] != '강관원재']
    
    # 0-2) '열처리' 컬럼의 결측값을 'NH'로 채움
    STS봉강_특수합금['열처리'] = STS봉강_특수합금['열처리'].fillna('NH')
    
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
    
    # 6) 파생변수 생성 (제품구분)
    # 조건 설정
    conditions = [
        (STS봉강_특수합금_입고['대강종코드']=='S'),
        (STS봉강_특수합금_입고['대강종코드']=='V')
    ]
    
    # 각 조건에 해당하는 값 목록
    choices = ['STS봉강','특수합금']
    
    # 조건에 맞지 않는 경우 default로 None 할당
    STS봉강_특수합금_입고['제품구분'] = np.select(conditions, choices, default='None')
    
    # 7) 파생변수 생성 (기간구분)
    # 조건 설정
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
    STS봉강_특수합금_입고["기간구분"] = STS봉강_특수합금_입고["입고년월"].apply(determine_quarter)
    
    # 8) 파생변수 생성 (압연구분)
    # 컬럼명 지정
    ps_col = '제품소성공정명'
    p_col  = '품종'
    o_col  = '주문형상'

    # 각 조건 정의
    cond1 = (
        STS봉강_특수합금_입고[ps_col].isin(['1단조 1800톤 프레스', 'RFM단조']) &
        (STS봉강_특수합금_입고[p_col] == '일반단조')
    )
    cond2 = (
        STS봉강_특수합금_입고[ps_col].isin(['1단조 1800톤 프레스', 'RFM단조']) &
        (STS봉강_특수합금_입고[p_col] == '대형재') &
        (STS봉강_특수합금_입고[o_col] == 'RB')
    )
    cond_1danjo   = cond1 | cond2

    cond_2danjo   = (
        (STS봉강_특수합금_입고[ps_col] == '2단조_9000톤_프레스') &
        (STS봉강_특수합금_입고[p_col] == '일반단조')
    )

    cond_daehyeong = STS봉강_특수합금_입고[ps_col].isin(['분괴압연', '분괴_SBM압연'])
    cond_sohyeong  = STS봉강_특수합금_입고[ps_col] == '소형압연'

    # np.select 로 파생컬럼 '압연구분' 추가
    STS봉강_특수합금_입고['압연구분'] = np.select(
        [cond_1danjo, cond_2danjo, cond_daehyeong, cond_sohyeong],
        ['1단조',      '2단조',      '대형압연',      '소형압연'],
        default='예외'
    )
    
    # 9) 파생변수 생성 : 각 컬럼 데이터의 공백을 제거하고, 문자열로 변환하여 KEY 생성
    STS봉강_특수합금_입고['KEY'] = (
        STS봉강_특수합금_입고['열처리_구분'].str.strip() + "_" +
        STS봉강_특수합금_입고['형상_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['특수제강_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['제품구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['기간구분'].str.strip() + "_" +
        STS봉강_특수합금_입고['열처리'].str.strip() + "_" +
        STS봉강_특수합금_입고['표면'].str.strip() + "_" +
        STS봉강_특수합금_입고['압연구분'].str.strip() 
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
    merged_df.drop(columns=['입고중량'], inplace=True)
    merged_df.rename(columns={'KEY 총 중량':'KEY 총 중량(입고중량)'}, inplace=True)
    
    # 9) boxplot 이미지 생성
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(6,2));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('on'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
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
# 데이터 로드 & 처리 (2-1. STS봉강_특수합금-A)
# ------------------------------------------------------------------
@st.cache_data
def load_data_sts_a():
    '''
    2) STS봉강 + 특수합금
    - 강종대분류: S, V
    - 형상: RB, FB, SB, HB
    '''
    # 0) raw_data parquet file import
    STS봉강_특수합금 = pd.read_parquet('2.STS봉강_특수합금.parquet')
    master_table = pd.read_parquet('master_table_sts_bar.parquet')
    
    # 0-1) '주문구분'이 '강관원재'가 아닌 행만 추출
    STS봉강_특수합금 = STS봉강_특수합금[STS봉강_특수합금['주문구분'] != '강관원재']
    
    # 0-2) '열처리' 컬럼의 결측값을 'NH'로 채움
    STS봉강_특수합금['열처리'] = STS봉강_특수합금['열처리'].fillna('NH')
    
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
    
    # 6) 파생변수 생성 (제품구분)
    # 조건 설정
    conditions = [
        (STS봉강_특수합금_입고['대강종코드']=='S'),
        (STS봉강_특수합금_입고['대강종코드']=='V')
    ]
    
    # 각 조건에 해당하는 값 목록
    choices = ['STS봉강','특수합금']
    
    # 조건에 맞지 않는 경우 default로 None 할당
    STS봉강_특수합금_입고['제품구분'] = np.select(conditions, choices, default='None')
    
    # 7) 파생변수 생성 (기간구분)
    # 조건 설정
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
    STS봉강_특수합금_입고["기간구분"] = STS봉강_특수합금_입고["입고년월"].apply(determine_quarter)
    
    # 8) 파생변수 생성 (압연구분)
    # 컬럼명 지정
    ps_col = '제품소성공정명'
    p_col  = '품종'
    o_col  = '주문형상'

    # 각 조건 정의
    cond1 = (
        STS봉강_특수합금_입고[ps_col].isin(['1단조 1800톤 프레스', 'RFM단조']) &
        (STS봉강_특수합금_입고[p_col] == '일반단조')
    )
    cond2 = (
        STS봉강_특수합금_입고[ps_col].isin(['1단조 1800톤 프레스', 'RFM단조']) &
        (STS봉강_특수합금_입고[p_col] == '대형재') &
        (STS봉강_특수합금_입고[o_col] == 'RB')
    )
    cond_1danjo   = cond1 | cond2

    cond_2danjo   = (
        (STS봉강_특수합금_입고[ps_col] == '2단조_9000톤_프레스') &
        (STS봉강_특수합금_입고[p_col] == '일반단조')
    )

    cond_daehyeong = STS봉강_특수합금_입고[ps_col].isin(['분괴압연', '분괴_SBM압연'])
    cond_sohyeong  = STS봉강_특수합금_입고[ps_col] == '소형압연'

    # np.select 로 파생컬럼 '압연구분' 추가
    STS봉강_특수합금_입고['압연구분'] = np.select(
        [cond_1danjo, cond_2danjo, cond_daehyeong, cond_sohyeong],
        ['1단조',      '2단조',      '대형압연',      '소형압연'],
        default='예외'
    )
    
    # 9) 파생변수 생성 (주요대상)
    # 관리 대상이 되는 등급 리스트를 집합(set)으로 추출
    관리대상_리스트 = set(master_table['steel_grade'])
    
    # 여기서는 pandas 제공 함수인 isin()을 사용한 예시를 보여드립니다.
    STS봉강_특수합금_입고['주요대상'] = STS봉강_특수합금_입고['사내강종명'].isin(관리대상_리스트) \
        .map({True: '관리대상', False: '예외'})
    
    # 관리대상만 필터링
    STS봉강_특수합금_입고 = STS봉강_특수합금_입고[STS봉강_특수합금_입고['주요대상'] == '관리대상'] 
    
    # 10) 파생변수 생성 : 각 컬럼 데이터의 공백을 제거하고, 문자열로 변환하여 KEY 생성
    STS봉강_특수합금_입고['KEY'] = (
        STS봉강_특수합금_입고['열처리_구분'].str.strip() + "_" +
        STS봉강_특수합금_입고['형상_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['특수제강_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['제품구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['기간구분'].str.strip() + "_" +
        STS봉강_특수합금_입고['열처리'].str.strip() + "_" +
        STS봉강_특수합금_입고['표면'].str.strip() + "_" +
        STS봉강_특수합금_입고['압연구분'].str.strip()
        )
    
    # 11) 통계치 계산
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
    merged_df.drop(columns=['입고중량'], inplace=True)
    merged_df.rename(columns={'KEY 총 중량':'KEY 총 중량(입고중량)'}, inplace=True)
    
    # 9) boxplot 이미지 생성
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(6,2));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('on'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
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
# 데이터 로드 & 처리 (2-2. STS봉강_특수합금-B)
# ------------------------------------------------------------------
@st.cache_data
def load_data_sts_b():
    '''
    2) STS봉강 + 특수합금
    - 강종대분류: S, V
    - 형상: RB, FB, SB, HB
    '''
    # 0) raw_data parquet file import
    STS봉강_특수합금 = pd.read_parquet('2.STS봉강_특수합금.parquet')
    master_table = pd.read_parquet('master_table_sts_bar.parquet')
    
    # 0-1) '주문구분'이 '강관원재'가 아닌 행만 추출
    STS봉강_특수합금 = STS봉강_특수합금[STS봉강_특수합금['주문구분'] != '강관원재']
    
    # 0-2) '열처리' 컬럼의 결측값을 'NH'로 채움
    STS봉강_특수합금['열처리'] = STS봉강_특수합금['열처리'].fillna('NH')
    
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
    
    # 6) 파생변수 생성 (제품구분)
    # 조건 설정
    conditions = [
        (STS봉강_특수합금_입고['대강종코드']=='S'),
        (STS봉강_특수합금_입고['대강종코드']=='V')
    ]
    
    # 각 조건에 해당하는 값 목록
    choices = ['STS봉강','특수합금']
    
    # 조건에 맞지 않는 경우 default로 None 할당
    STS봉강_특수합금_입고['제품구분'] = np.select(conditions, choices, default='None')
    
    # 7) 파생변수 생성 (기간구분)
    # 조건 설정
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
    STS봉강_특수합금_입고["기간구분"] = STS봉강_특수합금_입고["입고년월"].apply(determine_quarter)
    
    # 8) 파생변수 생성 (압연구분)
    # 컬럼명 지정
    ps_col = '제품소성공정명'
    p_col  = '품종'
    o_col  = '주문형상'

    # 각 조건 정의
    cond1 = (
        STS봉강_특수합금_입고[ps_col].isin(['1단조 1800톤 프레스', 'RFM단조']) &
        (STS봉강_특수합금_입고[p_col] == '일반단조')
    )
    cond2 = (
        STS봉강_특수합금_입고[ps_col].isin(['1단조 1800톤 프레스', 'RFM단조']) &
        (STS봉강_특수합금_입고[p_col] == '대형재') &
        (STS봉강_특수합금_입고[o_col] == 'RB')
    )
    cond_1danjo   = cond1 | cond2

    cond_2danjo   = (
        (STS봉강_특수합금_입고[ps_col] == '2단조_9000톤_프레스') &
        (STS봉강_특수합금_입고[p_col] == '일반단조')
    )

    cond_daehyeong = STS봉강_특수합금_입고[ps_col].isin(['분괴압연', '분괴_SBM압연'])
    cond_sohyeong  = STS봉강_특수합금_입고[ps_col] == '소형압연'

    # np.select 로 파생컬럼 '압연구분' 추가
    STS봉강_특수합금_입고['압연구분'] = np.select(
        [cond_1danjo, cond_2danjo, cond_daehyeong, cond_sohyeong],
        ['1단조',      '2단조',      '대형압연',      '소형압연'],
        default='예외'
    )
    
    # 9) 파생변수 생성 (주요대상)
    # 관리 대상이 되는 등급 리스트를 집합(set)으로 추출
    관리대상_리스트 = set(master_table['steel_grade'])
    
    # 여기서는 pandas 제공 함수인 isin()을 사용한 예시를 보여드립니다.
    STS봉강_특수합금_입고['주요대상'] = STS봉강_특수합금_입고['사내강종명'].isin(관리대상_리스트) \
        .map({True: '관리대상', False: '예외'})
    
    # 관리대상만 필터링
    STS봉강_특수합금_입고 = STS봉강_특수합금_입고[STS봉강_특수합금_입고['주요대상'] == '관리대상']
    
    # 10) 파생변수 생성 : 각 컬럼 데이터의 공백을 제거하고, 문자열로 변환하여 KEY 생성
    STS봉강_특수합금_입고['KEY'] = (
        STS봉강_특수합금_입고['형상_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['특수제강_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['제품구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['열처리'].str.strip() + "_" +
        STS봉강_특수합금_입고['표면'].str.strip() + "_" +
        STS봉강_특수합금_입고['압연구분'].str.strip() + "_" +
        STS봉강_특수합금_입고['사내강종명']
        )
    
    # 11) 통계치 계산
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
    merged_df.drop(columns=['입고중량'], inplace=True)
    merged_df.rename(columns={'KEY 총 중량':'KEY 총 중량(입고중량)'}, inplace=True)
    
    # 9) boxplot 이미지 생성
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(6,2));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('on'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
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
    merged_df.drop(columns=['입고중량'], inplace=True)
    merged_df.rename(columns={'KEY 총 중량':'KEY 총 중량(입고중량)'}, inplace=True)
    
    # 9) boxplot 이미지 생성
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(6,2));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('on'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
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
    merged_df.drop(columns=['입고중량'], inplace=True)
    merged_df.rename(columns={'KEY 총 중량':'KEY 총 중량(입고중량)'}, inplace=True)
    
    # 9) boxplot 이미지 생성
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(6,2));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('on'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
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
# 데이터 로드 & 처리 (5. 금형/공구강 : STD11_61종_대형재_BS품)
# ------------------------------------------------------------------
@st.cache_data
def load_data_std11_61():
    '''
    4) 공구강/금형강
    - STD11_61종_대형재_BS품
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

    # 4) 필터링 (STD11_61종_대형재_BS품)
    # 미리 정규식 패턴 컴파일 (반복 호출시 속도 향상)
    pattern = re.compile(r'STD11|STD61')

    # Boolean 마스크 생성
    mask = (
        공구_금형강_입고['사내강종명'].str.contains(pattern, na=False)  # STD11 또는 STD61 포함
        & 공구_금형강_입고['표면'].eq('BS')                            # 표면 == 'BS'
        & 공구_금형강_입고['품종'].eq('대형재')                         # 품종 == '대형재'
    )

    # 한 번에 필터링
    공구_금형강_입고 = 공구_금형강_입고.loc[mask]
    
    # 5) 파생변수 생성 (입고시기)
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

    # 6) 파생변수 생성 (강종구분 KEY)
    # 조건설정 : 사내강종명이 STD11로 시작하면 'STD11종', 아니면 'STD61종'
    mask = 공구_금형강_입고['사내강종명'].str.startswith('STD11', na=False)
    공구_금형강_입고['강종구분 KEY'] = np.where(mask, 'STD11종', 'STD61종')

    # 7) 파생변수 생성 (Roll SIZE KEY)
    # ─────────────────────────────────────────────────────────────────────────────
    # STD61, STD11 용 폭→값 매핑 테이블 정의
    # ─────────────────────────────────────────────────────────────────────────────
    ranges_std61 = [
        (72.6,  77.6,  80),  (77.6,  82.1,  85),  (82.1,  87.1,  90),
        (87.1,  93.0,  95),  (93.0,  97.0, 100),  (97.0, 102.0,105),
        (102.0,107.0,110),  (107.0,112.0,115),  (112.0,117.0,120),
        (117.0,122.0,125),  (122.0,127.0,130),  (127.0,132.0,135),
        (132.0,137.0,140),  (137.0,142.0,145),  (142.0,147.0,150),
        (147.0,152.0,155),  (152.0,157.0,160),  (157.0,162.0,165),
        (162.0,167.0,170),  (167.0,172.0,175),  (172.0,177.0,180),
        (177.0,182.0,185),  (182.0,187.0,190),  (187.0,191.0,195),
        (191.0,196.0,200),  (196.0,207.0,210),
    ]

    ranges_std11 = [
        (72.6,  77.6,  80),  (77.6,  82.1,  85),  (82.1,  87.1,  90),
        (87.1,  93.0,  95),  (93.0,  97.0, 100),  (97.0, 102.0,105),
        (102.0,107.0,110),  (107.0,112.0,115),  (112.0,117.0,120),
        (117.0,122.0,125),  (122.0,127.0,130),  (127.0,132.0,135),
        (132.0,137.0,140),  (137.0,142.0,145),  (142.0,147.0,150),
        (147.0,152.0,155),  (152.0,157.0,160),  (157.0,162.0,165),
        (162.0,167.0,170),  (167.0,172.0,175),  (172.0,177.0,180),
        (177.0,181.0,185),  (181.0,186.0,190),  (186.0,191.0,195),
        (191.0,196.0,200),  (196.0,207.0,210),
    ]

    # ─────────────────────────────────────────────────────────────────────────────
    # 행 단위로 레이블링해 줄 함수 정의
    # ─────────────────────────────────────────────────────────────────────────────
    def label_row(row):
        w = row['설계폭']
        kind = row['품종']
        shape = row['주문형상']
        key = row['강종구분 KEY']
        
        # '대형재'만 처리
        if kind != '대형재':
            return None
        
        # FB 형상 → W 레이블
        if shape == 'FB':
            if w < 205:
                return '205W'
            elif w < 280:
                return '280W'
            elif w < 380:
                return '380W'
            elif w < 517:
                return '517W'
            else:
                return '630W'
        
        # RB 형상 → STD61/11 매핑
        if shape == 'RB':
            table = ranges_std61 if key == 'STD61종' else ranges_std11 if key == 'STD11종' else []
            for lo, hi, label in table:
                if lo <= w < hi:
                    return label
        
        # 그 외
        return None

    # ─────────────────────────────────────────────────────────────────────────────
    # DataFrame 에 적용
    # ─────────────────────────────────────────────────────────────────────────────
    공구_금형강_입고['Roll SIZE'] = 공구_금형강_입고.apply(label_row, axis=1)
    
    # 8) 파생변수 생성 (종합 최종 KEY)
    # 조건설정
    # '품종', '주문형상', '표면' 컬럼의 공백을 제거하고, 각 컬럼을 문자열로 변환하여 KEY 생성
    공구_금형강_입고["KEY"] = (
    공구_금형강_입고["입고시기"].str.strip().astype(str) + "_" +
    공구_금형강_입고["품종"].str.strip().astype(str) + "_" +
    공구_금형강_입고["강종구분 KEY"].str.strip().astype(str) + "_" +
    공구_금형강_입고["표면"].str.strip().astype(str) + "_" +
    공구_금형강_입고["재료코드"].str.strip().astype(str) + "_" +
    공구_금형강_입고["Roll SIZE"].str.strip().astype(str)
    )

    # 9) 통계치 계산
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

    # 10) final summary dataframe 생성
    # 데이터프레임 리스트 생성
    dfs = [result_A, result_B, result_C, result_D, result_E, result_F, result_G, result_H, result_I]
    merged_df = reduce(lambda l,r: pd.merge(l,r,on='KEY'), dfs)
    merged_df.drop(columns=['입고중량'], inplace=True)
    merged_df.rename(columns={'KEY 총 중량':'KEY 총 중량(입고중량)'}, inplace=True)
    
    # 11) boxplot 이미지 생성
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(6,2));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('on'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode()
    imgs = [{'KEY':k,
             'BOX PLOT(원본)':f'<img src="data:image/png;base64,{make_img(g)}"/>',
             'BOX PLOT(이상치 숨김)':f'<img src="data:image/png;base64,{make_img(g,False)}"/>'}
            for k,g in 공구_금형강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)']]
    img_df = pd.DataFrame(imgs)

    # 12) final summary dataframe과 img_df merge
    merged_df = merged_df.merge(img_df, on='KEY')

    # 13) KEY별 LOT 갯수 기준으로 정렬
    merged_df = merged_df.sort_values(by='KEY별 LOT 갯수', ascending=False).reset_index(drop=True)
    return merged_df

# ------------------------------------------------------------------
# 데이터 로드 & 처리 (6. 금형/공구강 : STD11_61종_대형재_BS품, NEW KEY적용)
# ------------------------------------------------------------------
@st.cache_data
def load_data_std11_61_new_key():
    '''
    4) 공구강/금형강
    - STD11_61종_대형재_BS품
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

    # 4) 필터링 (STD11_61종_대형재_BS품)
    # 미리 정규식 패턴 컴파일 (반복 호출시 속도 향상)
    pattern = re.compile(r'STD11|STD61')

    # Boolean 마스크 생성
    mask = (
        공구_금형강_입고['사내강종명'].str.contains(pattern, na=False)  # STD11 또는 STD61 포함
        & 공구_금형강_입고['표면'].eq('BS')                            # 표면 == 'BS'
        & 공구_금형강_입고['품종'].eq('대형재')                         # 품종 == '대형재'
    )

    # 한 번에 필터링
    공구_금형강_입고 = 공구_금형강_입고.loc[mask]
    
    # 5) 파생변수 생성 (입고시기)
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

    # 6) 파생변수 생성 (강종구분 KEY)
    # 조건설정 : 사내강종명이 STD11로 시작하면 'STD11종', 아니면 'STD61종'
    mask = 공구_금형강_입고['사내강종명'].str.startswith('STD11', na=False)
    공구_금형강_입고['강종구분 KEY'] = np.where(mask, 'STD11종', 'STD61종')

    # 7) 파생변수 생성 (Roll SIZE KEY)
    # ─────────────────────────────────────────────────────────────────────────────
    # STD61, STD11 용 폭→값 매핑 테이블 정의
    # ─────────────────────────────────────────────────────────────────────────────
    ranges_std61 = [
        (72.6,  77.6,  80),  (77.6,  82.1,  85),  (82.1,  87.1,  90),
        (87.1,  93.0,  95),  (93.0,  97.0, 100),  (97.0, 102.0,105),
        (102.0,107.0,110),  (107.0,112.0,115),  (112.0,117.0,120),
        (117.0,122.0,125),  (122.0,127.0,130),  (127.0,132.0,135),
        (132.0,137.0,140),  (137.0,142.0,145),  (142.0,147.0,150),
        (147.0,152.0,155),  (152.0,157.0,160),  (157.0,162.0,165),
        (162.0,167.0,170),  (167.0,172.0,175),  (172.0,177.0,180),
        (177.0,182.0,185),  (182.0,187.0,190),  (187.0,191.0,195),
        (191.0,196.0,200),  (196.0,207.0,210),
    ]

    ranges_std11 = [
        (72.6,  77.6,  80),  (77.6,  82.1,  85),  (82.1,  87.1,  90),
        (87.1,  93.0,  95),  (93.0,  97.0, 100),  (97.0, 102.0,105),
        (102.0,107.0,110),  (107.0,112.0,115),  (112.0,117.0,120),
        (117.0,122.0,125),  (122.0,127.0,130),  (127.0,132.0,135),
        (132.0,137.0,140),  (137.0,142.0,145),  (142.0,147.0,150),
        (147.0,152.0,155),  (152.0,157.0,160),  (157.0,162.0,165),
        (162.0,167.0,170),  (167.0,172.0,175),  (172.0,177.0,180),
        (177.0,181.0,185),  (181.0,186.0,190),  (186.0,191.0,195),
        (191.0,196.0,200),  (196.0,207.0,210),
    ]

    # ─────────────────────────────────────────────────────────────────────────────
    # 행 단위로 레이블링해 줄 함수 정의
    # ─────────────────────────────────────────────────────────────────────────────
    def label_row(row):
        w = row['설계폭']
        kind = row['품종']
        shape = row['주문형상']
        key = row['강종구분 KEY']
        
        # '대형재'만 처리
        if kind != '대형재':
            return None
        
        # FB 형상 → W 레이블
        if shape == 'FB':
            if w < 205:
                return '205W'
            elif w < 280:
                return '280W'
            elif w < 380:
                return '380W'
            elif w < 517:
                return '517W'
            else:
                return '630W'
        
        # RB 형상 → STD61/11 매핑
        if shape == 'RB':
            table = ranges_std61 if key == 'STD61종' else ranges_std11 if key == 'STD11종' else []
            for lo, hi, label in table:
                if lo <= w < hi:
                    return label
        
        # 그 외
        return None

    # ─────────────────────────────────────────────────────────────────────────────
    # DataFrame 에 적용
    # ─────────────────────────────────────────────────────────────────────────────
    공구_금형강_입고['Roll SIZE'] = 공구_금형강_입고.apply(label_row, axis=1)
    
    # 8) 파생변수 생성 (BS 구분기준)
    # 조건 정의
    cond = (
        공구_금형강_입고['Roll SIZE'].isin(['205W', '280W'])
        | (
            (공구_금형강_입고['Roll SIZE'] == '380W')
            & (공구_금형강_입고['재료코드'] == 'D015')
        )
    )

    # 파생변수 생성
    공구_금형강_입고['BS_구분기준'] = np.where(
        cond,
        '25T*310W 미만',
        '25T*310W 초과'
    )
    
    # 9) 파생변수 생성 (종합 최종 KEY)
    # 조건설정
    # '품종', '주문형상', '표면' 컬럼의 공백을 제거하고, 각 컬럼을 문자열로 변환하여 KEY 생성
    공구_금형강_입고["KEY"] = (
    공구_금형강_입고["입고시기"].str.strip().astype(str) + "_" +
    공구_금형강_입고["BS_구분기준"].str.strip().astype(str)
    )

    # 10) 통계치 계산
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

    # 11) final summary dataframe 생성
    # 데이터프레임 리스트 생성
    dfs = [result_A, result_B, result_C, result_D, result_E, result_F, result_G, result_H, result_I]
    merged_df = reduce(lambda l,r: pd.merge(l,r,on='KEY'), dfs)
    merged_df.drop(columns=['입고중량'], inplace=True)
    merged_df.rename(columns={'KEY 총 중량':'KEY 총 중량(입고중량)'}, inplace=True)
    
    # 12) boxplot 이미지 생성
    def make_img(series, show_outliers=True):
        buf=io.BytesIO(); fig,ax=plt.subplots(figsize=(6,2));
        ax.boxplot(series, vert=False, showfliers=show_outliers)
        ax.axis('on'); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig);buf.seek(0)
        return base64.b64encode(buf.getvalue()).decode()
    imgs = [{'KEY':k,
             'BOX PLOT(원본)':f'<img src="data:image/png;base64,{make_img(g)}"/>',
             'BOX PLOT(이상치 숨김)':f'<img src="data:image/png;base64,{make_img(g,False)}"/>'}
            for k,g in 공구_금형강_입고.groupby('KEY')['제조공기(입고일-생산의뢰년월일)']]
    img_df = pd.DataFrame(imgs)

    # 13) final summary dataframe과 img_df merge
    merged_df = merged_df.merge(img_df, on='KEY')

    # 14) KEY별 LOT 갯수 기준으로 정렬
    merged_df = merged_df.sort_values(by='KEY별 LOT 갯수', ascending=False).reset_index(drop=True)
    return merged_df

# ------------------------------------------------------------------
# 원본데이터를 엑셀로 변환하는 함수
# ------------------------------------------------------------------
def to_excel_raw(df_raw):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_raw.to_excel(writer, sheet_name="RawData", index=False)
    output.seek(0)
    return output.read()

# ------------------------------------------------------------------
# 원본 “입고” DataFrame 로드 함수들
# ------------------------------------------------------------------
@st.cache_data
def load_raw_tan():
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
    
    return 탄합선재_탄합봉강_입고

@st.cache_data
def load_raw_sts():
    '''
    2) STS봉강 + 특수합금
    - 강종대분류: S, V
    - 형상: RB, FB, SB, HB
    '''
    # 0) raw_data parquet file import
    STS봉강_특수합금 = pd.read_parquet('2.STS봉강_특수합금.parquet')
    
    # 0-1) '주문구분'이 '강관원재'가 아닌 행만 추출
    STS봉강_특수합금 = STS봉강_특수합금[STS봉강_특수합금['주문구분'] != '강관원재']
    
    # 0-2) '열처리' 컬럼의 결측값을 'NH'로 채움
    STS봉강_특수합금['열처리'] = STS봉강_특수합금['열처리'].fillna('NH')
    
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
    
    # 6) 파생변수 생성 (제품구분)
    # 조건 설정
    conditions = [
        (STS봉강_특수합금_입고['대강종코드']=='S'),
        (STS봉강_특수합금_입고['대강종코드']=='V')
    ]
    
    # 각 조건에 해당하는 값 목록
    choices = ['STS봉강','특수합금']
    
    # 조건에 맞지 않는 경우 default로 None 할당
    STS봉강_특수합금_입고['제품구분'] = np.select(conditions, choices, default='None')
    
    # 7) 파생변수 생성 (기간구분)
    # 조건 설정
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
    STS봉강_특수합금_입고["기간구분"] = STS봉강_특수합금_입고["입고년월"].apply(determine_quarter)   
    
    # 8) 파생변수 생성 (압연구분)
    # 컬럼명 지정
    ps_col = '제품소성공정명'
    p_col  = '품종'
    o_col  = '주문형상'

    # 각 조건 정의
    cond1 = (
        STS봉강_특수합금_입고[ps_col].isin(['1단조 1800톤 프레스', 'RFM단조']) &
        (STS봉강_특수합금_입고[p_col] == '일반단조')
    )
    cond2 = (
        STS봉강_특수합금_입고[ps_col].isin(['1단조 1800톤 프레스', 'RFM단조']) &
        (STS봉강_특수합금_입고[p_col] == '대형재') &
        (STS봉강_특수합금_입고[o_col] == 'RB')
    )
    cond_1danjo   = cond1 | cond2

    cond_2danjo   = (
        (STS봉강_특수합금_입고[ps_col] == '2단조_9000톤_프레스') &
        (STS봉강_특수합금_입고[p_col] == '일반단조')
    )

    cond_daehyeong = STS봉강_특수합금_입고[ps_col].isin(['분괴압연', '분괴_SBM압연'])
    cond_sohyeong  = STS봉강_특수합금_입고[ps_col] == '소형압연'

    # np.select 로 파생컬럼 '압연구분' 추가
    STS봉강_특수합금_입고['압연구분'] = np.select(
        [cond_1danjo, cond_2danjo, cond_daehyeong, cond_sohyeong],
        ['1단조',      '2단조',      '대형압연',      '소형압연'],
        default='예외'
    )
    
    # 9) 파생변수 생성 : 각 컬럼 데이터의 공백을 제거하고, 문자열로 변환하여 KEY 생성
    STS봉강_특수합금_입고['KEY'] = (
        STS봉강_특수합금_입고['열처리_구분'].str.strip() + "_" +
        STS봉강_특수합금_입고['형상_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['특수제강_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['제품구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['기간구분'].str.strip() + "_" +
        STS봉강_특수합금_입고['열처리'].str.strip() + "_" +
        STS봉강_특수합금_입고['표면'].str.strip() + "_" +
        STS봉강_특수합금_입고['압연구분'].str.strip() 
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
    
    return STS봉강_특수합금_입고





















######################################################################################################


















@st.cache_data
def load_raw_sts_a():
    '''
    2) STS봉강 + 특수합금
    - 강종대분류: S, V
    - 형상: RB, FB, SB, HB
    '''
    # 0) raw_data parquet file import
    STS봉강_특수합금 = pd.read_parquet('2.STS봉강_특수합금.parquet')
    master_table = pd.read_parquet('master_table_sts_bar.parquet')
    
    # 0-1) '주문구분'이 '강관원재'가 아닌 행만 추출
    STS봉강_특수합금 = STS봉강_특수합금[STS봉강_특수합금['주문구분'] != '강관원재']
    
    # 0-2) '열처리' 컬럼의 결측값을 'NH'로 채움
    STS봉강_특수합금['열처리'] = STS봉강_특수합금['열처리'].fillna('NH')
    
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
    
    # 6) 파생변수 생성 (제품구분)
    # 조건 설정
    conditions = [
        (STS봉강_특수합금_입고['대강종코드']=='S'),
        (STS봉강_특수합금_입고['대강종코드']=='V')
    ]
    
    # 각 조건에 해당하는 값 목록
    choices = ['STS봉강','특수합금']
    
    # 조건에 맞지 않는 경우 default로 None 할당
    STS봉강_특수합금_입고['제품구분'] = np.select(conditions, choices, default='None')
    
    # 7) 파생변수 생성 (기간구분)
    # 조건 설정
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
    STS봉강_특수합금_입고["기간구분"] = STS봉강_특수합금_입고["입고년월"].apply(determine_quarter)   
    
    # 8) 파생변수 생성 (압연구분)
    # 컬럼명 지정
    ps_col = '제품소성공정명'
    p_col  = '품종'
    o_col  = '주문형상'

    # 각 조건 정의
    cond1 = (
        STS봉강_특수합금_입고[ps_col].isin(['1단조 1800톤 프레스', 'RFM단조']) &
        (STS봉강_특수합금_입고[p_col] == '일반단조')
    )
    cond2 = (
        STS봉강_특수합금_입고[ps_col].isin(['1단조 1800톤 프레스', 'RFM단조']) &
        (STS봉강_특수합금_입고[p_col] == '대형재') &
        (STS봉강_특수합금_입고[o_col] == 'RB')
    )
    cond_1danjo   = cond1 | cond2

    cond_2danjo   = (
        (STS봉강_특수합금_입고[ps_col] == '2단조_9000톤_프레스') &
        (STS봉강_특수합금_입고[p_col] == '일반단조')
    )

    cond_daehyeong = STS봉강_특수합금_입고[ps_col].isin(['분괴압연', '분괴_SBM압연'])
    cond_sohyeong  = STS봉강_특수합금_입고[ps_col] == '소형압연'

    # np.select 로 파생컬럼 '압연구분' 추가
    STS봉강_특수합금_입고['압연구분'] = np.select(
        [cond_1danjo, cond_2danjo, cond_daehyeong, cond_sohyeong],
        ['1단조',      '2단조',      '대형압연',      '소형압연'],
        default='예외'
    )
    # 9) 파생변수 생성 (주요대상)
    # 관리 대상이 되는 등급 리스트를 집합(set)으로 추출
    관리대상_리스트 = set(master_table['steel_grade'])
    
    # 여기서는 pandas 제공 함수인 isin()을 사용한 예시를 보여드립니다.
    STS봉강_특수합금_입고['주요대상'] = STS봉강_특수합금_입고['사내강종명'].isin(관리대상_리스트) \
        .map({True: '관리대상', False: '예외'})
    
    # 관리대상만 필터링
    STS봉강_특수합금_입고 = STS봉강_특수합금_입고[STS봉강_특수합금_입고['주요대상'] == '관리대상'] 
    
    # 9) 파생변수 생성 : 각 컬럼 데이터의 공백을 제거하고, 문자열로 변환하여 KEY 생성
    STS봉강_특수합금_입고['KEY'] = (
        STS봉강_특수합금_입고['열처리_구분'].str.strip() + "_" +
        STS봉강_특수합금_입고['형상_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['특수제강_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['제품구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['기간구분'].str.strip() + "_" +
        STS봉강_특수합금_입고['열처리'].str.strip() + "_" +
        STS봉강_특수합금_입고['표면'].str.strip() + "_" +
        STS봉강_특수합금_입고['압연구분'].str.strip() 
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
    
    return STS봉강_특수합금_입고






















@st.cache_data
def load_raw_sts_b():
    '''
    2) STS봉강 + 특수합금
    - 강종대분류: S, V
    - 형상: RB, FB, SB, HB
    '''
    # 0) raw_data parquet file import
    STS봉강_특수합금 = pd.read_parquet('2.STS봉강_특수합금.parquet')
    master_table = pd.read_parquet('master_table_sts_bar.parquet')
    
    # 0-1) '주문구분'이 '강관원재'가 아닌 행만 추출
    STS봉강_특수합금 = STS봉강_특수합금[STS봉강_특수합금['주문구분'] != '강관원재']
    
    # 0-2) '열처리' 컬럼의 결측값을 'NH'로 채움
    STS봉강_특수합금['열처리'] = STS봉강_특수합금['열처리'].fillna('NH')
    
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
    
    # 6) 파생변수 생성 (제품구분)
    # 조건 설정
    conditions = [
        (STS봉강_특수합금_입고['대강종코드']=='S'),
        (STS봉강_특수합금_입고['대강종코드']=='V')
    ]
    
    # 각 조건에 해당하는 값 목록
    choices = ['STS봉강','특수합금']
    
    # 조건에 맞지 않는 경우 default로 None 할당
    STS봉강_특수합금_입고['제품구분'] = np.select(conditions, choices, default='None')
    
    # 7) 파생변수 생성 (기간구분)
    # 조건 설정
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
    STS봉강_특수합금_입고["기간구분"] = STS봉강_특수합금_입고["입고년월"].apply(determine_quarter)   
    
    # 8) 파생변수 생성 (압연구분)
    # 컬럼명 지정
    ps_col = '제품소성공정명'
    p_col  = '품종'
    o_col  = '주문형상'

    # 각 조건 정의
    cond1 = (
        STS봉강_특수합금_입고[ps_col].isin(['1단조 1800톤 프레스', 'RFM단조']) &
        (STS봉강_특수합금_입고[p_col] == '일반단조')
    )
    cond2 = (
        STS봉강_특수합금_입고[ps_col].isin(['1단조 1800톤 프레스', 'RFM단조']) &
        (STS봉강_특수합금_입고[p_col] == '대형재') &
        (STS봉강_특수합금_입고[o_col] == 'RB')
    )
    cond_1danjo   = cond1 | cond2

    cond_2danjo   = (
        (STS봉강_특수합금_입고[ps_col] == '2단조_9000톤_프레스') &
        (STS봉강_특수합금_입고[p_col] == '일반단조')
    )

    cond_daehyeong = STS봉강_특수합금_입고[ps_col].isin(['분괴압연', '분괴_SBM압연'])
    cond_sohyeong  = STS봉강_특수합금_입고[ps_col] == '소형압연'

    # np.select 로 파생컬럼 '압연구분' 추가
    STS봉강_특수합금_입고['압연구분'] = np.select(
        [cond_1danjo, cond_2danjo, cond_daehyeong, cond_sohyeong],
        ['1단조',      '2단조',      '대형압연',      '소형압연'],
        default='예외'
    )
    # 9) 파생변수 생성 (주요대상)
    # 관리 대상이 되는 등급 리스트를 집합(set)으로 추출
    관리대상_리스트 = set(master_table['steel_grade'])
    
    # 여기서는 pandas 제공 함수인 isin()을 사용한 예시를 보여드립니다.
    STS봉강_특수합금_입고['주요대상'] = STS봉강_특수합금_입고['사내강종명'].isin(관리대상_리스트) \
        .map({True: '관리대상', False: '예외'})
    
    # 관리대상만 필터링
    STS봉강_특수합금_입고 = STS봉강_특수합금_입고[STS봉강_특수합금_입고['주요대상'] == '관리대상'] 
    
    # 9) 파생변수 생성 : 각 컬럼 데이터의 공백을 제거하고, 문자열로 변환하여 KEY 생성
    STS봉강_특수합금_입고['KEY'] = (
        STS봉강_특수합금_입고['형상_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['특수제강_구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['제품구분'].str.strip() + "_" + 
        STS봉강_특수합금_입고['열처리'].str.strip() + "_" +
        STS봉강_특수합금_입고['표면'].str.strip() + "_" +
        STS봉강_특수합금_입고['압연구분'].str.strip() + "_" +
        STS봉강_특수합금_입고['사내강종명']
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
    
    return STS봉강_특수합금_입고









































@st.cache_data
def load_raw_sts_wr():
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
    
    return STS선재_입고

@st.cache_data
def load_raw_tool():
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
    
    return 공구_금형강_입고

@st.cache_data
def load_raw_std11_61():
    '''
    4) 공구강/금형강
    - STD11_61종_대형재_BS품
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

    # 4) 필터링 (STD11_61종_대형재_BS품)
    # 미리 정규식 패턴 컴파일 (반복 호출시 속도 향상)
    pattern = re.compile(r'STD11|STD61')

    # Boolean 마스크 생성
    mask = (
        공구_금형강_입고['사내강종명'].str.contains(pattern, na=False)  # STD11 또는 STD61 포함
        & 공구_금형강_입고['표면'].eq('BS')                            # 표면 == 'BS'
        & 공구_금형강_입고['품종'].eq('대형재')                         # 품종 == '대형재'
    )

    # 한 번에 필터링
    공구_금형강_입고 = 공구_금형강_입고.loc[mask]
    
    # 5) 파생변수 생성 (입고시기)
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

    # 6) 파생변수 생성 (강종구분 KEY)
    # 조건설정 : 사내강종명이 STD11로 시작하면 'STD11종', 아니면 'STD61종'
    mask = 공구_금형강_입고['사내강종명'].str.startswith('STD11', na=False)
    공구_금형강_입고['강종구분 KEY'] = np.where(mask, 'STD11종', 'STD61종')

    # 7) 파생변수 생성 (Roll SIZE KEY)
    # ─────────────────────────────────────────────────────────────────────────────
    # STD61, STD11 용 폭→값 매핑 테이블 정의
    # ─────────────────────────────────────────────────────────────────────────────
    ranges_std61 = [
        (72.6,  77.6,  80),  (77.6,  82.1,  85),  (82.1,  87.1,  90),
        (87.1,  93.0,  95),  (93.0,  97.0, 100),  (97.0, 102.0,105),
        (102.0,107.0,110),  (107.0,112.0,115),  (112.0,117.0,120),
        (117.0,122.0,125),  (122.0,127.0,130),  (127.0,132.0,135),
        (132.0,137.0,140),  (137.0,142.0,145),  (142.0,147.0,150),
        (147.0,152.0,155),  (152.0,157.0,160),  (157.0,162.0,165),
        (162.0,167.0,170),  (167.0,172.0,175),  (172.0,177.0,180),
        (177.0,182.0,185),  (182.0,187.0,190),  (187.0,191.0,195),
        (191.0,196.0,200),  (196.0,207.0,210),
    ]

    ranges_std11 = [
        (72.6,  77.6,  80),  (77.6,  82.1,  85),  (82.1,  87.1,  90),
        (87.1,  93.0,  95),  (93.0,  97.0, 100),  (97.0, 102.0,105),
        (102.0,107.0,110),  (107.0,112.0,115),  (112.0,117.0,120),
        (117.0,122.0,125),  (122.0,127.0,130),  (127.0,132.0,135),
        (132.0,137.0,140),  (137.0,142.0,145),  (142.0,147.0,150),
        (147.0,152.0,155),  (152.0,157.0,160),  (157.0,162.0,165),
        (162.0,167.0,170),  (167.0,172.0,175),  (172.0,177.0,180),
        (177.0,181.0,185),  (181.0,186.0,190),  (186.0,191.0,195),
        (191.0,196.0,200),  (196.0,207.0,210),
    ]

    # ─────────────────────────────────────────────────────────────────────────────
    # 행 단위로 레이블링해 줄 함수 정의
    # ─────────────────────────────────────────────────────────────────────────────
    def label_row(row):
        w = row['설계폭']
        kind = row['품종']
        shape = row['주문형상']
        key = row['강종구분 KEY']
        
        # '대형재'만 처리
        if kind != '대형재':
            return None
        
        # FB 형상 → W 레이블
        if shape == 'FB':
            if w < 205:
                return '205W'
            elif w < 280:
                return '280W'
            elif w < 380:
                return '380W'
            elif w < 517:
                return '517W'
            else:
                return '630W'
        
        # RB 형상 → STD61/11 매핑
        if shape == 'RB':
            table = ranges_std61 if key == 'STD61종' else ranges_std11 if key == 'STD11종' else []
            for lo, hi, label in table:
                if lo <= w < hi:
                    return label
        
        # 그 외
        return None

    # ─────────────────────────────────────────────────────────────────────────────
    # DataFrame 에 적용
    # ─────────────────────────────────────────────────────────────────────────────
    공구_금형강_입고['Roll SIZE'] = 공구_금형강_입고.apply(label_row, axis=1)
    
    # 8) 파생변수 생성 (종합 최종 KEY)
    # 조건설정
    # '품종', '주문형상', '표면' 컬럼의 공백을 제거하고, 각 컬럼을 문자열로 변환하여 KEY 생성
    공구_금형강_입고["KEY"] = (
    공구_금형강_입고["입고시기"].str.strip().astype(str) + "_" +
    공구_금형강_입고["품종"].str.strip().astype(str) + "_" +
    공구_금형강_입고["강종구분 KEY"].str.strip().astype(str) + "_" +
    공구_금형강_입고["표면"].str.strip().astype(str) + "_" +
    공구_금형강_입고["재료코드"].str.strip().astype(str) + "_" +
    공구_금형강_입고["Roll SIZE"].str.strip().astype(str)
    )

    # 9) 통계치 계산
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

    return 공구_금형강_입고

@st.cache_data
def load_raw_std11_61_new_key():
    '''
    4) 공구강/금형강
    - STD11_61종_대형재_BS품
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

    # 4) 필터링 (STD11_61종_대형재_BS품)
    # 미리 정규식 패턴 컴파일 (반복 호출시 속도 향상)
    pattern = re.compile(r'STD11|STD61')

    # Boolean 마스크 생성
    mask = (
        공구_금형강_입고['사내강종명'].str.contains(pattern, na=False)  # STD11 또는 STD61 포함
        & 공구_금형강_입고['표면'].eq('BS')                            # 표면 == 'BS'
        & 공구_금형강_입고['품종'].eq('대형재')                         # 품종 == '대형재'
    )

    # 한 번에 필터링
    공구_금형강_입고 = 공구_금형강_입고.loc[mask]
    
    # 5) 파생변수 생성 (입고시기)
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

    # 6) 파생변수 생성 (강종구분 KEY)
    # 조건설정 : 사내강종명이 STD11로 시작하면 'STD11종', 아니면 'STD61종'
    mask = 공구_금형강_입고['사내강종명'].str.startswith('STD11', na=False)
    공구_금형강_입고['강종구분 KEY'] = np.where(mask, 'STD11종', 'STD61종')

    # 7) 파생변수 생성 (Roll SIZE KEY)
    # ─────────────────────────────────────────────────────────────────────────────
    # STD61, STD11 용 폭→값 매핑 테이블 정의
    # ─────────────────────────────────────────────────────────────────────────────
    ranges_std61 = [
        (72.6,  77.6,  80),  (77.6,  82.1,  85),  (82.1,  87.1,  90),
        (87.1,  93.0,  95),  (93.0,  97.0, 100),  (97.0, 102.0,105),
        (102.0,107.0,110),  (107.0,112.0,115),  (112.0,117.0,120),
        (117.0,122.0,125),  (122.0,127.0,130),  (127.0,132.0,135),
        (132.0,137.0,140),  (137.0,142.0,145),  (142.0,147.0,150),
        (147.0,152.0,155),  (152.0,157.0,160),  (157.0,162.0,165),
        (162.0,167.0,170),  (167.0,172.0,175),  (172.0,177.0,180),
        (177.0,182.0,185),  (182.0,187.0,190),  (187.0,191.0,195),
        (191.0,196.0,200),  (196.0,207.0,210),
    ]

    ranges_std11 = [
        (72.6,  77.6,  80),  (77.6,  82.1,  85),  (82.1,  87.1,  90),
        (87.1,  93.0,  95),  (93.0,  97.0, 100),  (97.0, 102.0,105),
        (102.0,107.0,110),  (107.0,112.0,115),  (112.0,117.0,120),
        (117.0,122.0,125),  (122.0,127.0,130),  (127.0,132.0,135),
        (132.0,137.0,140),  (137.0,142.0,145),  (142.0,147.0,150),
        (147.0,152.0,155),  (152.0,157.0,160),  (157.0,162.0,165),
        (162.0,167.0,170),  (167.0,172.0,175),  (172.0,177.0,180),
        (177.0,181.0,185),  (181.0,186.0,190),  (186.0,191.0,195),
        (191.0,196.0,200),  (196.0,207.0,210),
    ]

    # ─────────────────────────────────────────────────────────────────────────────
    # 행 단위로 레이블링해 줄 함수 정의
    # ─────────────────────────────────────────────────────────────────────────────
    def label_row(row):
        w = row['설계폭']
        kind = row['품종']
        shape = row['주문형상']
        key = row['강종구분 KEY']
        
        # '대형재'만 처리
        if kind != '대형재':
            return None
        
        # FB 형상 → W 레이블
        if shape == 'FB':
            if w < 205:
                return '205W'
            elif w < 280:
                return '280W'
            elif w < 380:
                return '380W'
            elif w < 517:
                return '517W'
            else:
                return '630W'
        
        # RB 형상 → STD61/11 매핑
        if shape == 'RB':
            table = ranges_std61 if key == 'STD61종' else ranges_std11 if key == 'STD11종' else []
            for lo, hi, label in table:
                if lo <= w < hi:
                    return label
        
        # 그 외
        return None

    # ─────────────────────────────────────────────────────────────────────────────
    # DataFrame 에 적용
    # ─────────────────────────────────────────────────────────────────────────────
    공구_금형강_입고['Roll SIZE'] = 공구_금형강_입고.apply(label_row, axis=1)
    
    # 8) 파생변수 생성 (BS 구분기준)
    # 조건 정의
    cond = (
        공구_금형강_입고['Roll SIZE'].isin(['205W', '280W'])
        | (
            (공구_금형강_입고['Roll SIZE'] == '380W')
            & (공구_금형강_입고['재료코드'] == 'D015')
        )
    )

    # 파생변수 생성
    공구_금형강_입고['BS_구분기준'] = np.where(
        cond,
        '25T*310W 미만',
        '25T*310W 초과'
    )
    
    # 9) 파생변수 생성 (종합 최종 KEY)
    # 조건설정
    # '품종', '주문형상', '표면' 컬럼의 공백을 제거하고, 각 컬럼을 문자열로 변환하여 KEY 생성
    공구_금형강_입고["KEY"] = (
    공구_금형강_입고["입고시기"].str.strip().astype(str) + "_" +
    공구_금형강_입고["BS_구분기준"].str.strip().astype(str)
    )

    # 10) 통계치 계산
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

    return 공구_금형강_입고

# ------------------------------------------------------------------
# 원본데이터를 엑셀로 변환하는 함수 (캐싱 적용)
# ------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def to_excel_raw(df_raw: pd.DataFrame) -> bytes:
    """
    DataFrame -> Excel bytes 변환을 캐시합니다.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_raw.to_excel(writer, sheet_name="RawData", index=False)
    output.seek(0)
    return output.read()

# ------------------------------------------------------------------
# Streamlit UI
# ------------------------------------------------------------------
st.set_page_config(page_title="입고 분석 앱", layout="wide")

# 1) 앱 헤더 이미지
# header.png 파일을 프로젝트 루트에 두고, 컬럼 전체 너비로 표시
st.image("history_kv.png", use_column_width=True)

st.title("제조공기 분석 결과 테이블")
st.subheader("2024년 ~ 2025년 1분기 내수/수출/강관원재 입고실적")

# 데이터 선택
dataset = st.sidebar.radio(
    "분석할 데이터 선택",
    ("1.탄합선재_탄합봉강","2.STS봉강_특수합금","2-1.STS봉강_특수합금_A","2-2.STS봉강_특수합금_B","3.STS선재","4.공구_금형강","5.STD11_61종_대형재_BS품","6.STD11_61종_대형재_BS품_새로운KEY")
    )

with st.spinner("데이터 로드 중..."):
    if dataset == "1.탄합선재_탄합봉강":
        df = load_data_tan()
        raw_df = load_raw_tan()
        subtitle = '''
        1) 탄합선재 + 탄합봉강 + MARAGING강
        - 강종대분류: A,B,C,K,M (대강종명 == '탄합강')
        - 형상(주문형상) : WR, RB, FB, SB, HB
        '''
    elif dataset == "2.STS봉강_특수합금":
        df = load_data_sts()
        raw_df = load_raw_sts()
        subtitle = '''
        2) STS봉강 + 특수합금 (MARAGING강, 강관원재 제외)
        - 강종대분류: S, V
        - 형상: RB, FB, SB, HB
        '''
    elif dataset == "2-1.STS봉강_특수합금_A":
        df = load_data_sts_a()
        raw_df = load_raw_sts_a()
        subtitle = '''
        2-1) STS봉강 + 특수합금 (MARAGING강, 강관원재 제외)
        - 강종대분류: S, V
        - 형상: RB, FB, SB, HB
        - 관리대상 필터링
        '''
    elif dataset == "2-2.STS봉강_특수합금_B":
        df = load_data_sts_b()
        raw_df = load_raw_sts_b()
        subtitle = '''
        2-2) STS봉강 + 특수합금 (MARAGING강, 강관원재 제외)
        - 강종대분류: S, V
        - 형상: RB, FB, SB, HB
        - 관리대상 필터링, 열처리/입고기간 KEY제외
        '''
    elif dataset == "3.STS선재":
        df = load_data_sts_wr()
        raw_df = load_raw_sts_wr()
        subtitle = '''
        3) STS/특수합금 선재 
        - 강종대분류 : S,V
        - 형상: WR
        '''
    elif dataset == "4.공구_금형강":
        df = load_data_tool()
        raw_df = load_raw_tool()
        subtitle = '''
        4) 공구강/금형강 + Forged Block(GEN 5.x)
        - 강종대분류: T
        - 형상 : ALL
        '''
    elif dataset == "5.STD11_61종_대형재_BS품":
        df = load_data_std11_61()
        raw_df = load_raw_std11_61()
        subtitle = '''
        5) 공구강/금형강 : STD11_61종_대형재_BS품
        '''
    else: # "6.STD11_61종_대형재_BS품_새로운KEY"
        df = load_data_std11_61_new_key()
        raw_df = load_raw_std11_61_new_key()
        subtitle = '''
        5) 공구강/금형강 : STD11_61종_대형재_BS품_새로운KEY
        '''

# 3) 선택된 데이터셋에 따른 마크다운 출력
st.markdown(subtitle)

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

# (A) 요약 결과 엑셀 다운로드
excel_data = to_excel_with_images(df)
st.download_button("📥 결과 테이블 엑셀 다운로드",
                   data=excel_data,
                   file_name=f"{dataset}_분석결과.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                   )

# (B) 원본 가공 데이터 다운로드
raw_excel = to_excel_raw(raw_df)
st.download_button("📥 원본 입고 데이터 엑셀 다운로드",
                   data=raw_excel,
                   file_name=f"{dataset}_원본입고데이터.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
