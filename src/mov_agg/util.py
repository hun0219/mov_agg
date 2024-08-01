import pandas as pd


def merge(load_dt="20240724"):
    read_df = pd.read_parquet('~/tmp/test_parquet')
    cols = ['movieCd', #영화의 대표코드를 출력합니다.
        'movieNm', #영화명(국문)을 출력합니다.
        #'openDt', #영화의 개봉일을 출력합니다.
        #'audiCnt', #해당일의 관객수를 출력합니다.
        'load_dt', # 입수일자
        'multiMovieYn', #다양성영화 유무
        'repNationCd', #한국외국영화 유무
       ]
    df = read_df[cols]
    print(df.head(30))
    # 20240724날짜 울버린만 조회(20247781영화코드)
    #dw = df[(df['movieCd'] == '20235974') & (df['load_dt'] == int(load_dt))].copy() #날짜 조건 load_dt 인자를 받기
    dw = df[(df['load_dt'] == int(load_dt))].copy() #날짜 조건 load_dt 인자를 받기
    print(dw)
    print(dw.dtypes)
    # 카테고리 타입 -> object
    dw['load_dt'] = dw['load_dt'].astype('object')
    dw['multiMovieYn'] = dw['multiMovieYn'].astype('object')
    dw['repNationCd'] = dw['repNationCd'].astype('object')
    print(dw.dtypes)
    
    # NaN 값 unknown으로 변경
    dw['multiMovieYn'] = dw['multiMovieYn'].fillna('unknown')
    dw['repNationCd'] = dw['repNationCd'].fillna('unknown')
    #result = df_where.concat(['multiMovieYn','repNationCd'], ignore_index=True, sort=False)
    
    #머지
    u_mul = dw[dw['multiMovieYn'] == 'unknown']
    u_nat = dw[dw['repNationCd'] == 'unknown']
    m_df = pd.merge(u_mul, u_nat, on='movieCd', suffixes=('_m', '_n'))
    print(m_df)
    m_df.loc[m_df['multiMovieYn_m'] == 'unknown', 'multiMovieYn_m'] = m_df['multiMovieYn_n']
    print(m_df)
    print(m_df.columns)
    print(m_df.columns[5:])
    print(m_df)
    m_df.drop(m_df.columns[5:], axis=1, inplace = True) #5부터 axis 열 다짜름 #  m_df에 적용 시키는게 inplace = True


    #print()
    print(m_df)
    return m_df

merge()
