import pandas as pd
from datetime import date, datetime, timedelta
import pytz


# create substitute holiday column
# 대략 아래와 같이 대체 공휴일을 지정할 수는 있으나 대체공휴일이 지정되는 방식이 해마다 달라 지정이 어려움
# 따라서 대체 공휴일은 추가하지 않는다
def create_substitute_holiday(df: pd.DataFrame) -> pd.DataFrame:
    # if lunar_holiday is not empty, the day is lunar_holiday
    # 3일 연휴와 1일의 휴일을 구분해서 작업해야 함
    df['is_lunar_holiday'] = df['lunar_holiday'].apply(lambda x: x != '')

    # If lunar_holiday is true and day_short_name_kor is "day", the next first Lunar_holiday changes false to true
    df['substitute_holiday_exists'] = df['is_lunar_holiday'] & (df['day_short_name_kor'] == '일')

    df['substitute_holiday_exists_expand'] = \
        (df['is_lunar_holiday'] & df['substitute_holiday_exists'].shift(1).fillna(False)) | \
        (df['is_lunar_holiday'] & df['substitute_holiday_exists'].shift(2).fillna(False))
    df['substitute_holiday_exists'] = df['substitute_holiday_exists'] | df['substitute_holiday_exists_expand']
    df.drop(['substitute_holiday_exists_expand'], axis=1, inplace=True)

    # if substitute_holiday_exists is changed from true to false, the change false to true
    df['substitute_holiday'] = ~df['substitute_holiday_exists'] & df['substitute_holiday_exists'].shift(1).fillna(False)

    # if substitute_holiday is true, set lunar_holiday value to "대체공휴일"
    df['lunar_holiday_expand_add'] = df['lunar_holiday'].shift(1).fillna(False)
    df['lunar_holiday_expand'] = df.apply(
        lambda x: f"대체공휴일({x['lunar_holiday_expand_add']})" if x['substitute_holiday'] else x['lunar_holiday'], axis=1)

    df['lunar_holiday'] = df.apply(
        lambda x: x['lunar_holiday_expand'] if x['lunar_holiday_expand'] != '' else x['lunar_holiday'], axis=1)
    df.drop(['lunar_holiday_expand_add', 'lunar_holiday_expand'], axis=1, inplace=True)


def create_bok_nal(df: pd.DataFrame) -> pd.DataFrame:
    df['first_char_of_gapja_day'] = df['lunar_gapja_kor'].str[8]

    # filter solar_term_name is '하지' or '입추' or first_char_of_gapja_day is '경'
    df_bok = df.loc[
        (df['solar_term_name'] == '하지') | (df['solar_term_name'] == '입추') | (df['first_char_of_gapja_day'] == '경'),
        ['date', 'solar_term_name', 'lunar_gapja_kor', 'first_char_of_gapja_day']].copy()

    df_bok['term_name_shift_1'] = df_bok['solar_term_name'].shift(1).fillna('')
    df_bok['term_name_shift_2'] = df_bok['solar_term_name'].shift(2).fillna('')
    df_bok['term_name_shift_3'] = df_bok['solar_term_name'].shift(3).fillna('')
    df_bok['term_name_shift_4'] = df_bok['solar_term_name'].shift(4).fillna('')

    df_bok['gapja_shift_1'] = df_bok['first_char_of_gapja_day'].shift(1).fillna('')
    df_bok['gapja_shift_2'] = df_bok['first_char_of_gapja_day'].shift(2).fillna('')
    df_bok['gapja_shift_3'] = df_bok['first_char_of_gapja_day'].shift(3).fillna('')
    df_bok['gapja_shift_4'] = df_bok['first_char_of_gapja_day'].shift(4).fillna('')

    # make is_chobok column with lambda function which is checking solar_term_name is '하지' and first_char_of_gapja_day is '경'
    df_bok['is_chobok'] = df_bok.apply(
        lambda x: ((x['solar_term_name'] != '입추') & (x['term_name_shift_2'] == '하지') & (x['gapja_shift_2'] == '경')) | \
                  ((x['solar_term_name'] != '입추') & (x['term_name_shift_3'] == '하지') & (x['gapja_shift_3'] != '경')),
        axis=1)

    df_bok['is_jungbok'] = df_bok.apply(
        lambda x: ((x['solar_term_name'] != '입추') & (x['term_name_shift_3'] == '하지') & (x['gapja_shift_3'] == '경')) | \
                  ((x['solar_term_name'] != '입추') & (x['term_name_shift_4'] == '하지') & (x['gapja_shift_4'] != '경')),
        axis=1)

    df_bok['is_malbok'] = df_bok.apply(
        lambda x: ((x['solar_term_name'] == '입추') & (x['first_char_of_gapja_day'] == '경')) | \
                  ((x['term_name_shift_1'] == '입추') & (x['gapja_shift_1'] != '경')), axis=1)

    df_bok['chobok_name'] = df_bok['is_chobok'].apply(lambda x: '초복' if x else '')
    df_bok['jungbok_name'] = df_bok['is_jungbok'].apply(lambda x: '중복' if x else '')
    df_bok['malbok_name'] = df_bok['is_malbok'].apply(lambda x: '말복' if x else '')
    df_bok['bok_name'] = df_bok['chobok_name'] + df_bok['jungbok_name'] + df_bok['malbok_name']

    df_bok.drop(
        ['solar_term_name', 'lunar_gapja_kor', 'first_char_of_gapja_day', 'term_name_shift_1', 'term_name_shift_2',
         'term_name_shift_3', 'term_name_shift_4', 'gapja_shift_1', 'gapja_shift_2', 'gapja_shift_3', 'gapja_shift_4',
         'is_chobok', 'is_jungbok', 'is_malbok', 'chobok_name', 'jungbok_name', 'malbok_name'], axis=1, inplace=True)

    df.drop(['first_char_of_gapja_day'], axis=1, inplace=True)

    df = pd.merge(df, df_bok, left_on='date', right_on='date', how='left')

    return df


def merge_holiday(df: pd.DataFrame) -> pd.DataFrame:
    df['holiday'] = df.apply(lambda x: x['lunar_holiday'] if x['lunar_holiday'] != '' else x['solar_holiday'], axis=1)
    df.drop(['lunar_holiday', 'solar_holiday'], axis=1, inplace=True)

    return df


# create solar 24 term
def create_solar24_term(start_date_str: str, end_date_str: str) -> pd.DataFrame:
    import eacal
    c_k = eacal.EACal(ko=True)

    start_year = int(start_date_str[:4])
    end_year = int(end_date_str[:4])

    values = []
    for year in range(start_year, end_year):
        for x in c_k.get_annual_solar_terms(year):
            values.append([datetime.strftime(x[2], "%Y-%m-%d"), x[1], x[0]])
            # print("%2d %s %s" % (x[1], x[0], datetime.strftime(x[2], "%Y-%m-%d %H:%M %Z")))

    df = pd.DataFrame(values, columns=['date', 'solar_term_number', 'solar_term_name'])
    print(df)
    df.info()

    return df


def create_solar_holiday(df: pd.DataFrame) -> pd.DataFrame:
    # gregorian calendar holidays in korea
    holidays_in_kor = [
        {'name': '신년', 'day': '01-01'},
        {'name': '삼일절', 'day': '03-01'},
        {'name': '어린이날', 'day': '05-05'},
        {'name': '현충일', 'day': '06-06'},
        {'name': '광복절', 'day': '08-15'},
        {'name': '개천절', 'day': '10-03'},
        {'name': '한글날', 'day': '10-09'},
        {'name': '성탄절', 'day': '12-25'}
    ]
    days = [holiday['day'] for holiday in holidays_in_kor]

    df['solar_holiday'] = df['date'].apply(lambda x: x[5:])

    # filter df['solar_holiday'] which is in holidays_in_kor's day key
    df['solar_holiday'] = df['solar_holiday'].apply(
        lambda x: [holiday['name'] for holiday in holidays_in_kor if holiday['day'] == x][0] if x in days else '')

    return df


# Create lunar holiday column from lunar date column
def create_lunar_holiday(df: pd.DataFrame) -> pd.DataFrame:
    # create lunar holiday column as boolean from lunar_date column
    # which contain '12-31', '01-01', '01-02', '04-08', '08-14', '08-15', '08-16'
    df['lunar_holiday'] = df['lunar_date'].apply(lambda x: x[5:])

    # lunar 1-1
    df['lunar_new_year'] = df['lunar_holiday'].apply(lambda x: '설날' if x in ['01-01', '01-02'] else '')
    df['lunar_new_year_beforeday'] = df['lunar_new_year'].shift(-1)

    # merge lunar_new_year and lunar_new_year_beforeday columns to lunar_new_year column
    df['lunar_new_year'] = df.apply(
        lambda x: x['lunar_new_year'] if x['lunar_new_year'] == '설날' else x['lunar_new_year_beforeday'], axis=1)
    df.drop(['lunar_new_year_beforeday'], axis=1, inplace=True)

    # lunar 4-8
    df['buddha_birthday'] = df['lunar_holiday'].apply(lambda x: '석가탄신일' if x == '04-08' else '')

    # lunar 8-14, 8-15, 8-16
    df['chuseok'] = df['lunar_holiday'].apply(lambda x: '추석' if x in ['08-14', '08-15', '08-16'] else '')

    # merge lunar_new_year, buddha_birthday, chuseok columns to lunar_holiday column
    df['lunar_holiday'] = df['lunar_new_year'] + df['buddha_birthday'] + df['chuseok']

    # drop lunar_new_year, buddha_birthday, chuseok columns
    df.drop(['lunar_new_year', 'buddha_birthday', 'chuseok'], axis=1, inplace=True)

    return df


# Create lunar calendar
def create_lunar_date(df: pd.DataFrame) -> pd.DataFrame:
    from korean_lunar_calendar import KoreanLunarCalendar

    # add new column calendar_object which has object of KoreanLunarCalendar()
    df['calendar_object'] = df['date_base'].apply(lambda x: KoreanLunarCalendar())

    # add new column lunar_date_object to df using calendar.setSolarDate function which has params df.year, df.month, df.day
    df['lunar_date_object'] = df.apply(lambda x:
                                       x['calendar_object'].setSolarDate(x['year'], x['month'], x['day']), axis=1)
    df['lunar_date'] = df['calendar_object'].apply(lambda x: x.LunarIsoFormat())
    df['lunar_gapja_kor'] = df['calendar_object'].apply(lambda x: x.getGapJaString())
    df['lunar_gapja_chinese'] = df['calendar_object'].apply(lambda x: x.getChineseGapJaString())
    df.drop(['calendar_object', 'lunar_date_object'], axis=1, inplace=True)

    return df


def create_dim_date(start_date_str: str, end_date_str: str):
    days_names = {
        i: names
        for i, names
        in enumerate([
            ['월요일', '월', 'Monday', 'Mon'],
            ['화요일', '화', 'Tuesday', 'Tue'],
            ['수요일', '수', 'Wednesday', 'Wed'],
            ['목요일', '목', 'Thursday', 'Thu'],
            ['금요일', '금', 'Friday', 'Fri'],
            ['토요일', '토', 'Saturday', 'Sat'],
            ['일요일', '일', 'Sunday', 'Sun']
        ])
    }

    # Create a list of dates
    start_date = date.fromisoformat(start_date_str)
    end_date = date.fromisoformat(end_date_str)
    date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days)]

    # Create a data frame
    df = pd.DataFrame(date_list, columns=['date_base'])

    # create dim_date dataframe from date frame
    df['date_key'] = df['date_base'].apply(lambda x: x.strftime('%Y%m%d'))
    df['date'] = df['date_base'].apply(lambda x: x.strftime('%Y-%m-%d'))

    df['year'] = df['date_base'].apply(lambda x: x.year)
    df['year_iso8601'] = df['date_base'].apply(lambda x: int(x.strftime('%G')))
    df['month'] = df['date_base'].apply(lambda x: x.month)
    df['day'] = df['date_base'].apply(lambda x: x.day)

    df['day_name_kor'] = df['date_base'].apply(lambda x: days_names[x.weekday()][0])
    df['day_short_name_kor'] = df['date_base'].apply(lambda x: days_names[x.weekday()][1])
    df['day_name'] = df['date_base'].apply(lambda x: days_names[x.weekday()][2])
    df['day_short_name'] = df['date_base'].apply(lambda x: days_names[x.weekday()][3])

    df['month_name'] = df['date_base'].apply(lambda x: x.strftime('%B'))
    df['month_short_name'] = df['date_base'].apply(lambda x: x.strftime('%b'))

    df['week_of_year'] = df['date_base'].apply(lambda x: int(x.strftime('%W')))
    df['week_of_year_iso8601'] = df['date_base'].apply(lambda x: int(x.strftime('%V')))

    df['quarter'] = df['date_base'].apply(lambda x: int((x.month - 1) / 3) + 1)
    df['half_year'] = df['date_base'].apply(lambda x: int((x.month - 1) / 6) + 1)

    df['day_of_week'] = df['date_base'].apply(lambda x: x.isoweekday())
    df['day_of_month'] = df['date_base'].apply(lambda x: x.day)
    df['day_of_year'] = df['date_base'].apply(lambda x: x.timetuple().tm_yday)

    # calculate dataframe column calculation that day_of_year minus min date value of quarter
    df['day_of_quarter'] = df['day_of_year'] - df.groupby(['quarter'])['day_of_year'].transform(min) + 1
    df['day_of_half_year'] = df['day_of_year'] - df.groupby(['half_year'])['day_of_year'].transform(min) + 1

    df['is_last_day_of_week'] = df['date_base'].apply(lambda x: x.isoweekday() == 7)
    df['is_last_day_of_month'] = df.groupby(['year', 'month'])['date_base'].transform(max) == df['date_base']
    df['is_last_day_of_quarter'] = df.groupby(['year', 'quarter'])['date_base'].transform(max) == df['date_base']
    df['is_last_day_of_half_year'] = df.groupby(['year', 'half_year'])['date_base'].transform(max) == df['date_base']
    df['is_last_day_of_year'] = df.groupby(['year'])['date_base'].transform(max) == df['date_base']

    df['is_first_day_of_year_iso8601'] = \
        df.groupby(['year_iso8601'])['date_base'].transform(min) == df['date_base']
    df['is_last_day_of_year_iso8601'] = \
        df.groupby(['year_iso8601'])['date_base'].transform(max) == df['date_base']

    return df


if __name__ == '__main__':
    start_date_str = '2000-01-01'
    end_date_str = '2051-01-01'

    df = create_dim_date(start_date_str=start_date_str, end_date_str=end_date_str)

    # 공휴일 추가
    df = create_solar_holiday(df)

    # # 음력 공휴일 추가
    df = create_lunar_date(df)
    df = create_lunar_holiday(df)

    df = merge_holiday(df)

    # call function create 24 solar term
    df_solar = create_solar24_term(start_date_str=start_date_str, end_date_str=end_date_str)

    df = pd.merge(df, df_solar, left_on='date', right_on='date', how='left')

    df['solar_term_number'] = df['solar_term_number'].fillna(-1).astype(int)
    df['solar_term_name'] = df['solar_term_name'].fillna('')

    df = create_bok_nal(df)

    KST = pytz.timezone('Asia/Seoul')
    df['createddate'] = datetime.now(KST).strftime('%Y-%m-%d')

    print(df)
    df.info()

    sink_file = 'dim_date_{}.snappy.parquet'.format(datetime.now(KST).strftime('%Y%m%d'))
    df.to_parquet(sink_file, index=False, partition_cols=None, compression='snappy')
