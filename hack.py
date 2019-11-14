import streamlit as st
import os
import psycopg2
import pandas as pd
from datetime import datetime, timedelta

from oauth2client.service_account import ServiceAccountCredentials
import gspread
import gspread_dataframe as gd


def load_data(unitcode,start,end):

    unitcode = str(unitcode)
    start = str(start)
    end = str(end)

    redshift_conn = psycopg2.connect(dbname='warehouse',
                        host='warehouse.vacasa.services',
                        user=os.environ.get('redshift_db_user'),
                        password=os.environ.get('redshift_db_pass'),
                        port=5439)

    sql ='''
    select r.unit_id
    , u.unitcode
    , u.cleaningfee
    , r.date
    , r.rate
    , r.component_analyst_minrate "analyst"
    , r.component_owner_minrate "owner"
    , r.component_analyst_unit_minrate "unit"
    , r.component_auto_minrate "auto"
    , c.countryname
    from rates.unit_date_rate r
    join vacasa.units as u on r.unit_id = u.unitid
    join vacasa.regions re on u.region = re.regionid
    join vacasa.countries c on re.country_id = c.countryid
    where u.unitcode = '{}'
    and r.date>= '{}'
    and r.date<='{}'
    order by r.unit_id, r.date asc;
    '''.format(str(unitcode),start,end)
    
    df = pd.read_sql_query(sql, redshift_conn)
    
    return df

def get_discount_matrix(country):
    path_to_json = 'spreadsheet.json'
    sheetname = 'Matriz Reservaciones'

    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

    creds = ServiceAccountCredentials.from_json_keyfile_name(path_to_json, scope)
    client = gspread.authorize(creds)

    sheet = client.open(sheetname)
    destination = sheet.worksheet(country)
    
    return gd.get_as_dataframe(destination)

def discount_matrix(country):
    a = get_discount_matrix(country)
    a.columns = a.iloc[0]
    a.index = a.loc[:,"Booking Window"]
    a = a.iloc[1:7,1:7]

    return a

def get_max_discount(country,end,start):
    lenght = (end-start).days
    booking_window = (start - datetime.now().date()).days
    dis_matrix = discount_matrix(country)

    if lenght<=30:
        dis_matrix = dis_matrix.filter(like = str(lenght)+',',axis=1)
    else:
        dis_matrix = dis_matrix.filter(like = '30,',axis=1)
    if booking_window<=30:
        dis_matrix = dis_matrix.filter(like = str(booking_window)+',',axis=0)
    else:
        dis_matrix = dis_matrix.filter(like = '30,',axis=0)
    
    return dis_matrix.iloc[0,0]


st.title('Min Rate Helper')
st.markdown("""This is the first version of this program. The idea is help CX team in theirs daily works when they receive a guest's phone call asking for discount in a certain timefrime.""")
# Ask for the word to search for

unitcode = st.sidebar.text_input("What is the unitcode of the guest property option? ",value='COBOCI05')
start = st.sidebar.date_input("First night reservation",datetime.now().date())
end = st.sidebar.date_input("Last night reservation",(datetime.now()+ timedelta(days=5)))


st.subheader('Reservation from {} to {}, for {} '.format(start,end,unitcode))

with st.spinner('Loading data from warehouse...'):
    data = load_data(unitcode,str(start),str(end))
    country = data.countryname.unique()[0]

with st.spinner('Making calculations...'):

    data['global_min_rate'] = data[['analyst','owner','unit','auto']].max(axis=1)
    data = data[['unitcode','date','rate','global_min_rate']]
    data['max_discount'] = data.rate - data.global_min_rate
    data.max_discount  = data.max_discount.astype(int)
    data['discount_percent'] = data.max_discount/data.rate*100

    actual_rent = data.rate.sum()
    min_rent = data.global_min_rate.sum()

    first_offer = round((actual_rent + min_rent)/2)

    ##select max percentage error
    max_discount = get_max_discount(country,end,start)
    min_discount = 1-min_rent/actual_rent

    disc = min(max_discount,min_discount)

    lenght = (end-start).days
    booking_window = (start - datetime.now().date()).days

    text = """
            Actual Rent: {}.

            Nights: {}.

            Booking Window: {}.
            """.format(str(round(actual_rent,0)),
                        str(lenght),
                        str(booking_window))
    st.info(text)
    # if min_discount<max_discount:
    #     st.success("""
    #         First offer to potential guest: {}. Discount percent: {}%""".format(
    #                                                 str(round(actual_rent*(1-min_discount),2)),
    #                                                 str(min_discount)))
    #     st.dataframe(data)
    # else:
    #     st.success("""
    #         First offer to potential guest: {}.

    #         Discount percent: {}%""".format(
    #                                 str(round(actual_rent*(1-max_discount),2)),
    #                                 str(max_discount*100)))
    #     st.dataframe(data)

    st.success("""
        First offer to potential guest: {}. Discount percent: {}%""".format(
                                                str(round(actual_rent*(1-disc),2)),
                                                str(disc)))
    st.dataframe(data)

    
st.balloons()