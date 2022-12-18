import glob

import pandas as pd
import streamlit as st
from PIL import Image

st.set_page_config(page_title="MLOPS",
                   layout="wide")

st.title("MLOPS - Analysis on parking sensor", anchor=None)

parking_path = "analysis_data"


def most_least_occupied_sensor_per_week():
    """
        At what day of the week there are most people parking and least people parking?
    """
    df = pd.concat(map(pd.read_csv, glob.glob(parking_path + "/question1_df" + "/*.csv")))
    st.dataframe(df)
    st.write(
        f'The day of the week where there is the most parking is day #{df["day"].iloc[-1]} with {df["count"].iloc[-1]} '
        f'minutes.')
    st.write(
        f'The day of the week where there is the least parking is day #{df["day"].iloc[0]} with {df["count"].iloc[0]} '
        f'minutes.')


def most_least_time_parked():
    """
    Quel est le montant le plus élevé et le plus bas de l'argent_retire à partir d'un distributeur automatique de billets (DAB)?
        What is the most and least time from a parking sensor?
    """
    df_min = pd.concat(map(pd.read_csv, glob.glob(parking_path + "/question2_df_min" + "/*.csv")))
    df_max = pd.concat(map(pd.read_csv, glob.glob(parking_path + "/question2_df_max" + "/*.csv")))
    st.write(f'The most minutes parked is {df_max["time"].iloc[-1]} minutes.')
    st.write(f'The least minutes parked is {df_min["time"].iloc[-1]} minutes.')


def highest_lowest_argent_retire_per_hour_all_ATMs():
    '''
    À quelle heure la somme d'argent_retire est-elle la plus élevée et la plus basse pour tous les DAB ?
    '''
    df = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question3" + "/*.csv")))
    st.dataframe(df)
    st.write(
        f'L heure à laquelle les sommes d argents sont les plus élevées pour tous les DAB est à {df["hour"].iloc[-1]}h avec {df["sum(argent_retire)"].iloc[-1]}€.')
    st.write(
        f'L heure à laquelle les sommes d argents sont les plus faibles pour tous les DAB est à {df["hour"].iloc[0]}h avec {df["sum(argent_retire)"].iloc[0]}€.')


def highest_lowest_argent_retire_per_hour_per_ATM():
    """
        Throughout the week, what point of the day each parking sensor has most time parked and least time parked?
    """
    df_min = pd.concat(map(pd.read_csv, glob.glob(parking_path + "/question3_df_min" + "/*.csv")))
    df_max = pd.concat(map(pd.read_csv, glob.glob(parking_path + "/question3_df_max" + "/*.csv")))
    with st.container():
        dataframe_max, dataframe_min = st.columns(2)
        with dataframe_max:
            st.table(df_max)
        with dataframe_min:
            st.table(df_min)


def highest_lowest_argent_retire_per_day_all_ATMs():
    '''
    Quel jour la somme d'argent retiré est-elle la plus élevée et le plus basse pour tous les DAB ?")
    '''
    df = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question5" + "/*.csv")))
    st.dataframe(df)
    st.write(
        f'Le jour avec la somme d argent retiré la plus élevée est le {df["day"].iloc[-1]} avec {df["sum(argent_retire)"].iloc[-1]}€.')
    st.write(
        f'Le jour avec la somme d argent retiré la plus basse est le {df["day"].iloc[0]} avec {df["sum(argent_retire)"].iloc[0]}€.')


def highest_lowest_argent_retire_per_day_each_ATM():
    '''
    Quel jour la somme d'argent retiré est-elle la plus élevée et la plus basse pour chaque DAB ?
    '''
    df_min = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question6a" + "/*.csv")))
    df_max = pd.concat(map(pd.read_csv, glob.glob("df_csvs/question6b" + "/*.csv")))

    with st.container():
        dataframe_max, dataframe_min = st.columns(2)
        with dataframe_max:
            st.table(df_max)
        with dataframe_min:
            st.table(df_min)


def main():
    st.subheader("At what day of the week there are most people parking and least people parking?")
    most_least_occupied_sensor_per_week()

    st.subheader("What is the most and least time from a parking sensor?")
    most_least_time_parked()

    st.subheader("Throughout the week, what point of the day each parking sensor has most time parked and least time parked?")
    highest_lowest_argent_retire_per_hour_per_ATM()



if __name__ == '__main__':
    main()
