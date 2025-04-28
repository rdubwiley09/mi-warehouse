import pandas as pd
import streamlit as st


def try_int(x):
    try:
        return int(x)
    except Exception:
        return 0

@st.cache_data()
def get_data():
    df = pd.read_parquet("../../data/mart/mi_elections/election_results_by_district.parquet")
    df['democratic_votes'] = df['democratic_votes'].apply(try_int)
    df['republican_votes'] = df['republican_votes'].apply(try_int)
    df['two_way_votes'] = df['two_way_votes'].apply(try_int)
    sl_df = df.loc[df['office_code_description'].isin(['State Senator', 'State Representative'])]
    return sl_df


@st.cache_data()
def get_senate_margin_adjustment():
    df = pd.read_parquet("../../data/mart/mi_elections/election_results_by_district.parquet")
    df['democratic_votes'] = df['democratic_votes'].apply(try_int)
    df['republican_votes'] = df['republican_votes'].apply(try_int)
    df['two_way_votes'] = df['two_way_votes'].apply(try_int)
    house_df = df.loc[ df['office_code_description'] == 'State Representative' ]
    senate_df = df.loc[ df['office_code_description'] == 'State Senator' ]
    senate_2022_df = senate_df.loc[ senate_df['election_year']== '2022' ]
    house_2024_df = house_df.loc[ house_df['election_year']== '2024' ]
    senate_margin = (senate_2022_df['democratic_votes'].sum()/senate_2022_df['two_way_votes'].sum())
    house_margin = (house_2024_df['democratic_votes'].sum()/house_2024_df['two_way_votes'].sum())
    return house_margin - senate_margin


def calculate_swing(df, adjustment_type, adjustment, swing):
    house_df = df.loc[ df['office_code_description'] == 'State Representative' ]
    senate_df = df.loc[ df['office_code_description'] == 'State Senator' ]
    senate_2022_df = senate_df.loc[ senate_df['election_year']== '2022' ]
    house_2024_df = house_df.loc[ house_df['election_year']== '2024' ]
    if adjustment_type == "Adjust Senate to house 2024 performance":
        adjustment_margin = adjustment
        adjust_body = "senate"
    elif adjustment_type == "Adjust house to senate 2022 performance":
        adjustment_margin = -1*adjustment
        adjust_body = "house"
    else:
        adjustment_margin = 0
        adjust_body = None
    cur_house_grouped = house_2024_df.groupby("winning_party").size()
    current_house_breakout = (cur_house_grouped['Dem'], cur_house_grouped['Rep'])
    cur_senate_grouped = senate_2022_df.groupby("winning_party").size()
    current_senate_breakout = (cur_senate_grouped['Dem'], cur_senate_grouped['Rep'])
    house_calculation_df = house_2024_df.loc[:, [
        'office_code_description',
        'district',
        'winning_first_name',
        'winning_last_name',
        'winning_party',
        'two_way_dem_percent'
    ]]
    if adjust_body == "house":
        house_swing = adjustment_margin+swing
    else:
        house_swing = swing
    house_calculation_df['adjusted_swing'] = house_swing
    house_calculation_df['modeled_two_way_dem_percent'] = house_calculation_df['two_way_dem_percent']+house_swing
    house_calculation_df['modeled_two_way_margin'] = (house_calculation_df['modeled_two_way_dem_percent']-0.5).abs()
    house_calculation_df['modeled_winning_party'] = house_calculation_df['modeled_two_way_dem_percent'].apply(
        lambda x: "Rep" if x<0.5 else "Dem"
    )
    house_swing_df = house_calculation_df.loc[house_calculation_df['winning_party'] != house_calculation_df['modeled_winning_party'] ]
    house_swing_df = house_swing_df.sort_values(['modeled_two_way_margin'])
    modeled_house_groupby = house_calculation_df.groupby(['modeled_winning_party']).size()
    universal_swing_house_breakout = (modeled_house_groupby["Dem"], modeled_house_groupby["Rep"])
    senate_calculation_df = senate_2022_df.loc[:, [
        'office_code_description',
        'district',
        'winning_first_name',
        'winning_last_name',
        'winning_party',
        'two_way_dem_percent'
    ]]
    if adjust_body == "senate":
        senate_swing = adjustment_margin+swing
    else:
        senate_swing = swing
    senate_calculation_df['adjusted_swing'] = senate_swing
    senate_calculation_df['modeled_two_way_dem_percent'] = senate_calculation_df['two_way_dem_percent']+senate_swing
    senate_calculation_df['modeled_two_way_margin'] = (senate_calculation_df['modeled_two_way_dem_percent']-0.5).abs()
    senate_calculation_df['modeled_winning_party'] = senate_calculation_df['modeled_two_way_dem_percent'].apply(
        lambda x: "Rep" if x<0.5 else "Dem"
    )
    senate_swing_df = senate_calculation_df.loc[senate_calculation_df['winning_party'] != senate_calculation_df['modeled_winning_party'] ]
    senate_swing_df = senate_swing_df.sort_values(['modeled_two_way_margin'])
    modeled_senate_groupby = senate_calculation_df.groupby(['modeled_winning_party']).size()
    universal_swing_senate_breakout = (modeled_senate_groupby["Dem"], modeled_senate_groupby["Rep"])
    return {
        "current_house_breakout": current_house_breakout, 
        "current_senate_breakout": current_senate_breakout, 
        "house_calculation_df": house_calculation_df.sort_values("district"), 
        "senate_calculation_df": senate_calculation_df.sort_values("district"),
        "house_swing_df": house_swing_df,
        "senate_swing_df": senate_swing_df,
        "universal_swing_house_breakout": universal_swing_house_breakout,
        "universal_swing_senate_breakout": universal_swing_senate_breakout
    }


def show_calculations(swing_calculations):
    current_house_margin = swing_calculations['current_house_breakout'][0] - swing_calculations['current_house_breakout'][1]
    swing_house_margin = swing_calculations['universal_swing_house_breakout'][0] - swing_calculations['universal_swing_house_breakout'][1]
    current_senate_margin = swing_calculations['current_senate_breakout'][0] - swing_calculations['current_senate_breakout'][1]
    swing_senate_margin = swing_calculations['universal_swing_senate_breakout'][0] - swing_calculations['universal_swing_senate_breakout'][1]
    change_in_house_seats = swing_calculations['universal_swing_house_breakout'][0] - swing_calculations['current_house_breakout'][0]
    change_in_senate_seats = swing_calculations['universal_swing_senate_breakout'][0] - swing_calculations['current_senate_breakout'][0]
    col1, col2 = st.columns([30,30])
    with col1:
        with st.container():
            st.write("House")
        with st.container():
            if current_house_margin == 0:
                swing_house_margin_message = "Split Control"
            elif current_house_margin <0 :
                current_house_margin_message = f"+{-1*current_house_margin}R"
            else:
                current_house_margin_message = f"+{current_house_margin}D"
            st.write("Current House Breakout:")
            st.write(f"{swing_calculations['current_house_breakout'][0]}D - {swing_calculations['current_house_breakout'][1]}R ({current_house_margin_message})")
        with st.container():
            if swing_house_margin == 0:
                swing_house_margin_message = "Split Control"
            elif swing_house_margin <0 :
                swing_house_margin_message = f"+{-1*swing_house_margin}R"
            else:
                swing_house_margin_message = f"+{swing_house_margin}D"
            st.write("Swing House Breakout:")
            st.write(f"{swing_calculations['universal_swing_house_breakout'][0]}D - {swing_calculations['universal_swing_house_breakout'][1]}R ({swing_house_margin_message})")
        with st.container():
            if change_in_house_seats>0:
                st.write(f"Swing Change in Seats: +{change_in_house_seats}D")
            elif change_in_house_seats == 0:
                st.write(f"Swing Change in Seats: No Change")
            else:
                st.write(f"Swing Change in Seats: +{-1*change_in_house_seats}R")
        with st.container():
            st.write("Flipped Districts:")
            st.write(swing_calculations['house_swing_df'])
        with st.container():
            st.write("All Districts:")
            st.write(swing_calculations['house_calculation_df'])
    with col2:
        with st.container():
            st.write("Senate")
        with st.container():
            if current_senate_margin == 0:
                swing_senate_margin_message = "Split Control"
            elif current_senate_margin <0 :
                current_senate_margin_message = f"+{-1*current_senate_margin}R"
            else:
                current_senate_margin_message = f"+{current_senate_margin}D"
            st.write("Current Senate Breakout:")
            st.write(f"{swing_calculations['current_senate_breakout'][0]}D - {swing_calculations['current_senate_breakout'][1]}R ({current_senate_margin_message})")
        with st.container():
            if swing_senate_margin == 0:
                swing_senate_margin_message = "Split Control"
            elif swing_senate_margin <0 :
                swing_senate_margin_message = f"+{-1*swing_senate_margin}R"
            else:
                swing_senate_margin_message = f"+{swing_senate_margin}D"
            st.write("Swing Senate Breakout:")
            st.write(f"{swing_calculations['universal_swing_senate_breakout'][0]}D - {swing_calculations['universal_swing_senate_breakout'][1]}R ({swing_senate_margin_message})")
        with st.container():
            if change_in_senate_seats>0:
                st.write(f"Swing Change in Seats: +{change_in_senate_seats}D")
            elif change_in_senate_seats == 0:
                st.write(f"Swing Change in Seats: No Change")
            else:
                st.write(f"Swing Change in Seats: +{-1*change_in_senate_seats}R")
        with st.container():
            st.write("Flipped districts:")
            st.write(swing_calculations['senate_swing_df'])
        with st.container():
            st.write("All Districts:")
            st.write(swing_calculations['senate_calculation_df'])


st.set_page_config(layout="wide")
df = get_data()
senate_adjustment = get_senate_margin_adjustment()

with st.container():
    with st.form("slider_form"):
        adjustment_type = st.selectbox("How would you like to baseline performance?",
            ("Adjust Senate to house 2024 performance", "Adjust house to senate 2022 performance", "None"),
        )
        swing = st.slider("Universal Swing", -25.0, 25.0, 0.00, 0.25)
        submitted = st.form_submit_button("Calculate")
        if submitted:
            swing = calculate_swing(df, adjustment_type, senate_adjustment, swing/100)
            show_calculations(swing)