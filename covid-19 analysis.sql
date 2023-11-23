#___________________________________________Analysis of COVID-19 Dataset______________________________________________________# 

###############################################LEAD()############################################################################


#1.Lead function to find the differences in total death cases between the countries.

SELECT Country_name, Covid_deaths as 'total death cases',
LEAD (Covid_deaths,1) OVER(ORDER BY Country_name) as Next_country_totaldeathcases
FROM World_data;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#2. For all the data of the covid with country_name = 'US', show the States, the number of confirmed cases and the 
#number of recovery case usins Lead.

SELECT Country_name , State_name , Confirmed_cases,
Lead(Recovered,0) OVER(ORDER BY Confirmed_cases desc) as Recoverd_cases
FROM Country_covid_data
where Country_name = 'US' ;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#3. Show the difference in percentage of states not using maks and also  show  0 instead of NULL if no LEAD value is found.

SELECT Country_name , State_name , Never_used,
Lead(Never_used,1,0) OVER(Partition by Country_name ORDER BY Confirmed_cases ) as Masks_not_used_diff
FROM Country_covid_data
inner join Masks_used using (State_ID) ;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#4. Show country_name,State_name, confirmed case  and next state confirmed case and partion by country 
#(for all when the mask was always used highest)

SELECT Country_name, State_name,Confirmed_cases ,
LEAD(Confirmed_cases,1) OVER(Partition by Country_name ) as Cases_based_on_masks_used
from Country_covid_data
inner join Masks_used using(State_ID) 
order by Always_used desc;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#5. List the states with highest number of covid cases and compare them with the next highest state in world.

SELECT 
    State_Name,
    Confirmed_cases,
    LEAD(Confirmed_cases,1) OVER (
        ORDER BY Confirmed_cases desc ) Next_highest_covidcases_state
FROM 
    Country_covid_data; 

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#6. List the country with highest recovery rate and compare it with the next state with the best recovery rate

SELECT 
    Country_name,
    Covid_recovered as 'Recovered cases',
    LEAD(Covid_recovered,1)OVER (
    ORDER BY Covid_recovered desc) Next_highest_Recovered_case
FROM 
    World_data
;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#7. Comparing the if the highest covid cases are due to masks not used ?

SELECT 
    Country_name,
    State_Name,
    Confirmed_cases,
    Never_used,
    LEAD(Never_used,1)OVER (
        ORDER BY Confirmed_cases desc) Mask_not_used
FROM 
    Country_covid_data
inner join Masks_used using(State_ID)
;


###########################################################Row_Number()##################################################################################

# Q1. What is the Death rate State wise for each country?
select row_number() over(partition by Country_name order by (Confirmed_deaths / Confirmed_cases * 100) desc) as 'Row Number', Country_name, State_Name, 
Confirmed_deaths, (Confirmed_deaths / Confirmed_cases * 100) as 'Death Rate'
from country_covid_data;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#Q2. What is the Recovery rate State wise for each country?
select row_number() over(partition by Country_name order by (Recovered / Confirmed_cases * 100) desc) as 'Row Number', Country_name, State_Name, 
Recovered, (Recovered / Confirmed_cases * 100) as 'Recovery Rate'
from country_covid_data;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Q3. confirmed cases vs always use mask.
select row_number() over(partition by Country_name order by Confirmed_cases desc) as 'Row Number',Country_name,State_Name, Confirmed_cases,
Always_used from country_covid_data ccd
join masks_used mu
on ccd.State_ID = mu.State_ID;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Q4. What is the Death Rate in the world?
select row_number() over(order by (Covid_deaths / Covid_cases * 100) desc) as 'Row Number', Country_name, 
Covid_deaths, (Covid_deaths / Covid_cases * 100) as 'Death Rate'
from world_data;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#Q5. What is the Recovery rate in the world?
select row_number() over(order by (Covid_recovered / Covid_cases * 100) desc) as 'Row Number', Country_name, 
Covid_recovered, (Covid_recovered / Covid_cases * 100) as 'Recovery Rate'
from world_data;

-- overall ranking of the state. 
select row_number() over(order by Confirmed_cases desc) as 'Row Number', State_Name, Confirmed_cases, Confirmed_deaths, Recovered from country_covid_data;
select row_number() over(order by Confirmed_deaths desc) as 'Row Number', State_Name, Confirmed_cases, Confirmed_deaths, Recovered from country_covid_data;
select row_number() over(order by Recovered desc) as 'Row Number', State_Name, Confirmed_cases, Confirmed_deaths, Recovered from country_covid_data;

-- never mask
select row_number() over(partition by Country_name order by Confirmed_deaths desc) as 'Row Number',Country_name,State_Name, Confirmed_deaths,
Never_used from country_covid_data ccd
join masks_used mu
on ccd.State_ID = mu.State_ID;

-- never use mask
select row_number() over(partition by Country_name order by Never_used desc) as 'Row Number',Country_name,State_Name, Confirmed_cases, 
Never_used from country_covid_data ccd
join masks_used mui
on ccd.State_ID = mu.State_ID;

-- overall ranking in world data.
select row_number() over(order by Covid_cases desc) as 'Row Number', Country_name, Covid_cases, Covid_deaths, Covid_recovered from world_data;
select row_number() over(order by Covid_deaths desc) as 'Row Number', Country_name,Covid_cases,Covid_deaths,Covid_recovered from world_data;
select row_number() over(order by Covid_recovered desc) as 'Row Number', Country_name,Covid_cases,Covid_deaths,Covid_recovered from world_data;

####################################################NTH_Value()###############################################################

#Q1> How are the Country-states stacked up in terms of number of cases 

create or replace view country_case_vw as (
select Country_name,State_ID,State_Name,
nth_value(State_Name,1) over(partition by Country_name order by Confirmed_cases desc) as highest_covid_case_state,
nth_value(Confirmed_cases,1) over(partition by Country_name order by Confirmed_cases desc) as highest_covid_cases,

nth_value(State_Name,2) over(partition by Country_name order by Confirmed_cases desc) as 2nd_highest_covid_case_state,
nth_value(Confirmed_cases,2) over(partition by Country_name order by Confirmed_cases desc) as 2nd_highest_covid_cases,

nth_value(State_Name,3) over(partition by Country_name order by Confirmed_cases desc) as 3rd_highest_covid_case_state,
nth_value(Confirmed_cases,3) over(partition by Country_name order by Confirmed_cases desc) as 3rd_highest_covid_cases,

nth_value(State_Name,4) over(partition by Country_name order by Confirmed_cases desc) as 4th_highest_covid_case_state,
nth_value(Confirmed_cases,4) over(partition by Country_name order by Confirmed_cases desc) as 4th_highest_covid_cases,

nth_value(State_Name,5) over(partition by Country_name order by Confirmed_cases desc) as 5th_highest_covid_case_state,
nth_value(Confirmed_cases,5) over(partition by Country_name order by Confirmed_cases desc) as 5th_highest_covid_cases,

nth_value(State_Name,6) over(partition by Country_name order by Confirmed_cases desc) as 6th_highest_covid_case_state,
nth_value(Confirmed_cases,6) over(partition by Country_name order by Confirmed_cases desc) as 6th_highest_covid_cases

from  Country_covid_data);

select * from country_case_vw;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Q2>Quantify How are states in India stacked up in terms of numbe of covid cases.

create or replace view state_case_reduction_vw as( 
select Country_name,State_Name,State_ID,highest_covid_case_state,2nd_highest_covid_case_state,
((highest_covid_cases-2nd_highest_covid_cases) / highest_covid_cases )*100 as percentage_reduction_from_1st,

3rd_highest_covid_case_state,
((2nd_highest_covid_cases-3rd_highest_covid_cases) / 2nd_highest_covid_cases )*100 as percentage_reduction_from_2nd,

4th_highest_covid_case_state,
((3rd_highest_covid_cases-4th_highest_covid_cases) / 3rd_highest_covid_cases )*100 as percentage_reduction_from_3rd,

5th_highest_covid_case_state,
((4th_highest_covid_cases-5th_highest_covid_cases) / 4th_highest_covid_cases )*100 as percentage_reduction_from_4rd,

6th_highest_covid_case_state,
((5th_highest_covid_cases-6th_highest_covid_cases) / 5th_highest_covid_cases )*100 as percentage_reduction_from_5th

from country_case_vw);

#india
 select * from state_case_reduction_vw where Country_name='India';
 
  -- select * from state_case_reduction_vw where Country_name='Brazil';
 
 #UK
 select * from state_case_reduction_vw where Country_name='UK';
 
 -- select * from state_case_reduction_vw where Country_name='US';
 
#Conclusion:: a>Karnataka with 2nd highest cases among indian states lags by 52% behind maharashtra(highest in cases) 
#               This huge gap indecated very high spread of covid in maharashtra compared to other states
 
#              b>On the other end Northern Ireland of UK has lowest cases of covid and lags over 94% behind 3rd placed scotland.
#                This shows Northern Ireland's covid rules being very effective in controlling community transmission 
 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 
 #Q3> What is the mask use relation with covid cases in states in India
 
 -- select * from masks_used;
 
 select ccv.Country_name,ccv.State_ID,ccv.State_Name,ccv.highest_covid_case_state,mu.Always_used/mu.Never_used as 'ratio of Always_Masks_use to Never Mask use'  
 from country_case_vw ccv inner join masks_used mu
 on ccv.State_ID=mu.State_ID 
 where ccv.Country_name='India';
 
#CONCLUSION:a> Maharashrta has the lowest Always-mask-use to Never-mask-use ratio of 1.76 which corresponds to highest cases among indian states
#              while delhi has highest Always-mask-use to Never-mask-use ratio of 10.29 corresponding to lowest case among indian states
#		     b> Mask use is inversely related to covid cases and a huge factor for restricting the spread of covid  
 
 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 #Q4> What is the Death rate and  PERCENTAGE HIKES in Death rate in the countries of the world
 #    compared to the country with LEAST death rate

--  creating a view on death rate
create or replace view death_rate_vw as (
select dense_rank() over(order by (Covid_deaths / Covid_cases * 100) desc) as Ranking_of_death_rate,
Country_name,Covid_cases,Covid_deaths,
(Covid_deaths / Covid_cases * 100) as 'Death_Rate_of_Countries' 
from world_data);

#death rate view
select * from death_rate_vw;

#percentage hike in death rates per countries w.r.t country having least death rate
select *,
nth_value(Death_Rate_of_Countries,1) over (order by Death_Rate_of_Countries asc) as least_Death_country,
(Death_Rate_of_Countries - nth_value(Death_Rate_of_Countries,1) 
over (order by Death_Rate_of_Countries asc)) / nth_value(Death_Rate_of_Countries,1) 
over (order by Death_Rate_of_Countries asc) *100 as percentage_hike_in_Death_rate
from death_rate_vw;

#CONCLUSION: a> Mexico has highest death rate while Turkey has the lowest by a margin of 809%
#            b> India has Third lowest death rate hike at 46% highter death compared to turkey(lowest) despite having 2nd highest number of cases
 
 
 ##############################################First_Value()########################################################################


# Q1.Country that is most affected based on covid cases ,deaths, recovery.
select *,
first_value(country_name) over(order by covid_cases desc) highest_covid_cases_country,
first_value(country_name) over(order by covid_deaths desc) highest_covid_deaths_country,
first_value(country_name) over(order by covid_recovered desc) highest_covid_recovered_country
from world_data;
-- US has highest covid cases and covid deaths.Therefore US is most affected by covid.
-- India has the highest recovery
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 #Q2. Country that is least affected based on covid cases, deaths, recovery.
select *,
first_value(country_name) over( order by covid_cases ) lowest_confirmed_cases,
first_value(country_name) over( order by covid_deaths ) lowest_confirmed_deaths,
first_value(country_name) over(order by covid_recovered ) lowest_confirmed_recovery
from world_data;
-- czechia has the lowest confirmed cases
-- Netherlanda Arabia has lowest covid deaths and lowest recovery.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#Q3.State most affected in US  vs the mask usage.
select cd.Country_name,cd.state_name, cd.confirmed_cases, -- cd.Confirmed_deaths, cd.Recovered,
mu.never_used, mu.sometimes_used , mu.Always_used,
first_value(cd.State_Name) over(partition by cd.country_name order by cd.confirmed_cases desc) state_affected_usa
from country_covid_data cd inner join masks_used mu
on cd.State_ID= mu.State_ID
where cd.Country_name="US"
#limit 1
;
-- Califorina state is most affected in US and mask used always is less compared to other states

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  -- -- -- -- Analyses on Mask Used in India States -- -- -- --
 
 
#Q1.State which used "mask always" in India .

select cd.Country_name,cd.state_id, cd.state_name, cd.confirmed_cases, mu.always_used,
first_value(cd.state_name) over(partition by country_name order by  cd.Confirmed_cases) least_cases_state_india,
first_value(cd.state_name) over(partition by country_name order by mu.always_used desc) mask_always_used
from country_covid_data cd inner join masks_used mu
on cd.State_ID= mu.State_ID
where country_name='India';
-- Least cases in Delhi state as they used the mask always.

#Q2.State which never used "mask never" India .

select cd.state_id, cd.state_name, cd.confirmed_cases, mu.never_used,
first_value(cd.state_name) over(partition by country_name order by  cd.Confirmed_cases desc) most_cases_state_india,
first_value(cd.state_name) over(partition by country_name order by mu.never_used desc) mask_never_used
from country_covid_data cd inner join masks_used mu
on cd.State_ID= mu.State_ID
where country_name='India';

-- Most cases are in Maharashtra state as they used never used mask.

#Q3.State which used "mask Sometimes " in India.

select cd.state_id, cd.state_name, cd.confirmed_cases, mu.Rarley_used,
dense_rank() over(partition by country_name order by  cd.Confirmed_cases) avgerage_cases_state_india,
first_value(cd.state_name) over(partition by cd.country_name order by mu.Sometimes_used desc) mask_Sometimes_used
from country_covid_data cd inner join masks_used mu
on cd.State_ID= mu.State_ID
where country_name='India';
-- Karanataka state used the  sometimes  used mask and Karanataka in ranked 5th out of the other states.

##############################################----THANK  YOU----##########################################################################################


