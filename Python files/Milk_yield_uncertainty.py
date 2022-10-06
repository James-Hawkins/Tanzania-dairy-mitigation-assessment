import sys
import os

sys.path.append('../../../..')
from livsim import Cow
from livsim import FeedStorage
from livsim import Feed

import pandas as pd
from pathlib import Path
import xlrd 
import math
import numpy as np


cwd = os.getcwd()
cwd




pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)




def yield_var(   res,
                 data_wb,
                 scenario_parameters,
                 b,
                 cohort,
                 lps,
                 reg,
                 subsectors,
                 l,
                 r,
                 s,
                 herd,
                 herd_y0,
                 herd_baseline_yf,
                 check_feasibility,
                 timestep,
                 MC_sims,
                 #MC_sims_my,
                 reg_analysis,
                 ss_analysis,
                 consequential):
    


        # Load data sheets from excel
        uncertainty_sheet= data_wb.sheet_by_index(2)
        yield_sheet=data_wb.sheet_by_index(5)
        ryld_data=data_wb.sheet_by_index(6)


        #feed_util_effcy=data_wb.sheet_by_index(8)
        my_uncertainty_feed = data_wb.sheet_by_index(10)
        my_uncertainty_breed = data_wb.sheet_by_index(11)

        # work sheets loaded per region
        if ( reg_analysis == 0 ) :

            if (l == 'MRT'):
                sheet = data_wb.sheet_by_index(3)
                diet_data=(cwd+str('\\diet_MRT.xls'))
            elif (l == 'MRH'):

                sheet = data_wb.sheet_by_index(4) 
                diet_data=(cwd+str('\\diet_MRH.xls'))


            diets=xlrd.open_workbook(diet_data) 
            diet_wb = xlrd.open_workbook(diet_data)

        elif ( reg_analysis == 1 ) :


            reg_data=Path(cwd+str('/regional data files/'))
            regional_herds=reg_data/'regional_herd_populationss.xls'
            regional_LUC_coeffs=reg_data/'regional_LUC_coeffss.xls'
            reg_herd=xlrd.open_workbook(regional_herds)


            if ( r == 'NB'):
                if (l == 'MRT'):
                    diet_data =reg_data/'diet_MRT_NB.xls'

                elif (l == 'MRH'):
                    diet_data =reg_data/'diet_MRH_NB.xls'


            elif (r == 'MF'):
                if (l == 'MRT'):
                    diet_data =reg_data/'diet_MRT_MF.xls'

                elif (l == 'MRH'):
                    diet_data =reg_data/'diet_MRH_MF.xls'

            elif (r == 'MM'):
                if (l == 'MRT'):

                    diet_data =reg_data/'diet_MRT_MM.xls'

                elif (l == 'MRH'):

                    diet_data =reg_data/'diet_MRH_MM.xls'

            elif (r == 'RW'):
                if (l == 'MRT'):

                    diet_data =reg_data/'diet_MRT_RW.xls'

                elif (l == 'MRH'):

                    diet_data =reg_data/'diet_MRH_RW.xls'


            diet_wb = xlrd.open_workbook(diet_data)
            sheet = diet_wb.sheet_by_index(7)


        # Main results from simulations are initially stored in dictionaries
        # feed intake
        grass_intake={}
        stover_intake={}
        maize_bran_intake={}
        sfsm_intake={}
        napier_intake={}
        napier_hy_intake={}
        pasture_intake={}

        utn_efy={}
        ensilage_fodder_frac={}
        
       
        out={}
       # count_calendar_month={}
        
        # Production variables (milk and meat)
        # milk yield
       # milk_yield_reg = {} 
       # milk_yield_lact = {}
        se_milk ={}
        
        
        # land footprint 
        feed_area_base={}
        feed_primary_area_base={}
        feed_area={}
        feed_primary_area={}

        total_grasslands_area={}

        crop_primary_area={}
        crop_primary_area_base={}

        pasture_area={}
        pasture_area_base={}

        grass_area={}
        grass_area_base={}

        stover_area={}
        stover_area_base={}

        napier_area={}
        napier_area_base={}

        napier_hy_area={}
        napier_hy_area_base={}

        maize_as_bran_area={}
        maize_as_bran_area_base={}

        sunflower_as_meal_area={}
        sunflower_as_meal_area_base={}

        

        feed_util_effcy=data_wb.sheet_by_index(8)
        # Use efficiencies for individdual feeds
        utn_efy[('grass')]=feed_util_effcy.cell_value(1,1)
        utn_efy[('pasture')]=feed_util_effcy.cell_value(2,1)
        utn_efy[('napier')]=feed_util_effcy.cell_value(3,1)
        utn_efy[('napier_hy')]=feed_util_effcy.cell_value(4,1)
        utn_efy[('maize_bran')]=feed_util_effcy.cell_value(5,1)
        utn_efy[('sunflower_seed_meal')]=feed_util_effcy.cell_value(6,1)
        utn_efy[('stover')]=feed_util_effcy.cell_value(7,1)


        
        
    # Initiate simulations

        for ss in subsectors:   
            
            #for b in breed:

            if (herd[(ss,b,'total_herd')] == 0): # if there are no animals of a given breed, continue to next step
                continue

            for c in cohort:

                if (b == 'local'):

                    bc_count=0
                    start_age=28
                    max_simu_range = 12*13-20

                    d_MC = Cow( age = start_age , breed='Zebu -- TZ south high')
                    d_MC.sex='f'


                else:

                    bc_count=5
                    start_age=20
                    max_simu_range=12*13-20

                    d_MC = Cow(age=start_age,breed='Improved Tanz southern highlands') 
                    d_MC.sex='f'

                # Set basic LivSim parameters
                d_MC._main_variables = None 
                d_MC._DETERMINISTIC= True
                d_MC.is_bull_present = 1
                d_MC._MAX_ITER = 20



                #~~~~~~~~~~~~~~~~~~~~~~~~ FEEDING SCRIPT ~~~~~~~~~~~~~~~~~~~~~~~

                # define feeds used in simulations from library
                feeds=['grass','stover','napier','napier_hy','maize_bran','sunflower_seed_meal','pasture']
                concentrates=['maize_bran','sunflower_seed_meal']

                GRASS = Feed(parameters='Native grass') 
                PASTURE = Feed(parameters='Pasture')
                NAPIER = Feed(parameters='napier grass') 
                NAPIER_HY = Feed(parameters='napier-legume') 
                NAPIER_SILAGE = Feed(parameters='napier silage') 
                SUNFLOWER_SEED_MEAL = Feed(parameters='sunflower seed meal')
                MAIZE_BRAN = Feed(parameters='maize bran')


                ## specify the diets for cows in line with the scenario
                feed_cow_base=diet_wb.sheet_by_index(0)

                if (b=='local'):
                    feed_cow_scen = diet_wb.sheet_by_index(int(scenario_parameters[(ss,'diet_id_cow_local',s)]))

                elif (b=='improved'):
                    feed_cow_scen=  diet_wb.sheet_by_index(int(scenario_parameters[(ss,'diet_id_cow_improved',s)]))

                # load stover nutrient properties 
                if ((c == 'cow') and  (scenario_parameters[(ss,'urea_treatment',s)]==1)):
                    STOVER=Feed(parameters='maize stover urea treated')
                    m_stover="maize stover urea treated"

                else:
                    STOVER=Feed(parameters='maize stover')
                    m_stover="maize stover"


                # define feed storage object
                feed_storage = FeedStorage()


                # cow specific feed intake variables
                # feed intake per day (kg dm/head/day) at specific stage of cow's production cycle (lactating, gestating, other)
                feed_intake_cow_early_lac={}
                feed_intake_cow_late_lac={}
                feed_intake_cow_gest={}
                feed_intake_cow_other={}
                feed_intake_cow_early_lac_base={}
                feed_intake_cow_late_lac_base={}
                feed_intake_cow_gest_base={}
                feed_intake_cow_other_base={}

                feed_offer_cow={}
                feed_offer_cow_base={}

                fd_offer_cycle={}
                fd_offer_cycle_base={}

                # feed intake per year from specific production stages of cow (kg DM/head/yr)
                cons_per_year_early_lac_base={}
                cons_per_year_late_lac_base={}
                cons_per_year_lac_base={}
                cons_per_year_oth_base={}
                cons_per_year_early_lac={}
                cons_per_year_late_lac={}
                cons_per_year_lac={}
                cons_per_year_oth={}

                # all feed intake variables
                feed_intake={} # feed intake on an annual basis per breed/cohort (kg dm/yr/head)
                feed_intake_base={} # reference feed intake on an annual basis per breed/cohort (kg dm/yr/head)

                cycle = ['e_lac','l_lac','other']

                # actual feed intake for this iteration
                # feed for cows
                if (c =='cow'):

                    lactation_duration = 30*d_MC.parameters['lactation_duration']['value']
                    early_lactation_duration = 30*5
                    late_lactation_duration = lactation_duration-early_lactation_duration
                    calving_interval = 365/d_MC.parameters['potential_annual_calving_rate']['value']
                    FRAC_time_lactation = lactation_duration/calving_interval
                    FRAC_time_early_lactation = early_lactation_duration/calving_interval
                    FRAC_time_late_lactation = late_lactation_duration/calving_interval
                    days_not_in_lac = ( 1 - FRAC_time_lactation)*365

                    # load diets from excel
                    if (b=='local'):

                        breed_set = 0

                    elif (b=='improved'):

                        breed_set = 1

                    for f in feeds:
                            row = (subsectors.index(ss)*3)+breed_set*(9)

                            # new
                            feed_offer_cow_base[ss,b,c,'e_lac',f]=feed_cow_base.cell(row+1,3+feeds.index(f)).value/1000
                            feed_offer_cow_base[ss,b,c,'l_lac',f]=feed_cow_base.cell(row+2,3+feeds.index(f)).value/1000
                            feed_offer_cow_base[ss,b,c,'other',f]=feed_cow_base.cell(row+3,3+feeds.index(f)).value/1000


                    # new

                    fd_offer_cycle_base[ss,b,c,'e_lac','sunflower_seed_meal']=feed_offer_cow_base[ss,b,c,'e_lac','sunflower_seed_meal']*early_lactation_duration*FRAC_time_early_lactation
                    fd_offer_cycle_base[ss,b,c,'l_lac','sunflower_seed_meal']=feed_offer_cow_base[ss,b,c,'l_lac','sunflower_seed_meal']*late_lactation_duration*FRAC_time_late_lactation
                    fd_offer_cycle_base[ss,b,c,'other','sunflower_seed_meal']=feed_offer_cow_base[ss,b,c,'other','sunflower_seed_meal']*days_not_in_lac

                    fd_offer_cycle_base[ss,b,c,'e_lac','maize_bran']=feed_offer_cow_base[ss,b,c,'e_lac','maize_bran']*early_lactation_duration*FRAC_time_early_lactation
                    fd_offer_cycle_base[ss,b,c,'l_lac','maize_bran']=feed_offer_cow_base[ss,b,c,'l_lac','maize_bran']*late_lactation_duration*FRAC_time_late_lactation
                    fd_offer_cycle_base[ss,b,c,'other','maize_bran']=feed_offer_cow_base[ss,b,c,'other','maize_bran']*days_not_in_lac

                    fd_offer_cycle_base[ss,b,c,'e_lac','napier']= (
                        feed_offer_cow_base[ss,b,c,'e_lac','napier']*
                        early_lactation_duration*FRAC_time_early_lactation)

                    fd_offer_cycle_base[ss,b,c,'l_lac','napier']=(
                        feed_offer_cow_base[ss,b,c,'l_lac','napier']*
                        late_lactation_duration*FRAC_time_late_lactation)

                    fd_offer_cycle_base[ss,b,c,'other','napier']=feed_offer_cow_base[ss,b,c,'other','napier']*days_not_in_lac

                    fd_offer_cycle_base[ss,b,c,'e_lac','napier_hy'] = (feed_offer_cow_base[ss,b,c,'e_lac','napier_hy']*
                                                                       early_lactation_duration*FRAC_time_early_lactation)

                    fd_offer_cycle_base[ss,b,c,'l_lac','napier_hy'] = (feed_offer_cow_base[ss,b,c,'l_lac','napier_hy']*
                                                                       late_lactation_duration*FRAC_time_late_lactation)
                    fd_offer_cycle_base[ss,b,c,'other','napier_hy']=feed_offer_cow_base[ss,b,c,'other','napier_hy']*days_not_in_lac

                    fd_offer_cycle_base[ss,b,c,'e_lac','grass']=(
                     feed_offer_cow_base[ss,b,c,'e_lac','grass']*early_lactation_duration*FRAC_time_early_lactation)

                    fd_offer_cycle_base[ss,b,c,'l_lac','grass']=( 
                        feed_offer_cow_base[ss,b,c,'l_lac','grass']*
                        late_lactation_duration*FRAC_time_late_lactation)

                    fd_offer_cycle_base[ss,b,c,'other','grass']=(
                    feed_offer_cow_base[ss,b,c,'other','grass']*days_not_in_lac)

                    fd_offer_cycle_base[ss,b,c,'e_lac','stover'] = ( 
                        feed_offer_cow_base[ss,b,c,'e_lac','stover']*
                        early_lactation_duration*FRAC_time_early_lactation)

                    fd_offer_cycle_base[ss,b,c,'l_lac','stover'] = (
                    feed_offer_cow_base[ss,b,c,'l_lac','stover']*
                        late_lactation_duration*FRAC_time_late_lactation)

                    fd_offer_cycle_base[ss,b,c,'other','stover'] = ( 
                        feed_offer_cow_base[ss,b,c,'other','stover']*days_not_in_lac)

                    fd_offer_cycle_base[ss,b,c,'e_lac','pasture'] = (
                        feed_offer_cow_base[ss,b,c,'e_lac','pasture']*
                                                                     early_lactation_duration*FRAC_time_early_lactation)

                    fd_offer_cycle_base[ss,b,c,'l_lac','pasture']=(
                        feed_offer_cow_base[ss,b,c,'l_lac','pasture']*
                                                                   late_lactation_duration*FRAC_time_late_lactation)

                    fd_offer_cycle_base[ss,b,c,'other','pasture']=feed_offer_cow_base[ss,b,c,'other','pasture']*days_not_in_lac



                    # the annual feed intake of a feed category is equal to the sum over all production stages
                    for f in feeds:
                        for cc in cycle:
                            feed_intake_base[ss,b,c,f] = 0

                    for f in feeds:
                        for cc in cycle:
                            feed_intake_base[ss,b,c,f] += fd_offer_cycle_base[ss,b,c,cc,f]




                    # if the current scenario is a feeding scenario, diet is different from base
                    if (scenario_parameters[(ss,'diet_scenario_boolean',s)] == 1):   

                        # load diets from excel
                        for f in feeds:
                            row = (subsectors.index(ss)*3)+breed_set*9

                            feed_offer_cow[ss,b,c,'e_lac',f]=feed_cow_scen.cell(row+1,3+feeds.index(f)).value/1000
                            feed_offer_cow[ss,b,c,'l_lac',f]=feed_cow_scen.cell(row+2,3+feeds.index(f)).value/1000
                            feed_offer_cow[ss,b,c,'other',f]=feed_cow_scen.cell(row+3,3+feeds.index(f)).value/1000


                            scenario_parameters[(ss,'ensilage_fodder_frac',s)] = float(feed_cow_scen.cell(row+1,21).value) 


                    else: ## if base scenario 

                        row = (subsectors.index(ss)*3)+breed_set*9
                        scenario_parameters[(ss,'ensilage_fodder_frac',s)] = float(feed_cow_scen.cell(row+1,21).value) 

                        for f in feeds:
                            for cc in cycle:
                                feed_offer_cow[ss,b,c,cc,f] = feed_offer_cow_base[ss,b,c,cc,f]


                    ## set annual consumption based on fraction of time in lactation

                    for f in feeds:

                        fd_offer_cycle[ss,b,c,'e_lac',f] = (feed_offer_cow[ss,b,c,'e_lac',f]*
                                                            early_lactation_duration*FRAC_time_early_lactation)
                        fd_offer_cycle[ss,b,c,'l_lac',f] = (feed_offer_cow[ss,b,c,'l_lac',f]*
                                                            late_lactation_duration*FRAC_time_late_lactation)
                        fd_offer_cycle[ss,b,c,'other',f] = feed_offer_cow[ss,b,c,'other',f]*days_not_in_lac


                    ## sum intake over production stages to get the intake on an annual basis
                    for f in feeds:
                        feed_intake[ss,b,c,f] = 0

                    for f in feeds:
                        for cc in cycle:
                             feed_intake[ss,b,c,f] += fd_offer_cycle[ss,b,c,cc,f]


                '''

                Define LAND FOOTPRINT

                This section takes the feed intake for the current animal as an annual value and calculates area of land 
                needed to produce this feed. This is done for both the reference (base) scenario and the current iteration,
                which is needed in order to calculate LUC emissions between scenarios 

                It is important to understand that in this framework, the concentrate feeds are fed according to the 
                production cycles of the cow (lactating, gestating, etc.) whereas the other feeds are specifed as annual intakes,
                and then the intake per month depends on the seasonality parameters (e.g the variation in yields of feeds, and 
                rationing parameters, etc.)

                ''' 

                # reference yield (Mg DM/ha/yr) (corrected for efficiency of harvest/grazing)
                grass_annual_yield = (utn_efy[('grass')]) * yield_sheet.cell_value(1, 2)
                pasture_annual_yield = (utn_efy[('pasture')]) * yield_sheet.cell_value(2, 2)
                napier_annual_yield = (utn_efy[('napier')]) * yield_sheet.cell_value(3, 2)
                napier_hy_annual_yield = (utn_efy[('napier_hy')]) * yield_sheet.cell_value(4, 2)
                stover_annual_yield = (utn_efy[('stover')]) * yield_sheet.cell_value(5, 2)
                maize_annual_yield = ((utn_efy[('maize_bran')]) * yield_sheet.cell_value(6, 2) *(scenario_parameters[(ss,'yield_scale_factor_maize','BAU')]**timestep))
                sunflower_annual_yield = (utn_efy[('sunflower_seed_meal')] * yield_sheet.cell_value(7, 2)*(scenario_parameters[(ss,'yield_scale_factor_sunflower','BAU')]**timestep))



                # reference area (ha)
                grass_area_base[ss,b,c]=(1/1000)*feed_intake_base[ss,b,c,'grass']/grass_annual_yield
                pasture_area_base[ss,b,c]=(1/1000)*feed_intake_base[ss,b,c,'pasture']/pasture_annual_yield
                stover_area_base[ss,b,c]=(1/1000)*(feed_intake_base[ss,b,c,'stover'])/stover_annual_yield
                sunflower_as_meal_area_base[ss,b,c]=(1/1000)*feed_intake_base[ss,b,c,'sunflower_seed_meal']/((scenario_parameters[(ss,'yield_scale_factor_sunflower','BAU')]**timestep)*
                                                                                                             sunflower_annual_yield)
                maize_as_bran_area_base[ss,b,c]=(1/1000)*feed_intake_base[ss,b,c,'maize_bran']/(scenario_parameters[(ss,'yield_scale_factor_maize','BAU')]*maize_annual_yield)
                napier_area_base[ss,b,c]=(1/1000)*feed_intake_base[ss,b,c,'napier']/napier_annual_yield       
                napier_hy_area_base[ss,b,c]=(1/1000)*feed_intake_base[ss,b,c,'napier_hy']/napier_annual_yield       

                # summarize area for total feed, primary feeds, and primarcy crops
                feed_area_base[(ss,b,c)]=pasture_area_base[ss,b,c]+grass_area_base[ss,b,c]+stover_area_base[ss,b,c]+sunflower_as_meal_area_base[ss,b,c]+maize_as_bran_area_base[ss,b,c]+napier_area_base[ss,b,c]+napier_hy_area_base[ss,b,c]
                feed_primary_area_base[(ss,b,c)]= pasture_area_base[ss,b,c]+grass_area_base[ss,b,c]+sunflower_as_meal_area_base[ss,b,c]+maize_as_bran_area_base[ss,b,c]+napier_area_base[ss,b,c]+napier_hy_area_base[ss,b,c]
                crop_primary_area_base[(ss,b,c)]=sunflower_as_meal_area_base[ss,b,c]+maize_as_bran_area_base[ss,b,c]

                # actual area for this simulation (ha)
                pasture_area[ss,b,c]=(1/1000)*feed_intake[ss,b,c,'pasture']/(pasture_annual_yield)
                grass_area[ss,b,c]=(1/1000)*feed_intake[ss,b,c,'grass']/(grass_annual_yield)
                stover_area[ss,b,c]=(1/1000)*(feed_intake[ss,b,c,'stover'])/(stover_annual_yield)                

                napier_area[ss,b,c]=(1/1000)*feed_intake[ss,b,c,'napier']/(napier_annual_yield)            
                napier_hy_area[ss,b,c]=(1/1000)*feed_intake[ss,b,c,'napier_hy']/(napier_hy_annual_yield) 
                sunflower_as_meal_area[ss,b,c]=(1/1000)*feed_intake[ss,b,c,'sunflower_seed_meal']/(sunflower_annual_yield*(scenario_parameters[(ss,'yield_scale_factor_sunflower','BAU')]**timestep))
                maize_as_bran_area[ss,b,c]=(1/1000)*feed_intake[ss,b,c,'maize_bran']/(maize_annual_yield*(scenario_parameters[(ss,'yield_scale_factor_maize','BAU')]**timestep))


                # summarize area for total feed, primary feeds, and primary crops
                feed_area[(ss,b,c)]=grass_area[ss,b,c]+stover_area[ss,b,c]+sunflower_as_meal_area[ss,b,c]+maize_as_bran_area[ss,b,c]+napier_area[ss,b,c]+pasture_area[ss,b,c]
                feed_primary_area[(ss,b,c)]=grass_area[ss,b,c]+sunflower_as_meal_area[ss,b,c]+maize_as_bran_area[ss,b,c]+napier_area[ss,b,c]+pasture_area[ss,b,c]

                crop_primary_area[(ss,b,c)]=sunflower_as_meal_area[ss,b,c]+maize_as_bran_area[ss,b,c]
                total_grasslands_area[(ss,b,c)]=grass_area[ss,b,c]+napier_area[ss,b,c]+pasture_area[ss,b,c]+napier_hy_area[ss,b,c]


                # Define feed available for feeding based on yield and area (defined as 'production') (kg DM/animal/d for every month)

                pasture_production=[]## production available for feeding (kg DM/head/day)
                napier_production=[] ## production available for feeding (kg DM/head/day)
                napier_hy_production=[] ## production available for feeding (kg DM/head/day)
                napier_silage_production=[] ## production available for feeding (kg DM/head/day)
                grass_production=[]  ## production available for feeding (kg DM/head/day)
                stover_production=[] ## production available for feeding (kg DM/head/day)
                maize_bran_production=[] ## production available for feeding (kg DM/head/day)
                sunflower_seed_meal_production=[] ## production available for feeding (kg DM/head/day)


                for i in range(0,12):

                    napier_production.append(0)
                    napier_hy_production.append(0)
                    napier_silage_production.append(0)
                    grass_production.append(0)
                    stover_production.append(0)
                    pasture_production.append(0)


                # for maize bran and sunflower cake there is no seasonal variation and therefore 
                # monthly consumption is  annual production divided by 12
                sunflower_yld=sunflower_annual_yield*1000/365
                maize_bran_yld=maize_annual_yield*1000/365

                maize_bran_production=maize_as_bran_area[ss,b,c]*maize_bran_yld
                sunflower_seed_meal_production=sunflower_as_meal_area[ss,b,c]*sunflower_yld


                # Feed seasonality
                # define feed available in each month (for stover, pasture, and fodder crops) based on seasonality parameters


                # specify months pertaining to different seasons 
                # the first 6 months of the year are set as the dry season, the last 6 are the rainy season
                am = [0,1,2,3,4,5,6,7,8,9,10,11]  # all months
                ld = [0,1,2,3,4,5]  # long dry season
                lr = [6,7,8,9,10,11] # long rainy season
                sr = [0,1,2,3,4,5]  # months in which stover is rationed (after harvest)

                # Load relative yield data from excel; these parameters are described in the SM of articles
                wsgn = ryld_data.cell_value(1,1) 
                dsdn = ryld_data.cell_value(2,1) 
                wsg_p= ryld_data.cell_value(1,2) 
                dsd_p= ryld_data.cell_value(2,2) 

                # define the fraction of fodder from wet season that is ensiled for dry season feeding (only for cows)   
                if (c == 'cow'):
                    ensilage_fodder_frac[(ss,b)] = scenario_parameters[(ss,'ensilage_fodder_frac',s)]

                elif (c != 'cow'):
                    ensilage_fodder_frac[(ss,b)] = 0


                for i in lr:
                    napier_production[i] = (wsgn*(1/12)*
                    (1-ensilage_fodder_frac[(ss,b)])*napier_area[ss,b,c]*1000*napier_annual_yield/(30)
                                           )

                    grass_production[i]=wsg_p*(1/30)*grass_area[ss,b,c]*1000*grass_annual_yield/12
                    pasture_production[i]=wsg_p*(1/30)*pasture_area[ss,b,c]*1000*pasture_annual_yield/12
                    napier_silage_production[i]=0
                    stover_production[i]=(0.25)*(1/6)*stover_area[ss,b,c]*1000*stover_annual_yield/30

                for i in ld:
                    napier_production[i] = dsdn*(1/12)*napier_area[ss,b,c]*1000*napier_annual_yield/(30)
                    napier_hy_production[i] = dsdn*(1/12)*napier_hy_area[ss,b,c]*1000*napier_hy_annual_yield/(30)
                    grass_production[i] = dsd_p*(1/30)*grass_area[ss,b,c]*1000*grass_annual_yield/12
                    pasture_production[i] = (dsd_p*(1/30)*pasture_area[ss,b,c]*1000*pasture_annual_yield/12)
                    napier_silage_production[i] = (
                        (1/6)*(ensilage_fodder_frac[(ss,b)])*
                    (wsgn*(1/2)*napier_area[ss,b,c]*1000*napier_annual_yield/(30)))

                # stover is rationed equally over 6 months of the year (immediately following crop harvest)    
                for i in sr:
                    stover_production[i]=(0.75)*(1/6)*stover_area[ss,b,c]*1000*stover_annual_yield/30

                ## Specify actual diets for the current animal
                if (c =='cow'):

                    # specify feed intakes for cows
                    # for concentrates feeds are specified based on production cycle
                    # for non-concentrates, feeds are specified by month based on seasonality of these feeds as defined above

                    feed_storage.add_external_roughage(grass_production,'Native grass', 'default')
                    feed_storage.add_external_roughage(pasture_production ,'Pasture', 'default')
                    feed_storage.add_external_roughage(stover_production, m_stover, 'default')
                    feed_storage.add_external_roughage(napier_production, 'napier grass', 'default')
                    feed_storage.add_external_roughage(napier_silage_production, 'napier silage', 'default')
                    feed_storage.add_external_roughage(napier_hy_production, 'napier-legume', 'default')



                    # for concentrates:
                    feed_storage.add_external_concentrate(feed_offer_cow[ss,b,c,'e_lac','sunflower_seed_meal'],'sunflower seed meal', 'early lactation')
                    feed_storage.add_external_concentrate(feed_offer_cow[ss,b,c,'e_lac','maize_bran'],'maize bran', 'early lactation')

                    feed_storage.add_external_concentrate(feed_offer_cow[ss,b,c,'l_lac','sunflower_seed_meal'],'sunflower seed meal', 'lactating')
                    feed_storage.add_external_concentrate(feed_offer_cow[ss,b,c,'l_lac','maize_bran'],'maize bran', 'lactating')

                    feed_storage.add_external_concentrate(feed_offer_cow[ss,b,c,'other','sunflower_seed_meal'],'sunflower seed meal', 'default')
                    feed_storage.add_external_concentrate(feed_offer_cow[ss,b,c,'other','maize_bran'],'maize bran', 'default')


                d_MC.feed_supply = feed_storage

                #  Rin LivSim simulation for a given breed b, cohort c

                print('Breed is ',b)
                print('Animal is ',c)

                # ~~~ Milk yield uncertainty, Monte carlo sims
                rfvars=0
                rbvars=0

                random_fvar={}
                random_bvar={}

                num_fvars = (my_uncertainty_feed.nrows-1)
                num_bvars = (my_uncertainty_breed.nrows-1)

                MC_my = 12

                for MC in range(0,MC_my):
                    for i in range(0,num_fvars):
                        random_fvar[MC,i]=np.random.normal(100,my_uncertainty_feed.cell_value(i+1, 2))/100

                for MC in range(0,MC_my):
                    for i in range(0,num_bvars):
                        random_bvar[MC,i]=np.random.normal(100,my_uncertainty_breed.cell_value(i+1, 2))/100



                # MC milk yield
                lifetime_milk_mc = {}
                annualized_milk_mc = {}

                MC_my_bool = 1

                if ((c == 'cow') & (MC_my_bool == 1)):

                    for i in range(0,MC_my):

                        if (b == 'local'):


                            # Define random breed parameters
                            # Un subscripted parameters
                            d_MC.parameters['gestation_duration']['value'] = random_bvar[i,2]*(
                            d_MC.parameters['gestation_duration']['value'])


                            d_MC.parameters['conception_after_calving']['value'] = random_bvar[i,3]*(
                            d_MC.parameters['conception_after_calving']['value'])


                            d_MC.parameters['lactation_duration']['value'] = random_bvar[i,4]*(
                            d_MC.parameters['lactation_duration']['value'])

                        # milk yield potential
                            for t in range(0,len(d_MC.parameters['milk_yield_potential']['value'])):

                                d_MC.parameters['milk_yield_potential']['value'][t][1] = (
                              d_MC.parameters['milk_yield_potential']['value'][t][1] * random_bvar[i,5])

                       # gestation curve
                            for t in range(0,len(d_MC.parameters['gestation_feasibility_curve']['value'])):

                                d_MC.parameters['gestation_feasibility_curve']['value'][t][1] = (
                              d_MC.parameters['gestation_feasibility_curve']['value'][t][1] * random_bvar[i,6])



                        elif (b == 'improved'):

                            #d_MC = Cow(age=start_age,breed='Improved Tanz southern highlands') 

                        # Define random breed parameters

                            # Un subscripted parameters
                            d_MC.parameters['gestation_duration']['value'] = random_bvar[i,10]*(
                            d_MC.parameters['gestation_duration']['value'])

                            d_MC.parameters['conception_after_calving']['value'] = random_bvar[i,11]*(
                            d_MC.parameters['conception_after_calving']['value'])

                            d_MC.parameters['lactation_duration']['value'] = random_bvar[i,12]*(
                            d_MC.parameters['lactation_duration']['value'])

                           # milk yield potential
                            for t in range(0,len(d_MC.parameters['milk_yield_potential']['value'])):

                                d_MC.parameters['milk_yield_potential']['value'][t][1] = (
                              d_MC.parameters['milk_yield_potential']['value'][t][1] * random_bvar[i,13])

                       # gestation curve
                            for t in range(0,len(d_MC.parameters['gestation_feasibility_curve']['value'])):

                                d_MC.parameters['gestation_feasibility_curve']['value'][t][1] = (
                              d_MC.parameters['gestation_feasibility_curve']['value'][t][1] * random_bvar[i,14])

                        for m in range(0,12):


                            GRASS.parameters['ME']['value'][m] = GRASS.parameters['ME']['value'][m] * random_fvar[i,0]  
                            GRASS.parameters['CP']['value'][m] = GRASS.parameters['CP']['value'][m] * random_fvar[i,1]
                            GRASS.parameters['DMD']['value'][m] = GRASS.parameters['DMD']['value'][m] * random_fvar[i,2]  
                            GRASS.parameters['NDF']['value'][m] = GRASS.parameters['NDF']['value'][m] * random_fvar[i,3]
                            GRASS.parameters['ADF']['value'][m] = GRASS.parameters['ADF']['value'][m] * random_fvar[i,4]

                            PASTURE.parameters['ME']['value'][m] = PASTURE.parameters['ME']['value'][m] * random_fvar[i,5]  
                            PASTURE.parameters['CP']['value'][m] = PASTURE.parameters['CP']['value'][m] * random_fvar[i,6]
                            PASTURE.parameters['DMD']['value'][m] = PASTURE.parameters['DMD']['value'][m] * random_fvar[i,7]  
                            PASTURE.parameters['NDF']['value'][m] = PASTURE.parameters['NDF']['value'][m] * random_fvar[i,8]
                            PASTURE.parameters['ADF']['value'][m] = PASTURE.parameters['ADF']['value'][m] * random_fvar[i,9]

                            NAPIER.parameters['ME']['value'][m] = NAPIER.parameters['ME']['value'][m] * random_fvar[i,10]  
                            NAPIER.parameters['CP']['value'][m] = NAPIER.parameters['CP']['value'][m] * random_fvar[i,11]
                            NAPIER.parameters['DMD']['value'][m] = NAPIER.parameters['DMD']['value'][m] * random_fvar[i,12]  
                            NAPIER.parameters['NDF']['value'][m] = NAPIER.parameters['NDF']['value'][m] * random_fvar[i,13]
                            NAPIER.parameters['ADF']['value'][m] = NAPIER.parameters['ADF']['value'][m] * random_fvar[i,14]

                            NAPIER_SILAGE.parameters['ME']['value'][m] = NAPIER_SILAGE.parameters['ME']['value'][m] * random_fvar[i,15]  
                            NAPIER_SILAGE.parameters['CP']['value']= NAPIER_SILAGE.parameters['CP']['value'] * random_fvar[i,16]  
                            NAPIER_SILAGE.parameters['DMD']['value'][m] = NAPIER_SILAGE.parameters['DMD']['value'][m] * random_fvar[i,17] 
                            NAPIER_SILAGE.parameters['NDF']['value'] = NAPIER_SILAGE.parameters['NDF']['value'] * random_fvar[i,18]
                            NAPIER_SILAGE.parameters['ADF']['value'] = NAPIER_SILAGE.parameters['ADF']['value'] * random_fvar[i,19]

                            SUNFLOWER_SEED_MEAL.parameters['ME']['value'] = (
                                SUNFLOWER_SEED_MEAL.parameters['ME']['value'] * random_fvar[i,20]  )
                            SUNFLOWER_SEED_MEAL.parameters['CP']['value'] = (
                                SUNFLOWER_SEED_MEAL.parameters['CP']['value']* random_fvar[i,21])
                            SUNFLOWER_SEED_MEAL.parameters['DMD']['value']= (
                                SUNFLOWER_SEED_MEAL.parameters['DMD']['value'] * random_fvar[i,22]  )
                            SUNFLOWER_SEED_MEAL.parameters['NDF']['value']= (
                                SUNFLOWER_SEED_MEAL.parameters['NDF']['value'] * random_fvar[i,23])
                            SUNFLOWER_SEED_MEAL.parameters['ADF']['value'] = (
                                SUNFLOWER_SEED_MEAL.parameters['ADF']['value'] * random_fvar[i,24])

                            MAIZE_BRAN.parameters['ME']['value'] = MAIZE_BRAN.parameters['ME']['value'] * random_fvar[i,25]  
                            MAIZE_BRAN.parameters['CP']['value'] = MAIZE_BRAN.parameters['CP']['value'] * random_fvar[i,26]
                            MAIZE_BRAN.parameters['DMD']['value'] = MAIZE_BRAN.parameters['DMD']['value'] * random_fvar[i,27]  
                            MAIZE_BRAN.parameters['NDF']['value'] = MAIZE_BRAN.parameters['NDF']['value'] * random_fvar[i,28]
                            MAIZE_BRAN.parameters['ADF']['value'] = MAIZE_BRAN.parameters['ADF']['value'] * random_fvar[i,29]

                            STOVER.parameters['ME']['value'][m] = STOVER.parameters['ME']['value'][m] * random_fvar[i,30]  
                            STOVER.parameters['CP']['value'][m] = STOVER.parameters['CP']['value'][m] * random_fvar[i,31]
                            STOVER.parameters['DMD']['value'][m] = STOVER.parameters['DMD']['value'][m] * random_fvar[i,32]  
                            STOVER.parameters['NDF']['value'][m] = STOVER.parameters['NDF']['value'][m] * random_fvar[i,33]
                            STOVER.parameters['ADF']['value'][m] = STOVER.parameters['ADF']['value'][m] * random_fvar[i,34]



                        output_df_MC = d_MC.run(max_simu_range)

                        simu_period = 0
                        start_lact = 0
                        start_count = 0
                        lifetime_milk_mc[i] = 0 

                        for m in range(len(output_df_MC)):


                            # if cow start counting after lactation begins
                            if (((output_df_MC._get_value(m,'Cow_milk_yield') > 0 ) and (start_lact == 0))
                                or ( (start_age == m))):

                                start_lact = 1

                                if (start_count == 0):
                                        start_count = m

                            if (start_count > 0 ):
                                simu_period += 1

                            # sum results variables over simulation period
                            lifetime_milk_mc[i] += output_df_MC._get_value(m,'Cow_milk_yield')

                        if (b == 'local'):

                            d_MC = Cow(age=start_age,breed='Zebu -- TZ south high') 
                            d_MC.sex='f' 
                            d_MC.feed_supply = feed_storage

                            # Define random breed parameters
                            # Un subscripted parameters
                            d_MC.parameters['gestation_duration']['value'] = (
                            d_MC.parameters['gestation_duration']['value'])/random_bvar[i,2]


                            d_MC.parameters['conception_after_calving']['value'] = (
                            d_MC.parameters['conception_after_calving']['value'])/random_bvar[i,3]


                            d_MC.parameters['lactation_duration']['value'] = (
                            d_MC.parameters['lactation_duration']['value'])/random_bvar[i,4]

                        # milk yield potential
                            for t in range(0,len(d_MC.parameters['milk_yield_potential']['value'])):

                                d_MC.parameters['milk_yield_potential']['value'][t][1] = (
                              d_MC.parameters['milk_yield_potential']['value'][t][1] / random_bvar[i,5])

                       # gestation curve
                            for t in range(0,len(d_MC.parameters['gestation_feasibility_curve']['value'])):

                                d_MC.parameters['gestation_feasibility_curve']['value'][t][1] = (
                              d_MC.parameters['gestation_feasibility_curve']['value'][t][1] / random_bvar[i,6])


                        elif (b == 'improved'):

                            d_MC = Cow(age=start_age,breed='Improved Tanz southern highlands') 
                            d_MC.sex='f' 
                            d_MC.feed_supply = feed_storage

                        # Define random breed parameters
                            # Un subscripted parameters
                            d_MC.parameters['gestation_duration']['value'] = (
                            d_MC.parameters['gestation_duration']['value'])/random_bvar[i,10]

                            d_MC.parameters['conception_after_calving']['value'] = (
                            d_MC.parameters['conception_after_calving']['value'])/random_bvar[i,11]

                            d_MC.parameters['lactation_duration']['value'] = (
                            d_MC.parameters['lactation_duration']['value'])/random_bvar[i,12]

                           # milk yield potential
                            for t in range(0,len(d_MC.parameters['milk_yield_potential']['value'])):

                                d_MC.parameters['milk_yield_potential']['value'][t][1] = (
                              d_MC.parameters['milk_yield_potential']['value'][t][1] /random_bvar[i,13])

                       # gestation curve
                            for t in range(0,len(d_MC.parameters['gestation_feasibility_curve']['value'])):

                                d_MC.parameters['gestation_feasibility_curve']['value'][t][1] = (
                              d_MC.parameters['gestation_feasibility_curve']['value'][t][1] / random_bvar[i,14])


                        # Re-iniitialize variables
                        for m in range(0,12):

                            GRASS.parameters['ME']['value'][m] = GRASS.parameters['ME']['value'][m] / random_fvar[i,0]  
                            GRASS.parameters['CP']['value'][m] = GRASS.parameters['CP']['value'][m] / random_fvar[i,1]
                            GRASS.parameters['DMD']['value'][m] = GRASS.parameters['DMD']['value'][m] / random_fvar[i,2]  
                            GRASS.parameters['NDF']['value'][m] = GRASS.parameters['NDF']['value'][m] / random_fvar[i,3]
                            GRASS.parameters['ADF']['value'][m] = GRASS.parameters['ADF']['value'][m] / random_fvar[i,4]

                            PASTURE.parameters['ME']['value'][m] = PASTURE.parameters['ME']['value'][m] / random_fvar[i,5]  
                            PASTURE.parameters['CP']['value'][m] = PASTURE.parameters['CP']['value'][m] / random_fvar[i,6]
                            PASTURE.parameters['DMD']['value'][m] = PASTURE.parameters['DMD']['value'][m] / random_fvar[i,7]  
                            PASTURE.parameters['NDF']['value'][m] = PASTURE.parameters['NDF']['value'][m] / random_fvar[i,8]
                            PASTURE.parameters['ADF']['value'][m] = PASTURE.parameters['ADF']['value'][m] / random_fvar[i,9]

                            NAPIER.parameters['ME']['value'][m] = NAPIER.parameters['ME']['value'][m] / random_fvar[i,10]  
                            NAPIER.parameters['CP']['value'][m] = NAPIER.parameters['CP']['value'][m] / random_fvar[i,11]
                            NAPIER.parameters['DMD']['value'][m] = NAPIER.parameters['DMD']['value'][m] / random_fvar[i,12]  
                            NAPIER.parameters['NDF']['value'][m] = NAPIER.parameters['NDF']['value'][m] / random_fvar[i,13]
                            NAPIER.parameters['ADF']['value'][m] = NAPIER.parameters['ADF']['value'][m] / random_fvar[i,14]

                            NAPIER_SILAGE.parameters['ME']['value'][m] = NAPIER_SILAGE.parameters['ME']['value'][m] / random_fvar[i,15]  
                            NAPIER_SILAGE.parameters['CP']['value'] = NAPIER_SILAGE.parameters['CP']['value'] / random_fvar[i,16]  
                            NAPIER_SILAGE.parameters['DMD']['value'][m] = NAPIER_SILAGE.parameters['DMD']['value'][m] / random_fvar[i,17] 
                            NAPIER_SILAGE.parameters['NDF']['value'] = NAPIER_SILAGE.parameters['NDF']['value'] / random_fvar[i,18]
                            NAPIER_SILAGE.parameters['ADF']['value'] = NAPIER_SILAGE.parameters['ADF']['value'] / random_fvar[i,19]

                            SUNFLOWER_SEED_MEAL.parameters['ME']['value'] = (
                                SUNFLOWER_SEED_MEAL.parameters['ME']['value'] / random_fvar[i,20]  )
                            SUNFLOWER_SEED_MEAL.parameters['CP']['value'] = (
                                SUNFLOWER_SEED_MEAL.parameters['CP']['value'] / random_fvar[i,21])
                            SUNFLOWER_SEED_MEAL.parameters['DMD']['value']= (
                                SUNFLOWER_SEED_MEAL.parameters['DMD']['value'] / random_fvar[i,22]  )
                            SUNFLOWER_SEED_MEAL.parameters['NDF']['value'] = (
                                SUNFLOWER_SEED_MEAL.parameters['NDF']['value'] / random_fvar[i,23])
                            SUNFLOWER_SEED_MEAL.parameters['ADF']['value']= (
                                SUNFLOWER_SEED_MEAL.parameters['ADF']['value'] / random_fvar[i,24])

                            MAIZE_BRAN.parameters['ME']['value'] = MAIZE_BRAN.parameters['ME']['value'] / random_fvar[i,25]  
                            MAIZE_BRAN.parameters['CP']['value'] = MAIZE_BRAN.parameters['CP']['value'] / random_fvar[i,26]
                            MAIZE_BRAN.parameters['DMD']['value'] = MAIZE_BRAN.parameters['DMD']['value'] / random_fvar[i,27]  
                            MAIZE_BRAN.parameters['NDF']['value'] = MAIZE_BRAN.parameters['NDF']['value'] / random_fvar[i,28]
                            MAIZE_BRAN.parameters['ADF']['value'] = MAIZE_BRAN.parameters['ADF']['value'] / random_fvar[i,29]

                            STOVER.parameters['ME']['value'][m] = STOVER.parameters['ME']['value'][m] / random_fvar[i,30]  
                            STOVER.parameters['CP']['value'][m] = STOVER.parameters['CP']['value'][m] / random_fvar[i,31]
                            STOVER.parameters['DMD']['value'][m] = STOVER.parameters['DMD']['value'][m] / random_fvar[i,32]  
                            STOVER.parameters['NDF']['value'][m] = STOVER.parameters['NDF']['value'][m] / random_fvar[i,33]
                            STOVER.parameters['ADF']['value'][m] = STOVER.parameters['ADF']['value'][m] / random_fvar[i,34]

                        if ( lifetime_milk_mc[i] > 0):   
                            annualized_milk_mc[i] =  lifetime_milk_mc[i]/simu_period


                    values = []

                    for value in annualized_milk_mc.values():

                        values.append(value)

                    # calculate standard deviation using np.std
                    ave_milk = {}
                    se_milk = {}
                    
                    if (b == 'local'):
                        
                        ave_milk['local'] = np.mean(values) 
                        se_milk['local'] =  round(100 * np.std(values) / ave_milk['local'], 1)
                        return  se_milk['local']

                    elif (b == 'improved'):
                        
                        ave_milk['improved'] = np.mean(values) 
                        se_milk['improved'] =round(100 * np.std(values) / ave_milk['improved'], 1)
                        return  se_milk['improved']

                    








