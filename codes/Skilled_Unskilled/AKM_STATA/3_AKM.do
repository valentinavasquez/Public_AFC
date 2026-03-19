********************************************************************************
* AKM estimation adapted to processed_data.csv
* Valentina - AFC processed data
********************************************************************************

clear all
clear frames
set more off

********************************************************************************
* 0. PATHS
********************************************************************************

global base   "/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases"
global data   "${base}/processed_data.csv"
global ipc    "${base}/ipc_clean.dta"
global out    "${base}"

********************************************************************************
* 1. INSTALL PACKAGES IF NEEDED
********************************************************************************

cap which reghdfe
if _rc ssc install reghdfe, replace

cap which ftools
if _rc ssc install ftools, replace

cap which gtools
if _rc ssc install gtools, replace

********************************************************************************
* 2. IMPORT MAIN DATABASE
********************************************************************************

import delimited "${data}", clear varn(1)

compress

********************************************************************************
* 3. DEFINE MAIN VARIABLES
********************************************************************************

* IDs
gen long wid = id_person
gen long fid = id_employer

* Time
gen int  year  = wage_year
gen byte month = wage_month

* Wage
gen double wage = taxable_income

* Tenure building block
gen byte meses = 1

* Keep relevant sample
drop if missing(wid)
drop if missing(fid)
drop if missing(year)
drop if missing(month)
drop if missing(wage)

drop if wage <= 0

********************************************************************************
* 4. MERGE MONTHLY IPC AND DEFLATE WAGES
********************************************************************************

merge m:1 year month using "${ipc}", keep(3) nogen

gen double wage_real = wage / ipc_def * 100
drop if missing(wage_real) | wage_real <= 0

gen double ln_wage = ln(wage_real)

********************************************************************************
* 5. KEEP ONE JOB PER PERSON-MONTH: HIGHEST WAGE
********************************************************************************

gsort wid year month -wage_real -fid
by wid year month: keep if _n == 1

********************************************************************************
* 6. CREATE AGE CONTROLS
********************************************************************************

* You already have age
drop if missing(age)

gen double edad_norm  = (age - 40) / 40
gen double edad_norm2 = edad_norm^2
gen double edad_norm3 = edad_norm^3

********************************************************************************
* 7. CREATE TENURE
********************************************************************************

sort wid fid year month
by wid fid: gen int tenure = sum(meses)

********************************************************************************
* 8. SAVE PRE-AKM SAMPLE
********************************************************************************

save "${out}/akm_prepared_sample.dta", replace

********************************************************************************
* 9. BUILD FIRM CONNECTED SET
*    Keep largest connected component of firm-worker graph
********************************************************************************

frames put fid wid wage_real meses year month, into(connected)
frame change connected

* initialize connected-component propagation
gen byte period = 1
gen double group = .
replace group = 1 if _n == 1

local dif_group = 1

while `dif_group' > 0 {
    
    count if group == 1
    local n1 = r(N)

    * all workers in same firm inherit group
    gsort period fid group
    by period fid: replace group = group[1]

    * all firms of same worker inherit group
    gsort period wid group
    by period wid: replace group = group[1]

    count if group == 1
    local n2 = r(N)

    local dif_group = `n2' - `n1'
    di "Difference in connected firms iteration: `dif_group'"
}

* identify firms outside largest connected component
gsort fid
by fid: gen byte uniq_fid = (_n == 1)

count if uniq_fid
di "Total firms: " r(N)

count if uniq_fid & group == 1
di "Connected firms: " r(N)

count if uniq_fid & missing(group)
di "Disconnected firms: " r(N)

keep if uniq_fid & missing(group)
keep fid

tempfile todrop_firms
save `todrop_firms', replace

frame change default

merge m:1 fid using `todrop_firms'
drop if _merge == 3
drop _merge

save "${out}/akm_firm_connected_sample.dta", replace

********************************************************************************
* 10. ESTIMATE AKM - SPEC 1: WORKER FE + FIRM FE
********************************************************************************

use "${out}/akm_firm_connected_sample.dta", clear

reghdfe ln_wage edad_norm2 edad_norm3 tenure, absorb(wid_fe1=wid fid_fe1=fid)

matrix coefs_reg1 = e(b)

gen double b_edad2_reg1  = coefs_reg1[1,1]
gen double b_edad3_reg1  = coefs_reg1[1,2]
gen double b_tenure_reg1 = coefs_reg1[1,3]

gen double ability_reg1 = wid_fe1 + ///
    b_edad2_reg1 * edad_norm2 + ///
    b_edad3_reg1 * edad_norm3 + ///
    b_tenure_reg1 * tenure

save "${out}/akm_spec1_data.dta", replace

********************************************************************************
* 11. BUILD FIRM×YEAR CONNECTED SET
********************************************************************************

frames put fid wid year, into(connected_fy)
frame change connected_fy

egen long fid_year_node = group(fid year)

gen double group_fy = .
replace group_fy = 1 if _n == 1

local dif_group = 1

while `dif_group' > 0 {

    count if group_fy == 1
    local n1 = r(N)

    * same firm-year node
    gsort fid_year_node group_fy
    by fid_year_node: replace group_fy = group_fy[1]

    * same worker across different firm-years
    gsort wid group_fy
    by wid: replace group_fy = group_fy[1]

    count if group_fy == 1
    local n2 = r(N)

    local dif_group = `n2' - `n1'
    di "Difference in firm-year connected iteration: `dif_group'"
}

gsort fid year
by fid year: gen byte uniq_fy = (_n == 1)

count if uniq_fy
di "Total firm-year nodes: " r(N)

count if uniq_fy & group_fy == 1
di "Connected firm-year nodes: " r(N)

count if uniq_fy & missing(group_fy)
di "Disconnected firm-year nodes: " r(N)

keep if uniq_fy & missing(group_fy)
keep fid year

tempfile todrop_fy
save `todrop_fy', replace

frame change default
frame drop connected_fy

merge m:1 fid year using `todrop_fy'
drop if _merge == 3
drop _merge

save "${out}/akm_firmyear_connected_sample.dta", replace

********************************************************************************
* 12. ESTIMATE AKM - SPEC 2: WORKER FE + FIRM×YEAR FE
********************************************************************************

use "${out}/akm_firmyear_connected_sample.dta", clear

reghdfe ln_wage edad_norm2 edad_norm3 tenure, absorb(wid_fe2=wid fid_year_fe2=fid#year)

matrix coefs_reg2 = e(b)

gen double b_edad2_reg2  = coefs_reg2[1,1]
gen double b_edad3_reg2  = coefs_reg2[1,2]
gen double b_tenure_reg2 = coefs_reg2[1,3]

gen double ability_reg2 = wid_fe2 + ///
    b_edad2_reg2 * edad_norm2 + ///
    b_edad3_reg2 * edad_norm3 + ///
    b_tenure_reg2 * tenure

save "${out}/akm_spec2_data.dta", replace

********************************************************************************
* 13. OPTIONAL: SPEC 3 WITHOUT TENURE
********************************************************************************

reghdfe ln_wage edad_norm2 edad_norm3, absorb(wid_fe3=wid fid_year_fe3=fid#year)

matrix coefs_reg3 = e(b)

gen double b_edad2_reg3 = coefs_reg3[1,1]
gen double b_edad3_reg3 = coefs_reg3[1,2]

gen double ability_reg3 = wid_fe3 + ///
    b_edad2_reg3 * edad_norm2 + ///
    b_edad3_reg3 * edad_norm3

save "${out}/akm_spec3_data.dta", replace

********************************************************************************
* 14. SAVE FIRM EFFECTS
********************************************************************************

preserve
    keep fid fid_fe1
    duplicates drop fid, force
    save "${out}/firm_fe_baseline.dta", replace
restore

preserve
    keep fid year fid_year_fe2
    duplicates drop fid year, force
    save "${out}/firmyear_fe_spec2.dta", replace
restore

********************************************************************************
* 15. SAVE WORKER ABILITY
********************************************************************************

preserve
    keep wid wid_fe1 ability_reg1
    duplicates drop wid, force
    save "${out}/worker_ability_spec1.dta", replace
restore

preserve
    keep wid wid_fe2 ability_reg2
    duplicates drop wid, force
    save "${out}/worker_ability_spec2.dta", replace
restore

********************************************************************************
* 16. QUICK DIAGNOSTICS
********************************************************************************

use "${out}/akm_spec1_data.dta", clear

bys wid (year month): gen move = fid != fid[_n-1] if _n > 1
replace move = 0 if missing(move)

tab move

sum ln_wage tenure edad_norm2 edad_norm3
sum wid_fe1 fid_fe1 ability_reg1

********************************************************************************
* END
********************************************************************************
di "AKM script finished successfully."
