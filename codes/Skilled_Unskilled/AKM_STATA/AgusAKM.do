/*==============================================================================
Estimación de "ability" del trabajador basado en Chan et al (2021)

El código para tener el conjunto conectado viene de Rosario Aldunate :)
==============================================================================*/

clear frames
clear all

global main		"Y:\DPM\GEE\DME\adiaz\Labor Concentration"
global load 	"${main}\data"

global wid = "rut_informado"
global fid = "rut_declara"
global ind = "ciiu_cl"
global cty = "comuna"
global emp = "labor"
global per = "year"
global wage= "renta_neta"


/*==============================================================================
						Prepare database
						
Keep one job per month
Restrict the sample to connected firms						
==============================================================================*/


/* 					Caso 1: Utilizamos datos anuales

- Mantener un trabajo al año según salario.
*/


use "${load}\full_sample_nested.dta", clear

gstats sum $wage
replace ${wage} = ${wage}/meses

// Filtro sueldo mínimo

	* minimum wage
	gen double wmin = .
	replace wmin =117824 if year==2004
	replace wmin =123750 if year==2005
	replace wmin =131250 if year==2006
	replace wmin =139500 if year==2007
	replace wmin =151500 if year==2008
	replace wmin =162000 if year==2009
	replace wmin =168500 if year==2010
	replace wmin =177000 if year==2011
	replace wmin =187500 if year==2012
	replace wmin =201500 if year==2013
	replace wmin =217500 if year==2014
	replace wmin =233000 if year==2015
	replace wmin =253750 if year==2016
	replace wmin =267000 if year==2017
	replace wmin =280000 if year==2018
	replace wmin =298833 if year==2019
	replace wmin =319250 if year==2020

	// filter
	gstats sum $wage
	keep if ${wage} > 0.9*wmin
	gstats sum $wage

	merge m:1 ${per} using "${load}\IPC.dta", nogenerate keepusing(ipc_def) keep(3)

	replace ${wage} = (${wage}/ipc_def*100)
	gstats sum ${wage}

	// Define connected set
	
frames put ${fid} ${wid} ${wage} meses ${per}, into(connected)
	frames change connected
	
	* keep the job with higher wage
	hashsort ${wid} ${per} ${fid}
	by ${wid} ${per}: keep if _n == _N
	by ${wid} ${per}: gen dup = _N
		gstats sum dup
		drop dup

	* define connected firms set	
	hashsort ${wid} ${per}
	by ${wid}: gen changer = ${fid} != ${fid}[_n-1] & _n!=1

	gegen changer_firm = sum(changer), by(${fid} ${per})

	gen byte period = 1
		
	gen group = .
		bys period: replace group = 1 if _n==1
		local dif_group = 1
	
	while `dif_group'>0{
		count if group==1
		local aux_group_1=`r(N)'	
			
			* all workers in the same firm
			hashsort period ${fid} group
				by period ${fid}: replace group = group[1]
				
			* all workers who worked in that firm
			hashsort period ${wid} group
				by period ${wid}: replace group = group[1]
				
		count if group==1
		local aux_group_2=`r(N)'	
		local dif_group = `aux_group_2'-`aux_group_1'
		di "Difference: `dif_group'"
	}

	count if missing(group)	// 169,496 of 70,791,968
	gstats sum group
	
	hashsort ${fid}
	by ${fid}: gen uniq_fid = (_n==1)
	
	count if uniq_fid
	count if uniq_fid & group==1
	count if uniq_fid & missing(group)
	
	* idenficar firmas no conectadas 
	keep if uniq_fid & missing(group) 
	keep ${fid} 
	
	tempfile todrop
	save `todrop', replace
	
frames change default

merge m:1 ${fid} using `todrop'
	drop if _merge==3
	drop _merge

// Save intermediate dataset with connected set
save "${load}/full_sample_connected.dta", replace
di as result "Connected set sample saved to full_sample_connected.dta"
	
	use "${load}/full_sample_connected.dta", clear
	// AKM estimation

	gen double ln_wage = ln(${wage})
		
	describe using "${load}\base_rc_idSII_merge_servel_28082018.dta"	
		
	destring ${wid}, gen(wid)
	merge m:1 wid using "${load}\base_rc_idSII_merge_servel_28082018.dta", ///
	keepusing(ano_nacimiento genero)
		keep if inlist(_merge, 3)
		drop _merge wid
		
	gen edad = year - ano_nacimiento
	encode genero, gen(gender)
		drop genero
	
	// --- Card-Heining-Kline (2013) age normalization ---
	// CHK normalize age before estimation: edad_norm = (edad - 40) / 40
	// This centers the polynomial at age 40 and rescales to [-1,1] range,
	// ensuring the quadratic and cubic terms are well-conditioned.
	// Linear age is omitted (collinear with worker FE + firm×year FE in Spec 2;
	// omitted in Spec 1 as well for consistency across specifications).
	gen edad_norm  = (edad - 40) / 40
	gen edad_norm2 = edad_norm^2
	gen edad_norm3 = edad_norm^3
	
	sort ${wid} ${fid} ${per}
	by ${wid} ${fid}: gen int tenure = sum(meses)
	* ojo: podría corregir para exigir años consecutivos... ¿necesario?

	*===========================================================================
	* SPECIFICATION 1: Firm FE (on firm-level connected set)
	*===========================================================================
	
	timer clear
	timer on 1
		
		// CHK: quadratic + cubic in normalized age, no linear term
		reghdfe ln_wage edad_norm2 edad_norm3 tenure, a(wid_fe1=${wid} fid_fe1=${fid})
		mat coefs_reg1 = e(b)
		mat var_reg1 = e(V)
		mat li coefs_reg1
		
	timer off 1
	timer list
	
	// Extract Spec 1 coefficients and compute ability
	gen b_edad2_reg1	= coefs_reg1[1,1]
	gen b_edad3_reg1	= coefs_reg1[1,2]
	gen b_tenure_reg1	= coefs_reg1[1,3]
	
	gen ability_reg1 = wid_fe1 + b_edad2_reg1 * edad_norm^2 + b_edad3_reg1 * edad_norm^3 + b_tenure_reg1 * tenure
	
	// Save full firm-connected sample with Spec 1 results
	// Use explicit path (not tempfile) so it survives frame switches
	save "${main}/data_int/temp/akm_spec1_data.dta", replace
	
	*===========================================================================
	* FIRM×YEAR CONNECTED SET FOR SPECIFICATION 2
	*
	* Following Chan, Mattana, Salgado & Xu (2023):
	* "Dynamic Wage Setting: The Role of Monopsony Power and Adjustment Costs"
	*
	* With firm×year FE, identification requires a connected set defined over
	* (firm, year) nodes rather than firm nodes. Two (firm, year) nodes are 
	* connected if the same worker is observed at both — either as a mover 
	* (different firms) or a stayer (same firm, consecutive years).
	* This is more restrictive than the firm-level connected set.
	*===========================================================================
	
	di as text _n(2) "{hline 80}"
	di as text "Computing firm×year connected set for Specification 2"
	di as text "{hline 80}" _n
	
	// Work in a separate frame to avoid modifying the main data
	frames put ${fid} ${wid} ${per}, into(connected_fy)
	frames change connected_fy
	
		// Create unique node identifier for each (firm, year) pair
		egen long fid_year_node = group(${fid} ${per})
		
		// Iterative connected component algorithm
		// Initialize: assign group=1 to first observation, propagate through
		// (firm,year) nodes and worker links until convergence
		gen group_fy = .
		replace group_fy = 1 if _n == 1
		local dif_group = 1
		
		while `dif_group' > 0 {
			count if group_fy == 1
			local aux_group_1 = `r(N)'
			
			// All workers at the same (firm, year) get same group
			hashsort fid_year_node group_fy
			by fid_year_node: replace group_fy = group_fy[1]
			
			// All (firm, year) nodes of the same worker get same group
			// This connects: stayers (j,t)→(j,t+1) and movers (j,t)→(j',t')
			hashsort ${wid} group_fy
			by ${wid}: replace group_fy = group_fy[1]
			
			count if group_fy == 1
			local aux_group_2 = `r(N)'
			local dif_group = `aux_group_2' - `aux_group_1'
			di "Difference (firm×year connected set): `dif_group'"
		}
		
		// Report diagnostics
		count if missing(group_fy)
		di as text _n "Workers not in any connected component: " r(N)
		
		hashsort ${fid} ${per}
		by ${fid} ${per}: gen uniq_fy = (_n == 1)
		
		count if uniq_fy
		di as text "Total (firm, year) pairs: " r(N)
		count if uniq_fy & group_fy == 1
		di as text "(firm, year) pairs in largest connected component: " r(N)
		count if uniq_fy & missing(group_fy)
		di as text "(firm, year) pairs NOT connected: " r(N)
		
		// Keep disconnected (firm, year) pairs — these will be dropped from Spec 2
		keep if uniq_fy & missing(group_fy)
		keep ${fid} ${per}
		
		// Use explicit path (not tempfile) so it survives the frame switch
		save "${main}/data_int/temp/akm_todrop_fy.dta", replace
	
	frames change default
	frame drop connected_fy
	
	// Restrict to firm×year connected set for Spec 2
	merge m:1 ${fid} ${per} using "${main}/data_int/temp/akm_todrop_fy.dta"
	count if _merge == 3
	di as text _n "Observations dropped (not in firm×year connected set): " r(N)
	drop if _merge == 3
	drop _merge
	
	di as text "Observations in firm×year connected set for Spec 2: " _N
	
	*===========================================================================
	* SPECIFICATION 2: Firm×Year FE (on firm×year connected set)
	*
	* CHK (2013) AGE NORMALIZATION:
	* With worker FE (alpha_i) and firm×year FE (psi_jt), linear age is
	* collinear: edad = year - birthyear. Following Card, Heining, and Kline
	* (2013, QJE), we use quadratic and cubic terms of normalized age
	* (edad_norm = (edad-40)/40) and omit the linear term. The same
	* specification is used in Spec 1 for consistency.
	*===========================================================================
	
	timer on 2
		
		// CHK: quadratic + cubic in normalized age, no linear term
		reghdfe ln_wage edad_norm2 edad_norm3 tenure, a(wid_fe2=${wid} fid_year_fe2=${fid}#year)
		mat coefs_reg2 = e(b)
		mat var_reg2 = e(V)
		
	timer off 2
	timer list
	
	// Extract Spec 2 coefficients and compute ability
	gen b_edad2_reg2	= coefs_reg2[1,1]
	gen b_edad3_reg2	= coefs_reg2[1,2]
	gen b_tenure_reg2	= coefs_reg2[1,3]

	gen ability_reg2 = wid_fe2 + b_edad2_reg2 * edad_norm^2 + b_edad3_reg2 * edad_norm^3 + b_tenure_reg2 * tenure

	// SPECIFICATION 4: Firm×Year FE without age/tenure controls (variant of Spec 2)
	// Run worker and firm×year fixed effects only (no age or tenure covariates)
	timer on 3
		reghdfe ln_wage, a(wid_fe4=${wid} fid_year_fe4=${fid}#year)
		mat coefs_reg4 = e(b)
	timer off 3
	timer list

	gen ability_reg4 = wid_fe4

	// SPECIFICATION 5: Firm×Year FE with age but WITHOUT tenure (variant of Spec 2)
	// Run worker and firm×year fixed effects with age controls but no tenure
	// This is like Spec 2 but omitting tenure from both the regression and ability measure
	timer on 4
		reghdfe ln_wage edad_norm2 edad_norm3, a(wid_fe5=${wid} fid_year_fe5=${fid}#year)
		mat coefs_reg5 = e(b)
		mat var_reg5 = e(V)
	timer off 4
	timer list

	// Extract Spec 5 coefficients and compute ability (NO TENURE TERM)
	gen b_edad2_reg5	= coefs_reg5[1,1]
	gen b_edad3_reg5	= coefs_reg5[1,2]

	gen ability_reg5 = wid_fe5 + b_edad2_reg5 * edad_norm^2 + b_edad3_reg5 * edad_norm^3

	// Save Spec 2, Spec 4, and Spec 5 results for merging back
	// Deduplicate at worker-firm-year (values are identical within group;
	// duplicates arise from the nested market structure in full_sample_nested)
	keep ${wid} ${fid} year wid_fe2 fid_year_fe2 ability_reg2 b_edad2_reg2 b_edad3_reg2 b_tenure_reg2 ///
		wid_fe4 fid_year_fe4 ability_reg4 ///
		wid_fe5 fid_year_fe5 ability_reg5 b_edad2_reg5 b_edad3_reg5
	gduplicates drop ${wid} ${fid} year, force
	save "${main}/data_int/temp/akm_spec2_results.dta", replace
	
	*===========================================================================
	* MERGE BOTH SPECIFICATIONS
	*===========================================================================
	
	// Reload full firm-connected sample with Spec 1 results
	use "${main}/data_int/temp/akm_spec1_data.dta", clear
	
	// Merge Spec 2 results (available only for firm×year connected observations)
	// m:1 because master may have duplicate worker-firm-year rows (nested markets)
	merge m:1 ${wid} ${fid} year using "${main}/data_int/temp/akm_spec2_results.dta", nogen
	
	// Report coverage
	count if !missing(ability_reg1)
	di as text _n "Observations with valid ability_reg1 (Spec 1, firm connected): " r(N)
	count if !missing(ability_reg2)
	di as text "Observations with valid ability_reg2 (Spec 2, firm×year connected): " r(N)
	count if !missing(ability_reg4)
	di as text "Observations with valid ability_reg4 (Spec 4, firm×year no age/tenure): " r(N)
	count if !missing(ability_reg5)
	di as text "Observations with valid ability_reg5 (Spec 5, firm×year age no tenure): " r(N)
	count if !missing(ability_reg1) & missing(ability_reg2)
	di as text "In firm connected set but NOT in firm×year connected set: " r(N)
	
	// CHK: both specs use edad_norm^2 and edad_norm^3 (no linear age)
	keep ${wid} ${fid} year meses tenure edad_norm wid_fe1 wid_fe2 wid_fe4 wid_fe5 fid_fe1 fid_year_fe2 fid_year_fe5 ability* b_* ln_wage
	
	save "${load}/worker_ability.dta", replace
	
	// Save firm fixed effects separately - Baseline specification (firm FE)
	preserve
		gcollapse (mean) fid_fe1, by(${fid})
		save "${load}/firm_fe_baseline.dta", replace
	restore
	
	// Save firm fixed effects separately - Firm-Year specification
	preserve
		gcollapse (mean) fid_year_fe2, by(${fid} year)
		save "${load}/firm_fe_firmyear.dta", replace
	restore
	
	// Save firm fixed effects separately - Firm-Year Spec 5 (age no tenure)
	preserve
		gcollapse (mean) fid_year_fe5, by(${fid} year)
		save "${load}/firm_fe_firmyear_notenure.dta", replace
	restore
	
	// Also save average firm-year FE collapsed to firm level
	preserve
		gcollapse (mean) fid_year_fe2_avg=fid_year_fe2, by(${fid})
		save "${load}/firm_fe_firmyear_avg.dta", replace
	restore
	
	// Also save average firm-year FE Spec 5 collapsed to firm level
	preserve
		gcollapse (mean) fid_year_fe5_avg=fid_year_fe5, by(${fid})
		save "${load}/firm_fe_firmyear_notenure_avg.dta", replace
	restore
	
	*===========================================================================
	* SUMMARY STATISTICS FOR WORKER FIXED EFFECTS
	*===========================================================================
	
	// Keep one observation per worker (from the firm×year connected set where all specs exist)
	preserve
	// Only keep workers with all worker FE measures (requires firm×year connectivity)
	keep if !missing(wid_fe1) & !missing(wid_fe2) & !missing(wid_fe4) & !missing(wid_fe5)
	bysort ${wid}: keep if _n == 1
	
	// Summary statistics for the worker fixed effects
	estpost sum wid_fe1 wid_fe2 wid_fe4 wid_fe5, d
	esttab using "${tables}\akm_worker_fe_stats.tex", replace ///
		cells("count mean sd min max") label nomtitle nonumber
	
	restore
	
	cap log close

	
	
	