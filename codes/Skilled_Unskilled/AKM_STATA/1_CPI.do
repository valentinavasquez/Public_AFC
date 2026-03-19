clear

*---------------------------------------------------
* 1. Importar Excel
*---------------------------------------------------

import excel "/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases/IPC_EMP_2023.xlsx", ///
firstrow clear

rename Periodo periodo
rename ÍndiceIPCGeneral ipc

*---------------------------------------------------
* 2. Verificar formato de fecha
*---------------------------------------------------

format periodo %td

*---------------------------------------------------
* 3. Extraer año y mes
*---------------------------------------------------

gen year  = year(periodo)
gen month = month(periodo)

*---------------------------------------------------
* 4. Convertir IPC a numérico
*---------------------------------------------------

destring ipc, replace dpcomma

rename ipc ipc_def

*---------------------------------------------------
* 5. Mantener variables necesarias
*---------------------------------------------------

keep year month ipc_def

sort year month

*---------------------------------------------------
* 6. Guardar base limpia
*---------------------------------------------------

save "/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases/ipc_clean.dta", replace


export delimited using "/Users/valentinavasquez/Documents/GitHub/Public_AFC/bases/ipc_clean.csv", replace
