SELECT emp.codi_emp,
       emp.nome_emp,
       emp.cgce_emp,
       
       sn.anexo,

       /* iss */
       sn.biss AS base_iss,
       sn.viss AS valor_iss, 
       aliquota_iss_normal = round(sn.viss / CASE WHEN sn.biss > 0 THEN sn.biss ELSE 1 END * 100, 2),
       aliquota_iss_retido = CASE WHEN sn.anexo IN (3,4,5) THEN round(sn.aliquota_efetiva_calculada - sn.aliquotan, 2) ELSE 0 END,
       aliquota_iss = CASE WHEN aliquota_iss_normal > 0 THEN aliquota_iss_normal ELSE aliquota_iss_retido END,
       aliq_tot = sn.aliquota_efetiva_calculada,

       /* icms */
       sn.bicms AS base_icms, 
       sn.vicms AS valor_icms, 
       aliquota_icms_normal = round(sn.vicms / CASE WHEN sn.bicms > 0 THEN sn.bicms ELSE 1 END * 100, 2),
       aliquota_icms_st = CASE WHEN sn.anexo IN (1,2) THEN round(sn.aliquota_efetiva_calculada - sn.aliquotan, 2) ELSE 0 END,
       aliquota_icms = CASE WHEN aliquota_icms_normal > 0 THEN aliquota_icms_normal ELSE aliquota_icms_st END,
        
       /* total imposto */
       base_total = sn.basen,
       valor_total = sn.valorn  

  FROM bethadba.efsdoimp_simples_nacional AS sn
       INNER JOIN bethadba.geempre AS emp
            ON    emp.codi_emp = sn.codi_emp

 WHERE sn.data_sim = '#date_filter#' 
   AND sn.anexo <> 0

ORDER BY emp.codi_emp, sn.anexo