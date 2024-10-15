SELECT emp.codi_emp,
       list(distinct sn.anexo) as anexos

  FROM bethadba.efsdoimp_simples_nacional AS sn
       INNER JOIN bethadba.geempre AS emp
            ON    emp.codi_emp = sn.codi_emp

 WHERE sn.data_sim = '#date_filter#' 
   AND sn.anexo <> 0

GROUP BY emp.codi_emp

ORDER BY emp.codi_emp