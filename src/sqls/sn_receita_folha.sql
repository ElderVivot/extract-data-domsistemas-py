select codi_emp, sum(valor + valor_inss_cpp) as valor
  from bethadba.efsimples_nacional_folha_anterior
 where codi_emp = '#codi_emp#'
   and periodo BETWEEN date('#competence#') and date('#competence_fim#')
group by codi_emp