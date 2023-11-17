SELECT STRING(subDataFetch.codeCompanieAccountSystem) AS codeCompanieAccountSystem,
       STRING(subDataFetch.codeEmployee) AS codeEmployee,
       MAX(subDataFetch.name) AS name,
       DATEFORMAT(MAX(subDataFetch.admission), 'YYYY-MM-DD') AS admission,
       MAX(string(subDataFetch.cpf)) AS cpf,
       MAX(string(subDataFetch.matriculaEsocial)) AS matriculaEsocial,
       MAX(string(subDataFetch.codeRole)) AS codeRole,
       MAX(subDataFetch.nameRole) AS nameRole,
       DATEFORMAT(MAX(subDataFetch.resignation), 'YYYY-MM-DD') AS resignation,
       MAX(subDataFetch.typeEmployee) AS typeEmployee,
       MAX(subDataFetch.vinculo) AS vinculo,
       MAX(subDataFetch.hasHealthMonitoring) AS hasHealthMonitoring

  FROM (

    SELECT fun.codi_emp AS codeCompanieAccountSystem, fun.i_empregados AS codeEmployee, fun.nome AS name, fun.admissao AS admission, fun.cpf AS cpf, 
           CASE WHEN fun.codigo_esocial IS NULL THEN '' ELSE fun.codigo_esocial END AS matriculaEsocial,
          fun.i_cargos AS codeRole, cargo.nome AS nameRole, res.demissao AS resignation, fun.tipo_epr AS typeEmployee, fun.vinculo, 0 AS hasHealthMonitoring

      FROM bethadba.foempregados AS fun
          INNER JOIN bethadba.focargos AS cargo
                ON    cargo.codi_emp = fun.codi_emp
                  AND cargo.i_cargos = fun.i_cargos
          LEFT JOIN bethadba.forescisoes AS res 
              ON    res.codi_emp = fun.codi_emp 
                AND res.i_empregados = fun.i_empregados 

    WHERE fun.codi_emp = '#codi_emp#'
      and ( res.demissao is null or res.demissao > date('2023-01-15'))
      
      
    UNION ALL 
    
    SELECT mst.codi_emp AS codeCompanieAccountSystem, mst.i_empregados AS codeEmployee, '' AS name, null AS admission, '' AS cpf, '' AS matriculaEsocial,
          0 AS codeRole, '' AS nameRole, null AS resignation, null as typeEmployee, null as vinculo, 1 AS hasHealthMonitoring

      FROM bethadba.FOMONITORAMENTO_SAUDE_TRABALHADOR AS mst
           inner join bethadba.foempregados as fun 
                on    fun.codi_emp = mst.CODI_EMP 
                  and fun.i_empregados = mst.I_EMPREGADOS
           LEFT JOIN bethadba.forescisoes AS res 
              ON    res.codi_emp = fun.codi_emp 
                AND res.i_empregados = fun.i_empregados 
           inner join bethadba.FOMONITORAMENTO_SAUDE_TRABALHADOR_ENVIOS_ESOCIAL as mste
                on    mste.codi_emp = mst.codi_emp
                  and mste.i_empregados = mst.i_empregados
                  and mste.sequencial = mst.sequencial
           inner join bethadba.FOESOCIAL_DADOS_EVENTOS as ede 
                on    ede.codi_emp = mste.CODI_EMP_DADOS_EVENTOS 
                  and ede.I_DADOS_EVENTOS = mste.I_DADOS_EVENTOS 
                  and ede.I_EVENTO_ESOCIAL = 2220
                  and ede.I_RESPOSTA in (201,202)
                  and ede.TIPO_ENVIO = 1
                  and NOT EXISTS ( SELECT 1
	                                  FROM bethadba.foesocial_dados_eventos AS ede2
	                                  WHERE ede2.codi_emp = ede.codi_emp
	                                    AND ede2.i_evento_esocial = 3000
	                                    AND ede2.tipo_envio = 3
	                                    AND ede2.i_evento_esocial_excluido = ede.i_evento_esocial
	                                    AND ede2.numero_recibo_excluido = ede.numero_recibo )

    WHERE mst.codi_emp = '#codi_emp#'
      and ( res.demissao is null or res.demissao > date('2023-01-15'))

  ) AS subDataFetch
  
GROUP BY subDataFetch.codeCompanieAccountSystem, subDataFetch.codeEmployee
  
ORDER BY subDataFetch.codeCompanieAccountSystem, subDataFetch.codeEmployee