SELECT subDataFetch.codeCompanieAccountSystem,
       subDataFetch.federalRegistration,
       subDataFetch.name,
       MAX(subDataFetch.markedFolhaModule) AS markedFolhaModule,
       MAX(subDataFetch.existParameterFolha) AS existParameterFolha,
       MAX(subDataFetch.markedFolhaParameterToSendEsocial) AS markedFolhaParameterToSendEsocial,
       SUM(subDataFetch.qtdEmployeesActive) AS qtdEmployeesActive,
       SUM(subDataFetch.qtdContribuintesActive) AS qtdContribuintesActive,
       SUM(subDataFetch.qtdContribuintesTypeEmpregadorActive) AS qtdContribuintesTypeEmpregadorActive,
       SUM(subDataFetch.qtdEventsS1000) AS qtdEventsS1000,
       COALESCE(MAX(subDataFetch.lastDateEmployeeResignation), '') AS lastDateEmployeeResignation,
       COALESCE(MAX(subDataFetch.lastDateContribuinteResignation), '') AS lastDateContribuinteResignation

  FROM (
        SELECT  emp.codi_emp AS codeCompanieAccountSystem, COALESCE(TRIM(emp.cgce_emp), '') AS federalRegistration, 
                emp.nome_emp AS name, emp.ufol_emp AS markedFolhaModule,
                CASE WHEN parmto.codi_emp IS NOT NULL THEN 1 ELSE 0 END AS existParameterFolha,
                CASE WHEN parmto.enviar_dados_esocial IS NOT NULL THEN parmto.enviar_dados_esocial ELSE 0 END AS markedFolhaParameterToSendEsocial,
                0 AS qtdEmployeesActive,
                0 AS qtdContribuintesActive,
                0 AS qtdContribuintesTypeEmpregadorActive,
                0 AS qtdEventsS1000,
                NULL AS lastDateEmployeeResignation,
                NULL AS lastDateContribuinteResignation
            
            FROM bethadba.geempre AS emp
                LEFT JOIN bethadba.foparmto AS parmto
                    ON    parmto.codi_emp = emp.codi_emp
                    
            WHERE emp.codi_emp = 10 --'#codi_emp'

        UNION ALL 

        SELECT  emp.codi_emp AS codeCompanieAccountSystem, COALESCE(TRIM(emp.cgce_emp), '') AS federalRegistration, 
                emp.nome_emp AS name, emp.ufol_emp AS markedFolhaModule,
                0 AS existParameterFolha,
                0 AS markedFolhaParameterToSendEsocial,
                SUM(CASE WHEN fun.tipo_epr = 1 AND res.demissao IS NULL THEN 1 ELSE 0 END) AS qtdEmployeesActive,
                SUM(CASE WHEN fun.tipo_epr = 2 AND afa.i_empregados IS NULL THEN 1 ELSE 0 END) AS qtdContribuintesActive,
                SUM(CASE WHEN fun.tipo_epr = 2 AND fun.tipo_contrib = 'E' AND afa.i_empregados IS NULL THEN 1 ELSE 0 END) AS qtdContribuintesActiveTypeEmpregador,
                0 AS qtdEventsS1000,
                DATEFORMAT( MAX( res.demissao ), 'yyyy-mm-dd' ) AS lastDateEmployeeResignation,
                DATEFORMAT( MAX( CASE WHEN fun.tipo_epr = 2 THEN afa.data_real ELSE NULL END ), 'yyyy-mm-dd' ) AS lastDateContribuinteResignation
            
          FROM bethadba.geempre AS emp
              INNER JOIN bethadba.foempregados AS fun
                    ON    fun.codi_emp = emp.codi_emp
              LEFT OUTER JOIN bethadba.forescisoes AS res
                        ON    res.codi_emp = fun.codi_emp
                          AND res.i_empregados = fun.i_empregados
                          AND res.tipo IN (1,2,3)
              LEFT OUTER JOIN bethadba.foafastamentos AS afa
                        ON    afa.codi_emp = fun.codi_emp
                          AND afa.i_empregados = fun.i_empregados
                          AND afa.i_afastamentos = 8
                  
          WHERE emp.codi_emp = 10 --'#codi_emp'

          GROUP BY codeCompanieAccountSystem, federalRegistration, name, markedFolhaModule, existParameterFolha, markedFolhaParameterToSendEsocial, qtdEventsS1000

          UNION ALL

          SELECT  emp.codi_emp AS codeCompanieAccountSystem, COALESCE(TRIM(emp.cgce_emp), '') AS federalRegistration, 
              emp.nome_emp AS name, emp.ufol_emp AS markedFolhaModule,
              0 AS existParameterFolha,
              0 AS markedFolhaParameterToSendEsocial,
              0 AS qtdEmployeesActive,
              0 AS qtdContribuintesActive,
              0 AS qtdContribuintesActiveTypeEmpregador,
              COUNT(1) AS qtdEventsS1000,
              NULL AS lastDateEmployeeResignation,
              NULL AS lastDateContribuinteResignation
          
          FROM bethadba.geempre AS emp
                INNER JOIN bethadba.foesocial_dados_eventos AS esocial
                    ON    emp.codi_emp = esocial.codi_emp
                      AND esocial.i_evento_esocial = 1000
                      AND esocial.tipo_envio in (1,2,4) /* nao eh exclusao */            
                      AND esocial.validado = 1
                  
          WHERE emp.codi_emp = 10 --'#codi_emp'
            AND NOT EXISTS ( SELECT 1
                              FROM bethadba.foesocial_dados_eventos AS esocial2
                              WHERE esocial2.codi_emp = esocial.codi_emp
                                  AND esocial2.i_evento_esocial = 3000
                                  AND esocial2.tipo_envio = 3
                                  AND esocial2.i_evento_esocial_excluido = esocial.i_evento_esocial
                                  AND esocial2.numero_recibo_excluido = esocial.numero_recibo )

          GROUP BY codeCompanieAccountSystem, federalRegistration, name, markedFolhaModule, existParameterFolha, markedFolhaParameterToSendEsocial,
                    qtdEmployeesActive, qtdContribuintesActive, qtdContribuintesActiveTypeEmpregador, lastDateEmployeeResignation, lastDateContribuinteResignation
      ) AS subDataFetch

GROUP BY codeCompanieAccountSystem, federalRegistration, name