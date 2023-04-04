SELECT STRING(subDataFetch.codeCompanieAccountSystem) AS codeCompanieAccountSystem,
       subDataFetch.federalRegistration,
       subDataFetch.competence AS competence,
       STRING(MAX(subDataFetch.taxRegime)) AS taxRegime,
       SUM(subDataFetch.qtdCalculationFolha) AS qtdCalculationFolha,
       SUM(subDataFetch.qtdEmployeesActive) AS qtdEmployeesActive,
       SUM(subDataFetch.qtdEmployeesFired) AS qtdEmployeesFired,
       SUM(subDataFetch.qtdContribuintesActive) AS qtdContribuintesActive,
       SUM(subDataFetch.qtdContribuintesFired) AS qtdContribuintesFired,
       SUM(subDataFetch.qtdEventsS1200) AS qtdEventsS1200,
       SUM(subDataFetch.qtdEventsS1210) AS qtdEventsS1210,
       SUM(subDataFetch.qtdEventsS1299) AS qtdEventsS1299,
       SUM(subDataFetch.qtdEventsS1200Processing) AS qtdEventsS1200Processing,
       SUM(subDataFetch.qtdEventsS1210Processing) AS qtdEventsS1210Processing,
       SUM(subDataFetch.qtdEventsS1299Processing) AS qtdEventsS1299Processing,
       SUM(subDataFetch.qtdEventsS1200Error) AS qtdEventsS1200Error,
       SUM(subDataFetch.qtdEventsS1210Error) AS qtdEventsS1210Error,
       SUM(subDataFetch.qtdEventsS1299Error) AS qtdEventsS1299Error

  FROM (
        SELECT  emp.codi_emp AS codeCompanieAccountSystem, 
                COALESCE(TRIM(emp.cgce_emp), '') AS federalRegistration,
                DATE('#competence#') AS competence,
                COALESCE(vig.rfed_par, 99) AS taxRegime,
                0 AS qtdCalculationFolha,
                0 AS qtdEmployeesActive,
                0 AS qtdEmployeesFired,
                0 AS qtdContribuintesActive,
                0 AS qtdContribuintesFired,
                0 AS qtdEventsS1200,
                0 AS qtdEventsS1210,
                0 AS qtdEventsS1299,
                0 AS qtdEventsS1200Processing,
                0 AS qtdEventsS1210Processing,
                0 AS qtdEventsS1299Processing,
                0 AS qtdEventsS1200Error,
                0 AS qtdEventsS1210Error,
                0 AS qtdEventsS1299Error
            
            FROM bethadba.geempre AS emp
                LEFT JOIN bethadba.efparametro_vigencia AS vig
                    ON    vig.codi_emp = emp.codi_emp
                      AND vig.vigencia_par = ( SELECT MAX(vig2.vigencia_par )
                                                 FROM bethadba.efparametro_vigencia AS vig2
                                                WHERE vig2.codi_emp = emp.codi_emp 
                                                  AND vig2.vigencia_par <= competence )
                    
            WHERE emp.codi_emp = '#codi_emp#'


        UNION ALL 


        SELECT  emp.codi_emp AS codeCompanieAccountSystem, 
                COALESCE(TRIM(emp.cgce_emp), '') AS federalRegistration,
                DATE('#competence#') AS competence,
                NULL AS taxRegime,
                0 AS qtdCalculationFolha,
                SUM(CASE WHEN fun.tipo_epr = 1
                          AND YMD( YEAR(fun.admissao), MONTH(fun.admissao), 1 ) <= competence
                          AND ( res.demissao IS NULL OR YMD( YEAR(res.demissao), MONTH(res.demissao), 1 ) > competence )
                    THEN 1 ELSE 0 END) AS qtdEmployeesActive,
                SUM(CASE WHEN fun.tipo_epr = 1
                          AND competence = YMD( YEAR(res.demissao), MONTH(res.demissao), 1 ) 
                    THEN 1 ELSE 0 END) AS qtdEmployeesFired,
                SUM(CASE WHEN fun.tipo_epr = 2
                          AND YMD( YEAR(fun.admissao), MONTH(fun.admissao), 1 ) <= competence
                          AND ( afa.data_real IS NULL OR YMD( YEAR(afa.data_real), MONTH(afa.data_real), 1 ) > competence )
                    THEN 1 ELSE 0 END) AS qtdContribuintesActive,
                SUM(CASE WHEN fun.tipo_epr = 2
                          AND competence = YMD( YEAR(afa.data_real), MONTH(afa.data_real), 1 ) 
                    THEN 1 ELSE 0 END) AS qtdContribuintesFired,
                0 AS qtdEventsS1200,
                0 AS qtdEventsS1210,
                0 AS qtdEventsS1299,
                0 AS qtdEventsS1200Processing,
                0 AS qtdEventsS1210Processing,
                0 AS qtdEventsS1299Processing,
                0 AS qtdEventsS1200Error,
                0 AS qtdEventsS1210Error,
                0 AS qtdEventsS1299Error
            
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
                  
          WHERE emp.codi_emp = '#codi_emp#'

          GROUP BY codeCompanieAccountSystem, federalRegistration, competence, taxRegime, qtdCalculationFolha, qtdEventsS1200, qtdEventsS1210,
                   qtdEventsS1299, qtdEventsS1200Processing, qtdEventsS1210Processing, qtdEventsS1299Processing, qtdEventsS1200Error,
                   qtdEventsS1210Error, qtdEventsS1299Error


        UNION ALL


        SELECT  emp.codi_emp AS codeCompanieAccountSystem, 
                COALESCE(TRIM(emp.cgce_emp), '') AS federalRegistration,
                DATE('#competence#') AS competence,
                NULL AS taxRegime,
                0 AS qtdCalculationFolha,
                0 AS qtdEmployeesActive,
                0 AS qtdEmployeesFired,
                0 AS qtdContribuintesActive,
                0 AS qtdContribuintesFired,
                SUM(CASE WHEN esocial.i_evento_esocial = 1200
                          AND esocial.i_resposta IN (201,202)
                          AND esocial.validado = 1
                    THEN 1 ELSE 0 END) AS qtdEventsS1200,
                SUM(CASE WHEN esocial.i_evento_esocial = 1210
                          AND esocial.i_resposta IN (201,202)
                          AND esocial.validado = 1
                    THEN 1 ELSE 0 END) AS qtdEventsS1210,
                0 AS qtdEventsS1299,
                SUM(CASE WHEN esocial.i_evento_esocial = 1200
                          AND esocial.validado = 0
                          AND esocial.aguardando_ajuste = 0
                    THEN 1 ELSE 0 END) AS qtdEventsS1200Processing,
                SUM(CASE WHEN esocial.i_evento_esocial = 1210
                          AND esocial.validado = 0
                          AND esocial.aguardando_ajuste = 0
                    THEN 1 ELSE 0 END) AS qtdEventsS1210Processing,
                0 AS qtdEventsS1299Processing,
                SUM(CASE WHEN esocial.i_evento_esocial = 1200
                          AND esocial.validado = 0
                          AND esocial.aguardando_ajuste = 1
                    THEN 1 ELSE 0 END) AS qtdEventsS1200Error,
                SUM(CASE WHEN esocial.i_evento_esocial = 1210
                          AND esocial.validado = 0
                          AND esocial.aguardando_ajuste = 1
                    THEN 1 ELSE 0 END) AS qtdEventsS1210Error,
                0 AS qtdEventsS1299Error
        
        FROM bethadba.geempre AS emp
              INNER JOIN bethadba.foesocial_dados_eventos AS esocial
                  ON    emp.codi_emp = esocial.codi_emp
                    AND esocial.competencia_calculo = competence
                    AND esocial.i_evento_esocial IN (1200,1210)
                    AND esocial.tipo_envio in (1)
                
        WHERE emp.codi_emp = '#codi_emp#'
          AND NOT EXISTS ( SELECT 1
                            FROM bethadba.foesocial_dados_eventos AS esocial2
                            WHERE esocial2.codi_emp = esocial.codi_emp
                                AND esocial2.i_evento_esocial = 3000
                                AND esocial2.tipo_envio = 3
                                AND esocial2.i_evento_esocial_excluido = esocial.i_evento_esocial
                                AND esocial2.numero_recibo_excluido = esocial.numero_recibo )

        GROUP BY codeCompanieAccountSystem, federalRegistration, competence, taxRegime, qtdCalculationFolha, qtdEmployeesActive, qtdEmployeesFired,
                   qtdContribuintesActive, qtdContribuintesFired, qtdEventsS1299, qtdEventsS1299Processing, qtdEventsS1299Error

      
      UNION ALL


        SELECT  emp.codi_emp AS codeCompanieAccountSystem, 
                COALESCE(TRIM(emp.cgce_emp), '') AS federalRegistration,
                DATE('#competence#') AS competence,
                NULL AS taxRegime,
                0 AS qtdCalculationFolha,
                0 AS qtdEmployeesActive,
                0 AS qtdEmployeesFired,
                0 AS qtdContribuintesActive,
                0 AS qtdContribuintesFired,
                0 AS qtdEventsS1200,
                0 AS qtdEventsS1210,
                SUM(CASE WHEN esocial.i_evento_esocial = 1299
                          AND esocial.i_resposta IN (201,202)
                          AND esocial.validado = 1
                    THEN 1 ELSE 0 END) AS qtdEventsS1299,
                0 AS qtdEventsS1200Processing,
                0 AS qtdEventsS1210Processing,
                SUM(CASE WHEN esocial.i_evento_esocial = 1299
                          AND esocial.validado = 0
                          AND esocial.aguardando_ajuste = 0
                    THEN 1 ELSE 0 END) AS qtdEventsS1299Processing,
                0 AS qtdEventsS1200Error,
                0 AS qtdEventsS1210Error,
                SUM(CASE WHEN esocial.i_evento_esocial = 1299
                          AND esocial.validado = 0
                          AND esocial.aguardando_ajuste = 1
                    THEN 1 ELSE 0 END) AS qtdEventsS1299Error
        
        FROM bethadba.geempre AS emp
              INNER JOIN bethadba.foesocial_dados_eventos AS esocial
                  ON    emp.codi_emp = esocial.codi_emp
                    AND esocial.competencia_calculo = competence
                    AND esocial.i_evento_esocial IN (1299)
                    AND esocial.tipo_envio in (1)
                
        WHERE emp.codi_emp = '#codi_emp#'
          AND NOT EXISTS ( SELECT 1
                               FROM bethadba.foesocial_dados_eventos AS esocial2
                              WHERE esocial2.codi_emp = esocial.codi_emp
                                AND esocial2.i_evento_esocial = 1298
                                AND esocial2.chave_tabela_dados = esocial.chave_tabela_dados
                                AND esocial2.data_criacao_registro > esocial.data_hora_conclusao )

        GROUP BY codeCompanieAccountSystem, federalRegistration, competence, taxRegime, qtdCalculationFolha, qtdEmployeesActive, qtdEmployeesFired,
                   qtdContribuintesActive, qtdContribuintesFired, qtdEventsS1200, qtdEventsS1210, qtdEventsS1200Processing, qtdEventsS1210Processing,
                   qtdEventsS1200Error, qtdEventsS1210Error


      ) AS subDataFetch

GROUP BY codeCompanieAccountSystem, federalRegistration, competence