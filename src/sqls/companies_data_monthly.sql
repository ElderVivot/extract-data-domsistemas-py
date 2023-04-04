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
      ) AS subDataFetch

GROUP BY codeCompanieAccountSystem, federalRegistration, competence